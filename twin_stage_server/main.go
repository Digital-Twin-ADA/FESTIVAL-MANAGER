package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Alert matches the JSON structure coming from your Java Central Server
type Alert struct {
	ID       int    `json:"id"`
	StageID  int    `json:"stageId"`
	Type     string `json:"type"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
	Resolved bool   `json:"resolved"`
}

var (
	// Keep track of all connected Android phones
	androidClients = make(map[*websocket.Conn]bool)
	clientsMutex   = sync.Mutex{}

	// Upgrader upgrades the standard HTTP connection to a WebSocket
	upgrader = websocket.Upgrader{
		// Allow connections from the Android app (bypasses CORS)
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// Environment variables
	stageID      int
	centralWsURL string
)

func main() {
	// 1. Load configurations from Environment variables
	port := getEnv("PORT", "8080")
	centralWsURL = getEnv("CENTRAL_WS_URL", "wss://twin-central-server.onrender.com/ws")

	stageIDStr := getEnv("STAGE_ID", "1")
	parsedID, err := strconv.Atoi(stageIDStr)
	if err != nil {
		log.Fatalf("Error: STAGE_ID must be a valid number. Received: %s", stageIDStr)
	}
	stageID = parsedID

	fmt.Printf("=== Starting Stage Server ===\n")
	fmt.Printf(">> STAGE_ID: %d\n", stageID)
	fmt.Printf(">> CENTRAL_URL: %s\n", centralWsURL)
	fmt.Printf(">> PORT: %s\n", port)

	// 2. Connect to Central Server in a background process
	go connectToCentralServer()

	// 3. Define endpoints
	http.HandleFunc("/", healthCheckHandler)
	http.HandleFunc("/ws", androidWebSocketHandler)                  // For raw Android connections
	http.HandleFunc("/topic/mobile-alerts", androidWebSocketHandler) // Alternative endpoint

	// 4. Start the server
	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("Error starting server: ", err)
	}
}

// ==========================================
// ANDROID SERVER LOGIC
// ==========================================

func androidWebSocketHandler(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading Android WebSocket:", err)
		return
	}
	defer ws.Close()

	// Register new Android client
	clientsMutex.Lock()
	androidClients[ws] = true
	clientsMutex.Unlock()

	log.Println("📱 New Android client connected!")

	// Keep connection open and listen for disconnects
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			log.Println("📱 Android client disconnected.")
			clientsMutex.Lock()
			delete(androidClients, ws)
			clientsMutex.Unlock()
			break
		}
	}
}

func broadcastToAndroid(alert Alert) {
	clientsMutex.Lock()
	defer clientsMutex.Unlock()

	for client := range androidClients {
		// Send the alert as JSON to connected phones
		err := client.WriteJSON(alert)
		if err != nil {
			log.Printf("Error sending to Android: %v", err)
			client.Close()
			delete(androidClients, client)
		}
	}
	log.Printf("🚀 Alert forwarded to %d active Android clients.", len(androidClients))
}

// ==========================================
// CENTRAL SERVER CLIENT LOGIC
// ==========================================

func connectToCentralServer() {
	for {
		log.Println("🔄 Attempting to connect to Central Server:", centralWsURL)

		// Connect via WebSocket
		c, _, err := websocket.DefaultDialer.Dial(centralWsURL, nil)
		if err != nil {
			log.Println("Dial error to Central Server:", err)
			time.Sleep(5 * time.Second) // Retry after 5 seconds
			continue
		}

		log.Println("✅ Connected to Central Server WebSocket!")

		// Send STOMP CONNECT frame
		connectFrame := "CONNECT\naccept-version:1.1,1.2\nheart-beat:10000,10000\n\n\x00"
		c.WriteMessage(websocket.TextMessage, []byte(connectFrame))

		// Send STOMP SUBSCRIBE frame to /topic/alerts
		subscribeFrame := "SUBSCRIBE\nid:sub-0\ndestination:/topic/alerts\n\n\x00"
		c.WriteMessage(websocket.TextMessage, []byte(subscribeFrame))

		// Read messages from Java Central Server
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("❌ Connection to Central Server lost:", err)
				c.Close()
				break // Exit inner loop and retry connection from scratch
			}

			msgStr := string(message)

			// Spring Boot STOMP sends messages as text frames
			// Extract just the JSON body
			if strings.HasPrefix(msgStr, "MESSAGE") {
				startIndex := strings.Index(msgStr, "{")
				if startIndex != -1 {
					jsonStr := strings.TrimRight(msgStr[startIndex:], "\x00")

					var incomingAlert Alert
					err := json.Unmarshal([]byte(jsonStr), &incomingAlert)
					if err != nil {
						log.Println("Error parsing incoming alert JSON:", err)
						continue
					}

					log.Printf("📥 Alert received from Central: [StageID: %d] %s", incomingAlert.StageID, incomingAlert.Message)

					// FILTER: Forward ONLY if stageID matches
					if incomingAlert.StageID == stageID {
						log.Println("🎯 Alert matches this STAGE_ID! Forwarding to Android...")
						broadcastToAndroid(incomingAlert)
					} else {
						log.Println("🛑 Alert ignored (not for this Stage).")
					}
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
}

// ==========================================
// UTILITY FUNCTIONS
// ==========================================

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Stage Server [ID: %d] is Live!", stageID)
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
