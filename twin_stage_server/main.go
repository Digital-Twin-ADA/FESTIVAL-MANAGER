package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	fiberws "github.com/gofiber/websocket/v2"
	gorillaws "github.com/gorilla/websocket"
)

type Alert struct {
	StageID  int    `json:"stageId"`
	Type     string `json:"type"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
}

// Thread-safe map to keep track of connected Android clients
var clients = make(map[*fiberws.Conn]bool)
var clientsMutex sync.Mutex

// Connects to the Java Central Server instantly on startup
func connectToCentral() {
	centralURL := os.Getenv("CENTRAL_WS_URL")
	if centralURL == "" {
		log.Println("ERROR: CENTRAL_WS_URL environment variable is missing")
		return
	}

	targetStageID, _ := strconv.Atoi(os.Getenv("STAGE_ID"))

	for {
		log.Printf("Connecting to Central Server at %s...", centralURL)
		centralConn, _, err := gorillaws.DefaultDialer.Dial(centralURL, nil)
		if err != nil {
			log.Println("Error connecting to Central Server, retrying in 5s:", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Exact log required by Petronela
		log.Println("Connected to Central Server WebSocket")

		for {
			_, msg, err := centralConn.ReadMessage()
			if err != nil {
				log.Println("Disconnected from Central Server:", err)
				centralConn.Close()
				break // Breaks out to retry connection
			}

			var alert Alert
			if err := json.Unmarshal(msg, &alert); err == nil {
				// Filter the message based on STAGE_ID
				if alert.StageID == targetStageID {
					// Broadcast to all connected Android apps
					clientsMutex.Lock()
					for client := range clients {
						if err := client.WriteMessage(fiberws.TextMessage, msg); err != nil {
							log.Println("Error sending to client, dropping connection:", err)
							client.Close()
							delete(clients, client)
						}
					}
					clientsMutex.Unlock()
				}
			}
		}
		time.Sleep(5 * time.Second) // Wait before trying to reconnect
	}
}

func main() {
	app := fiber.New()

	// 1. Run the Central Server connection in the background immediately
	go connectToCentral()

	// 2. Middleware to allow WebSocket connections from Android/Postman
	app.Use("/ws", func(c *fiber.Ctx) error {
		if fiberws.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	// 3. Endpoint for Android/Postman to connect and wait for data
	app.Get("/ws", fiberws.New(func(c *fiberws.Conn) {
		log.Println("Android/Postman client connected to Stage Server!")

		clientsMutex.Lock()
		clients[c] = true
		clientsMutex.Unlock()

		// Keep connection alive until the Android app disconnects
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				log.Println("Android/Postman client disconnected")
				clientsMutex.Lock()
				delete(clients, c)
				clientsMutex.Unlock()
				break
			}
		}
	}))

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	log.Printf("Stage Server running on port %s", port)
	log.Fatal(app.Listen(":" + port))
}
