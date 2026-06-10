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

var clients = make(map[*fiberws.Conn]bool)
var clientsMutex sync.Mutex

func connectToCentral() {
	centralURL := os.Getenv("CENTRAL_WS_URL")
	if centralURL == "" {
		log.Println("ERROR: CENTRAL_WS_URL is missing")
		return
	}

	targetStageID, _ := strconv.Atoi(os.Getenv("STAGE_ID"))

	for {
		log.Printf("Connecting to Central Server at %s...", centralURL)
		centralConn, _, err := gorillaws.DefaultDialer.Dial(centralURL, nil)
		if err != nil {
			log.Println("Error connecting, retrying in 5s:", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Println("✅ Connected to Central Server WebSocket")

		for {
			_, msg, err := centralConn.ReadMessage()
			if err != nil {
				log.Println("❌ Disconnected from Central Server:", err)
				centralConn.Close()
				break 
			}

			// DEBUG: Print EXACTLY what Java sent us
			log.Printf("📥 RAW DATA RECEIVED FROM JAVA: %s\n", string(msg))

			var alert Alert
			if err := json.Unmarshal(msg, &alert); err == nil {
				// Filter the message based on STAGE_ID
				if alert.StageID == targetStageID {
					log.Printf("🎯 MATCH! Alert is for Stage %d. Forwarding to Android...", targetStageID)
					
					clientsMutex.Lock()
					clientCount := len(clients)
					if clientCount == 0 {
						log.Println("⚠️ WARNING: No Android apps are connected right now to receive this!")
					}

					for client := range clients {
						if err := client.WriteMessage(fiberws.TextMessage, msg); err != nil {
							log.Println("Error sending to client:", err)
							client.Close()
							delete(clients, client)
						} else {
							log.Println("🚀 SUCCESSFULLY SENT TO ANDROID CLIENT!")
						}
					}
					clientsMutex.Unlock()
				} else {
					log.Printf("⏭️ IGNORED: Alert is for Stage %d, but I am Stage %d.\n", alert.StageID, targetStageID)
				}
			} else {
				log.Println("❌ JSON PARSE ERROR (Data format from Java is wrong):", err)
			}
		}
		time.Sleep(5 * time.Second) 
	}
}

func main() {
	app := fiber.New()

	go connectToCentral()

	app.Use("/ws", func(c *fiber.Ctx) error {
		if fiberws.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get("/ws", fiberws.New(func(c *fiberws.Conn) {
		log.Println("📱 ANDROID/POSTMAN CLIENT CONNECTED!")

		clientsMutex.Lock()
		clients[c] = true
		clientsMutex.Unlock()

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				log.Println("📱 ANDROID/POSTMAN CLIENT DISCONNECTED!")
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