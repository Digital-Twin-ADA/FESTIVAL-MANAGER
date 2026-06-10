package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	fiberws "github.com/gofiber/websocket/v2"
	gorillaws "github.com/gorilla/websocket"
)

type Alert struct {
	ID         int     `json:"id"`
	StageID    int     `json:"stageId"`
	Type       string  `json:"type"`
	Message    string  `json:"message"`
	Severity   string  `json:"severity"`
	CreatedAt  string  `json:"createdAt"`
	Resolved   bool    `json:"resolved"`
	ResolvedAt *string `json:"resolvedAt"`
}

var clients = make(map[*fiberws.Conn]bool)
var clientsMutex sync.Mutex

func extractStompBody(msg []byte) []byte {
	s := string(msg)

	if strings.HasPrefix(s, "CONNECTED") {
		return nil
	}

	if strings.HasPrefix(s, "MESSAGE") {
		parts := strings.SplitN(s, "\n\n", 2)
		if len(parts) != 2 {
			return nil
		}

		body := strings.TrimSuffix(parts[1], "\x00")
		return []byte(body)
	}

	return msg
}

func sendStompFrame(conn *gorillaws.Conn, frame string) error {
	return conn.WriteMessage(gorillaws.TextMessage, []byte(frame+"\x00"))
}

func connectToCentral() {
	centralURL := os.Getenv("CENTRAL_WS_URL")
	if centralURL == "" {
		centralURL = "wss://twin-central-server.onrender.com/ws/websocket"
	}

	targetStageID, err := strconv.Atoi(os.Getenv("STAGE_ID"))
	if err != nil {
		targetStageID = 1
	}

	for {
		log.Printf("Connecting to Central Server at %s...", centralURL)

		centralConn, _, err := gorillaws.DefaultDialer.Dial(centralURL, nil)
		if err != nil {
			log.Println("Error connecting, retrying in 5s:", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Println("Connected to Central Server WebSocket")

		err = sendStompFrame(
			centralConn,
			"CONNECT\naccept-version:1.2\nhost:twin-central-server.onrender.com\nheart-beat:10000,10000\n\n",
		)
		if err != nil {
			log.Println("STOMP CONNECT failed:", err)
			centralConn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		err = sendStompFrame(
			centralConn,
			"SUBSCRIBE\nid:alerts-sub\ndestination:/topic/alerts\nack:auto\n\n",
		)
		if err != nil {
			log.Println("STOMP SUBSCRIBE failed:", err)
			centralConn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		log.Println("Subscribed to /topic/alerts")

		for {
			_, msg, err := centralConn.ReadMessage()
			if err != nil {
				log.Println("Disconnected from Central Server:", err)
				centralConn.Close()
				break
			}

			body := extractStompBody(msg)
			if body == nil || len(body) == 0 {
				continue
			}

			log.Printf("Received alert body: %s", string(body))

			var alert Alert
			if err := json.Unmarshal(body, &alert); err != nil {
				log.Println("JSON parse error:", err)
				continue
			}

			if alert.StageID != targetStageID {
				log.Printf("Ignored alert for Stage %d. This server is Stage %d.", alert.StageID, targetStageID)
				continue
			}

			clientsMutex.Lock()

			if len(clients) == 0 {
				log.Println("No Android clients connected")
			}

			for client := range clients {
				if err := client.WriteMessage(gorillaws.TextMessage, body); err != nil {
					log.Println("Error sending to client:", err)
					client.Close()
					delete(clients, client)
				}
			}

			clientsMutex.Unlock()

			log.Println("Forwarded alert to Android clients")
		}

		time.Sleep(5 * time.Second)
	}
}

func main() {
	app := fiber.New()

	go connectToCentral()

	app.Use("/ws", func(c *fiber.Ctx) error {
		if fiberws.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get("/ws", fiberws.New(func(c *fiberws.Conn) {
		log.Println("Android client connected")

		clientsMutex.Lock()
		clients[c] = true
		clientsMutex.Unlock()

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				log.Println("Android client disconnected")

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
