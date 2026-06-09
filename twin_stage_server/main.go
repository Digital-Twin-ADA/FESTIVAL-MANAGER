package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	// Grab the PORT variable we set in Render earlier
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Create a simple health check route
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Stage Server is Live!")
	})

	// Start the server
	fmt.Println("Starting Stage Server on port " + port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Println("Server failed to start:", err)
	}
}
