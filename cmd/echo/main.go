package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
)

func main() {
	addr := flag.String("addr", ":42064", "server listener address")
	flag.Parse()

	http.HandleFunc("/greet", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("received request", "from", r.RemoteAddr)
		name := r.URL.Query().Get("name")
		if name == "" {
			name = "stranger"
		}
		fmt.Fprintf(w, "Hello, %s!", name)
	})

	log.Printf("Server starting on %s\n", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
