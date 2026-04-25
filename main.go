package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"
)

func main() {
	port := flag.String("port", "8080", "Port to listen on")
	flag.Parse()

	fmt.Printf("Queue broker starting on port %s\n", *port)
	http.ListenAndServe(":"+*port, nil)
}
