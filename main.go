package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"rodrigocitadin/pubsub/lib"
	"syscall"
	"time"
)

func main() {
	os.Setenv("KAFKA_BROKERS", "localhost:9092")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go lib.Consumer(ctx)
	go lib.Producer(ctx)

	<-quit
	fmt.Println("Leaving the program...")
	cancel()
	time.Sleep(1 * time.Second)
}
