package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"rodrigocitadin/pubsub-bulk/consumer"
	"rodrigocitadin/pubsub-bulk/producer"
	"syscall"
	"time"
)

func main() {
	os.Setenv("KAFKA_BROKERS", "localhost:9092")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go consumer.Start(ctx)
	go producer.Start(ctx)

	<-quit
	fmt.Println("Leaving the program...")
	cancel()
	time.Sleep(1 * time.Second)
}
