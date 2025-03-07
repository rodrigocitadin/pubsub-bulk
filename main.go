package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"rodrigocitadin/pubsub-bulk/consumer"
	"rodrigocitadin/pubsub-bulk/producer"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	producer := producer.New([]string{"localhost:9092"}, "my-topic", ctx)
	consumer := consumer.New([]string{"localhost:9092"}, "my-topic", "my-group", ctx)

	go startProducer(producer)
	go startConsumer(consumer)

	<-sig
	log.Println("Exiting the application...")
	producer.Close()
	consumer.Close()
	cancel()
}

func startProducer(p producer.Producer) {
	for {
		uuid, err := uuid.NewV7()
		if err != nil {
			log.Fatal("Error creating UUID V7")
		}

		if err := p.WriteMessage(uuid.String(), []byte("Random Message")); err != nil {
			log.Fatalf("Message %v did not arrive", uuid.String())
		}
		log.Printf("Message %v arrived\n", uuid.String())

		time.Sleep(time.Second)
	}
}

func startConsumer(c consumer.Consumer) {
	for {
		if msgs, err := c.ReadMessageBatch(5, 10); err != nil {
			log.Fatal("Error reading batch of messages")
		} else {
			log.Printf("Successful batch reading of %d messages\n", len(*msgs))

			for _, msg := range *msgs {
				log.Printf("Message %v within batch", string(msg.Key))
			}
		}
	}
}
