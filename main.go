package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"rodrigocitadin/pubsub-bulk/consumer"
	"rodrigocitadin/pubsub-bulk/database"
	"rodrigocitadin/pubsub-bulk/producer"
	"rodrigocitadin/pubsub-bulk/proto"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const (
	postgresURL = "host=localhost user=pubsub password=pubsub dbname=pubsub port=5432 sslmode=disable"
	kafkaURL    = "localhost:9092"
	kafkaTopic  = "my-topic"
	kafkaGroup  = "my-group"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	database, err := database.Conn(postgresURL)
	if err != nil {
		log.Fatal("Error connecting into database")
	}

	producer := producer.New([]string{kafkaURL}, kafkaTopic, ctx)
	consumer := consumer.New([]string{kafkaURL}, kafkaTopic, kafkaGroup, ctx)

	go startProducer(producer)
	go startConsumer(consumer, *database)

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

		msg := database.Message{ID: uuid, Message: "Random message"}
		byteMsg, err := proto.Marshal(pb.ToProtoMessage(msg))
		if err != nil {
			log.Fatalf("Error in protobuf marshal")
		}

		if err := p.WriteMessage(
			uuid.String(),
			byteMsg); err != nil {
			log.Print(err)
			log.Fatalf("Message %v did not arrive", uuid.String())
		}

		log.Printf("Message %v arrived\n", uuid.String())
		time.Sleep(time.Second)
	}
}

func startConsumer(c consumer.Consumer, db database.Database) {
	for {
		if msgs, err := c.ReadMessageBatch(5, 10); err != nil {
			log.Fatal("Error reading batch of messages")
		} else {
			batch := make([]database.Message, 0, 5)

			for _, msg := range *msgs {
				id, err := uuid.ParseBytes(msg.Key)
				if err != nil {
					log.Fatal("Error parsing uuid from bytes")
				}

				var protoMessage pb.Message
				if err := proto.Unmarshal(msg.Value, &protoMessage); err != nil {
					log.Fatalf("Error in unmarshal message %v", id)
				}

				if message, err := pb.FromProtoMessage(&protoMessage); err != nil {
					log.Fatal("Error parsing proto message")
				} else {
					batch = append(batch, message)
				}

				log.Printf("Message %v within batch", id)
			}

			log.Printf("Successful batch reading of %d messages\n", len(*msgs))

			if err := db.BulkMessages(batch); err != nil {
				log.Println("Error inserting messages to database:", err)
			} else {
				log.Print("All messages entered into the database")
			}
		}
	}
}
