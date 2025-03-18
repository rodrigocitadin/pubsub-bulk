package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"rodrigocitadin/pubsub-bulk/consumer"
	"rodrigocitadin/pubsub-bulk/database"
	"rodrigocitadin/pubsub-bulk/producer"
	pb "rodrigocitadin/pubsub-bulk/proto"
	"strconv"
	"syscall"

	"github.com/google/uuid"
	_ "github.com/joho/godotenv/autoload"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

var (
	postgresURL                     = os.Getenv("POSTGRES_URL")
	kafkaURL                        = os.Getenv("KAFKA_URL")
	kafkaTopic                      = os.Getenv("KAFKA_TOPIC")
	kafkaGroup                      = os.Getenv("KAFKA_GROUP")
	producersQuantityString         = os.Getenv("PRODUCERS")
	messagesPerConsumerString       = os.Getenv("MESSAGES_PER_CONSUMER")
	consumerTimeoutString           = os.Getenv("CONSUMER_TIMEOUT")
	producersQuantity         uint8 = 2  // default value
	messagesPerConsumer       uint8 = 20 // default value
	consumerTimeout           uint8 = 10 // default value
)

func parseEnv() {
	if i, err := strconv.ParseUint(producersQuantityString, 10, 8); err != nil {
		log.Println("Error parsing messages per producer env, using default value:", producersQuantity)
	} else {
		producersQuantity = uint8(i)
	}

	if i, err := strconv.ParseUint(messagesPerConsumerString, 10, 8); err != nil {
		log.Println("Error parsing messages per consumer env, using default value:", messagesPerConsumer)
	} else {
		messagesPerConsumer = uint8(i)
	}

	if i, err := strconv.ParseUint(consumerTimeoutString, 10, 8); err != nil {
		log.Println("Error parsing consumer timeout env, using default value:", consumerTimeout)
	} else {
		consumerTimeout = uint8(i)
	}
}

func main() {
	parseEnv()

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	database, err := database.Conn(postgresURL)
	if err != nil {
		log.Fatal("Error connecting into database")
	}

	producer := producer.New([]string{kafkaURL}, kafkaTopic, ctx)
	consumer := consumer.New([]string{kafkaURL}, kafkaTopic, kafkaGroup, ctx)

	go startProducer(producer, producersQuantity)
	go startConsumer(consumer, *database)

	<-sig
	log.Println("Exiting the application...")
	cancel()
}

func startConsumer(c consumer.Consumer, db database.Database) {
	onConsume := func(msgs []kafka.Message) error {
		batch := make([]database.Message, 0, messagesPerConsumer)
		for _, msg := range msgs {
			id, err := uuid.ParseBytes(msg.Key)
			if err != nil {
				return errors.New("Error parsing uuid from bytes")
			}

			var protoMessage pb.Message
			if err := proto.Unmarshal(msg.Value, &protoMessage); err != nil {
				return errors.New(fmt.Sprintf("Error in unmarshal message %v", id))
			}

			if message, err := pb.FromProtoMessage(&protoMessage); err != nil {
				return errors.New("Error parsing proto message")
			} else {
				batch = append(batch, message)
			}

			log.Printf("Message %v within batch", id)
		}

		log.Printf("Successful batch reading of %d messages\n", len(msgs))

		if err := db.BulkMessages(batch); err != nil {
			return errors.New(fmt.Sprintf("Error inserting messages to database: %v", err))
		} else {
			log.Println("All messages entered into the database")
		}

		return nil
	}

	for {
		if err := c.ReadMessageBatch(messagesPerConsumer, consumerTimeout, onConsume); err != nil {
			log.Fatal(err)
		}
	}
}

func startProducer(p producer.Producer, quantity uint8) {
	for i := 0; i < int(quantity); i++ {
		go func() {
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
			}
		}()
	}
}
