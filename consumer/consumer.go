package consumer

import (
	"context"
	"encoding/json"
	"log"

	"github.com/google/uuid"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/kafkapubsub"
)

type Message struct {
	ID      uuid.UUID `json:"id"`
	Message string    `json:"message"`
}

func Start(ctx context.Context) {
	subURL := "kafka://my-group?topic=messages"

	sub, err := pubsub.OpenSubscription(ctx, subURL)
	if err != nil {
		log.Fatalf("Error opening sub: %v", err)
	}
	defer sub.Shutdown(ctx)

	log.Println("Consumer running...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer closed")
			return
		default:
			msg, err := sub.Receive(ctx)
			if err != nil {
				log.Printf("Error receiving message: %v", err)
				continue
			}

			var message Message
			if err := json.Unmarshal(msg.Body, &message); err != nil {
				log.Printf("Error in JSON Unmarshal: %v", err)
				continue
			}

			log.Printf("Message %v received", message.ID)
			msg.Ack()
		}
	}
}
