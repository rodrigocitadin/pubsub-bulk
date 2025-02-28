package pubsub

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/kafkapubsub"
)

func Producer() {
	ctx := context.Background()

	messagesTopicURL := "kafka://messages"
	messagesTopic, err := pubsub.OpenTopic(ctx, messagesTopicURL)
	if err != nil {
		log.Fatalf("Error connecting to topic: %v", err)
	}
	defer messagesTopic.Shutdown(ctx)

	for i := 1; i <= 10; i++ {
		if uuid, err := uuid.NewV7(); err == nil {
			data := fmt.Sprintf(`{"id":%d, "message":"Message %d"}`, uuid, i)

			if err := messagesTopic.Send(ctx, &pubsub.Message{Body: []byte(data)}); err != nil {
				log.Printf("Error sending message: %v", err)
			} else {
				log.Printf("Message %v sent!", uuid)
			}
		} else {
			log.Fatalf("Error generating uuid v7: %v", err)
		}
	}
}
