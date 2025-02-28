package producer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/kafkapubsub"
)

func Start(ctx context.Context) {
	pubURL := "kafka://messages"

	pub, err := pubsub.OpenTopic(ctx, pubURL)
	if err != nil {
		log.Fatalf("Error opening pub: %v", err)
	}
	defer pub.Shutdown(ctx)

	log.Println("Producer running...")

	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			log.Println("Producer closed")
			return
		default:
			if uuid, err := uuid.NewV7(); err == nil {
				data := fmt.Sprintf(`{"id":"%v", "message":"Message %d"}`, uuid, rand.Intn(100))

				if err := pub.Send(ctx, &pubsub.Message{Body: []byte(data)}); err != nil {
					log.Printf("Error sending message: %v", err)
				} else {
					log.Printf("Message %v sent", uuid)
				}
			} else {
				log.Fatalf("Error generating uuid v7: %v", err)
			}

			time.Sleep(2 * time.Second)
		}
	}
}
