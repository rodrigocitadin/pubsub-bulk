package main

import (
	"os"
	"rodrigocitadin/pubsub/lib"
)

func main() {
	os.Setenv("KAFKA_BROKERS", "localhost:9092")
	pubsub.Producer()
}
