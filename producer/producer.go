package producer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	ctx    context.Context
}

func New(urls []string, topic string, context context.Context) Producer {
	return Producer{
		ctx: context,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(urls...),
			Topic:                  topic,
			AllowAutoTopicCreation: true,
			Balancer:               &kafka.LeastBytes{},
		}}
}

func (p Producer) WriteMessage(key string, message []byte) error {
	if err := p.writer.WriteMessages(p.ctx, kafka.Message{
		Key:   []byte(key),
		Value: message}); err != nil {
		return err
	}
	return nil
}

func (p Producer) Close() {
	p.writer.Close()
}
