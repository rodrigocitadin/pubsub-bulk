package consumer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	ctx    context.Context
}

func New(urls []string, topic string, groupID string, context context.Context) Consumer {
	return Consumer{
		ctx: context,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: urls,
			Topic:   topic,
			GroupID: groupID})}
}

func (c Consumer) ReadMessage() (*kafka.Message, error) {
	msg, err := c.reader.ReadMessage(c.ctx)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}

func (c Consumer) ReadMessageBatch(batchSize uint8, timeout uint8) (*[]kafka.Message, error) {
	batch := make([]kafka.Message, 0, batchSize)
	batchDeadline := time.Now().Add(time.Duration(timeout) * time.Second)

	for len(batch) < int(batchSize) {
		msgCtx, cancel := context.WithDeadline(c.ctx, batchDeadline)
		msg, err := c.reader.FetchMessage(msgCtx)
		cancel()

		if time.Now().After(batchDeadline) {
			break
		}

		if err != nil {
			return nil, err
		}

		batch = append(batch, msg)
	}

	if len(batch) > 0 {
		for _, msg := range batch {
			c.reader.CommitMessages(c.ctx, msg)
		}
	}

	return &batch, nil
}

func (p Consumer) Close() {
	p.reader.Close()
}

