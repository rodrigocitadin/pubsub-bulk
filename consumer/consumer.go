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

type OnConsume func(kafka.Message) error
type OnConsumeBatch func([]kafka.Message) error

func New(urls []string, topic string, groupID string, context context.Context) Consumer {
	return Consumer{
		ctx: context,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: urls,
			Topic:   topic,
			GroupID: groupID})}
}

func (c Consumer) Close() {
	c.reader.Close()
}

func (c Consumer) ReadMessage(onConsume OnConsume) error {
	msg, err := c.reader.FetchMessage(c.ctx)
	if err != nil {
		return err
	}

	if err := onConsume(msg); err != nil {
		return err
	}

	c.reader.CommitMessages(c.ctx, msg)
	return nil
}

func (c Consumer) ReadMessageBatch(batchSize uint8, timeout uint8, onConsumeBatch OnConsumeBatch) error {
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
			return err
		}

		batch = append(batch, msg)
	}

	if len(batch) > 0 {
		if err := onConsumeBatch(batch); err != nil {
			return err
		}

		for _, msg := range batch {
			c.reader.CommitMessages(c.ctx, msg)
		}
	}

	return nil
}
