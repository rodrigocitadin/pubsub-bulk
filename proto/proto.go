package pb

import (
	"rodrigocitadin/pubsub-bulk/database"

	"github.com/google/uuid"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

func ToProtoMessage(m database.Message) *Message {
	return &Message{
		Id:        m.ID.String(),
		Message:     m.Message,
		CreatedAt: timestamppb.New(m.CreatedAt),
		UpdatedAt: timestamppb.New(m.UpdatedAt),
		DeletedAt: timestamppb.New(m.DeletedAt.Time)}
}

func FromProtoMessage(pbMsg *Message) (database.Message, error) {
	id, err := uuid.Parse(pbMsg.Id)
	if err != nil {
		return database.Message{}, err
	}

	msg := database.Message{
		ID:    id,
		Message: pbMsg.Message,
	}

	msg.CreatedAt = pbMsg.CreatedAt.AsTime()
	msg.UpdatedAt = pbMsg.UpdatedAt.AsTime()

	if pbMsg.DeletedAt != nil {
		msg.DeletedAt = gorm.DeletedAt{Time: pbMsg.DeletedAt.AsTime(), Valid: true}
	}

	return msg, nil
}
