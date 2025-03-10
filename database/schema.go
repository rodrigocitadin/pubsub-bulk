package database

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Message struct {
	gorm.Model
	ID    uuid.UUID `gorm:"type:uuid;primaryKey"`
	Message string
}
