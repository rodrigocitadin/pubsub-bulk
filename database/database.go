package database

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Database struct {
	db *gorm.DB
}

func Conn(url string) (*Database, error) {
	if db, err := gorm.Open(postgres.Open(url), &gorm.Config{}); err != nil {
		return nil, err
	} else {
		db.AutoMigrate(&Message{})
		database := &Database{db: db}

		return database, nil
	}
}

func (d Database) BulkMessages(messages []Message) {
	d.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(&messages)
}
