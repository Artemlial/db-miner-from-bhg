package main

import (
	// inner
	"context"
	"log"
	"os"
	"time"

	// my modules
	"dbMiner"

	// outer
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoMiner struct {
	Host    string
	session *mongo.Client
}

func New(host string) (*MongoMiner, error) {
	m := MongoMiner{Host: host}
	err := m.connect()
	if err != nil {
		log.Println("Func New: connecrtion error")
		return nil, err
	}
	return &m, nil
}

func (m *MongoMiner) connect() error {
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()
	s, err := mongo.Connect(ctx, options.Client().ApplyURI(m.Host))
	if err != nil {
		log.Println("Func connect: connection error:")
		return err
	}
	m.session = s
	return nil
}

func (m *MongoMiner) GetSchema() (*dbMiner.Schema, error) {
	var s = new(dbMiner.Schema)

	dbnames, err := m.session.ListDatabaseNames(context.TODO(), bson.D{})
	if err != nil {
		log.Println("Func GetSchema: mongo.Client.ListDatabaseNames error:")
		return nil, err
	}

	for _, dbname := range dbnames {
		db := dbMiner.Database{Name: dbname, Tables: []dbMiner.Table{}}
		collections, err := m.session.Database(dbname).ListCollectionNames(context.TODO(), bson.D{})
		if err != nil {
			log.Println("Func GetSchema: mongo.Database.ListCollectionNames error:")
			return nil, err
		}
		for _, collection := range collections {
			table := dbMiner.Table{Name: collection, Columns: []string{}}

			var doc bson.D
			err := m.session.Database(dbname).Collection(collection).FindOne(context.TODO(), bson.D{}).Decode(&doc)
			if err != nil {
				log.Println("Func GetSchema: mongo.SingleResult error:")
				return nil, err
			}

			for _, f := range doc {
				table.Columns = append(table.Columns, f.Key)
			}

			db.Tables = append(db.Tables, table)
		}
		s.Databases = append(s.Databases, db)
	}
	return s, nil
}

func main() {
	mm, err := New(os.Args[1])
	if err != nil {
		panic(err)
	}
	if err := dbMiner.Search(mm); err != nil {
		panic(err)
	}
}
