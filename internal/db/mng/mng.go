package mng

import (
	"context"
	"fmt"
	"sync"

	"github.com/maris-cyber/mailsender/internal/letter"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
)

type DB struct {
	mCollection *mongo.Collection
	mClient     *mongo.Client
	CfgMongo    MongoConfig
	mu          *sync.Mutex
	ctx         context.Context
}

// конструктор
func New(ctx context.Context) (*DB, error) {
	qH := DB{}
	qH.ctx = ctx
	qH.mu = &sync.Mutex{}

	if err := qH.CfgMongo.GetConfig(); err != nil {
		return &qH, fmt.Errorf("queue Start CfgMongo.GetConfig error: %v", err)
	}

	if err := qH.connectToDB(); err != nil {
		return &qH, fmt.Errorf("queue Start ConnectToDB error: %v", err)
	}

	return &qH, nil
}

func (qH *DB) connectToDB() error {
	var err error

	// проверка существования подключения и необходимости выполнять подключение
	if qH.mClient != nil {
		if err = qH.mClient.Ping(qH.ctx, readpref.Primary()); err == nil {
			zap.S().Debug("queue connected to DB, not need ConnectToDB")

			return nil
		}
	}

	cOpts := options.Client().ApplyURI(qH.CfgMongo.MongoDBConnectionString)

	qH.mClient, err = mongo.Connect(qH.ctx, cOpts)
	if err != nil {
		zap.S().Debugf("queue ConnectToDB function mongo.connect error: %v", err)

		return fmt.Errorf("queue ConnectToDB function mongo.connect error: %v", err)
	} else {
		zap.S().Debug("ConnectToDB passed")

	}

	qH.mCollection = qH.mClient.Database(qH.CfgMongo.dbName).Collection(qH.CfgMongo.dbCollection)

	return nil
}

// create
func (qH *DB) Create(e *letter.Letter) error {
	if err := qH.mClient.Ping(qH.ctx, readpref.Primary()); err != nil {
		zap.S().Debugf("mongo Create Ping error: %v\n", err)

		if err := qH.connectToDB(); err != nil {
			zap.S().Debugf("mongo Create connectToDB error: %v\n", err)

			return fmt.Errorf("mongo Create connectToDB error: %v", qH.mClient)
		}

		zap.S().Debug("mongo Create connectToDB pass")
	}

	res, err := qH.mCollection.InsertOne(qH.ctx, e)
	if err != nil {
		return fmt.Errorf("mongo Create error: %v", err)
	}

	zap.S().Debugf("in mongo inserted: %v\n", res)

	return nil
}

// read
func (qH *DB) Read(target *letter.Letter, stts string) error {
	if err := qH.mClient.Ping(qH.ctx, readpref.Primary()); err != nil {
		zap.S().Debugf("mongo Get Ping error: %v", err)

		if err := qH.connectToDB(); err != nil {
			zap.S().Debugf("mongo Get connectToDB error: %v\n", err)

			return fmt.Errorf("mongo Get connectToDB error: %v", err)
		}
	}
	// сортировка по возрастанию даты вставки
	// работает, письма выбираются сначала самые ранние

	options := options.FindOne()
	options.SetSort(bson.D{{"datefield", 1}})

	filter := bson.D{primitive.E{Key: "status", Value: stts}}
	result := qH.mCollection.FindOne(qH.ctx, filter, options)

	br, err := result.DecodeBytes()
	if err != nil {
		zap.S().Errorf("result.DecodeBytes error: %v\n", err)
	}

	if err = bson.Unmarshal(br, target); err != nil {
		return fmt.Errorf("mng Read bson.Unmarshal %v", err)
	}

	return err
}

func (qH *DB) UpdateSttById(id primitive.ObjectID, stts string) error {
	update := bson.D{primitive.E{Key: "$set", Value: bson.D{primitive.E{Key: "status", Value: stts}}}}

	res, err := qH.mCollection.UpdateByID(qH.ctx, id, update)
	if err != nil {
		zap.S().Debugf("Error updating DB after processing QuElement: %v\n", err)

		return fmt.Errorf("error updating DB after processing QuElement: %v", err)
	}

	zap.S().Debugf("mongodb modified: %v status: %s count: %d", id, stts, res.ModifiedCount)

	return nil
}

// // изменить все статусы oldstts на newstts
func (qH *DB) UpdateSttsAll(oldstts string, newstts string) error {
	update := bson.D{primitive.E{Key: "$set", Value: bson.D{primitive.E{Key: "status", Value: newstts}}}}
	filter := bson.D{primitive.E{Key: "status", Value: oldstts}}

	res, err := qH.mCollection.UpdateMany(qH.ctx, filter, update)
	if err != nil {
		zap.S().Debugf("Error updating DB after processing QuElement: %v\n", err)

		return fmt.Errorf("error updating DB after processing QuElement: %v", err)
	}

	zap.S().Debugf("mongodb modified oldstatus: %s newstatus: %s\n Result: %v", oldstts, newstts, res)

	return nil
}

// Для коллекции
// Delete
func (qH *DB) Delete(id primitive.ObjectID) error {
	filter := bson.D{primitive.E{Key: "_id", Value: id}}

	res, err := qH.mCollection.DeleteOne(qH.ctx, filter)
	if err != nil {
		zap.S().Debugf("Error updating DB after processing QuElement: %v\n", err)

		return fmt.Errorf("error updating DB after processing QuElement: %v", err)
	}

	zap.S().Debugf("delete Result: %d", res)

	return nil
}

func (qH *DB) Stop() error {
	if err := qH.mClient.Disconnect(qH.ctx); err != nil {
		return fmt.Errorf("mongo.Client.Diconnect error: %v", err)
	}

	return nil
}
