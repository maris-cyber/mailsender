package mng

import (
	"context"
	"os"
	"testing"

	"github.com/maris-cyber/mailsender/internal/letter"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

var tdb *DB
var ctx context.Context

func TestMain(m *testing.M) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	ctx = context.Background()
	var err error
	var ok bool

	tdb, err = New(ctx)
	if err != nil {
		tdb.CfgMongo.MongoDBConnectionString = "mongodb://adminuser:password123@10.104.15.176:27017/"

		if tdb.CfgMongo.dbName, ok = os.LookupEnv(MONGODB_BASE); !ok {
			tdb.CfgMongo.dbName = DB_NAME
		}

		if tdb.CfgMongo.dbCollection, ok = os.LookupEnv(MONGODB_COLLECTION); !ok {
			tdb.CfgMongo.dbCollection = Q_Collection
		}
	}

	zap.S().Debugf("Test mongo config: %v\n", tdb.CfgMongo)

	if err = tdb.connectToDB(); err != nil {
		zap.S().Fatal("Can't ConnectTO DB: ", err)
	}

	os.Exit(m.Run())
}

func Test_CRUD(t *testing.T) {
	var tL letter.Letter
	var err error

	if err = tdb.connectToDB(); err != nil {
		zap.S().Fatal("Can't ConnectTO DB: ", err)
	}

	tL.Addresses = []string{"uuunet@mailto.plus", "yhuzfu@mailto.plus"}
	tL.Body = "туловище письма из queue_test"
	tL.Subject = "тема письма из queue_test"
	tL.Status = "Testing"
	tL.ID = primitive.NewObjectID()

	// Create
	err = tdb.Create(&tL)
	if err != nil {
		t.Errorf("Test Mongo can't create\n")
	}

	zap.S().Debugf("Test Mongo create %v\n", tL)

	// Read
	var tR letter.Letter

	err = tdb.Read(&tR, "Testing")
	if err != nil {
		t.Errorf("Test Mongo can't read\n")
	}

	zap.S().Debugf("Test Mongo read %v\n", tR)

	// Update
	err = tdb.UpdateSttById(tR.ID, "delete_me")
	if err != nil {
		t.Errorf("Test Mongo can't update by ID\n")
	}

	zap.S().Debugf("Test Mongo updated by ID %v\n", tR)

	// Delete
	err = tdb.Delete(tR.ID)
	if err != nil {
		t.Errorf("Test Mongo can't update\n")
	}

	zap.S().Debugf("Test Mongo delete %v\n", tR)

	// Create
	err = tdb.Create(&tL)
	if err != nil {
		t.Errorf("Test Mongo can't create\n")
	}

	zap.S().Debugf("Test Mongo create %v\n", tL)

	err = tdb.UpdateSttsAll("Testing", "Processing")
	if err != nil {
		t.Errorf("Test Mongo can't update Ststus\n")
	}

	zap.S().Debugf("Test Mongo updated Status\n")
}
