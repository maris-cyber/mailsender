package mem

import (
	"context"
	"os"
	"strconv"
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
	tdb, err = New(ctx)

	if err != nil {
		zap.S().Fatal("Can't ConnectTO DB: ", err)
	}

	defer tdb.Stop()

	// Запуск тестов.
	os.Exit(m.Run())
}

func Test_CRUDl(t *testing.T) {
	var err error

	var tL [5]letter.Letter

	for i := range tL {
		tL[i].Addresses = []string{"uuunet@mailto.plus", "yhuzfu@mailto.plus"}
		tL[i].Body = "туловище " + strconv.Itoa(i)
		tL[i].Subject = "тема " + strconv.Itoa(i)
		tL[i].Status = "Testing"
		tL[i].ID = primitive.NewObjectID()
	}

	// Create
	for i := range tL {
		err := tdb.Create(&tL[i])

		if err != nil {
			t.Errorf("Test MemDB can't create error:%v\n", err)
		}

		zap.S().Debugf("Test MemDB created %v\n", tL[i])
	}

	// Read
	var tR letter.Letter

	err = tdb.Read(&tR, "Testing")
	if err != nil {
		t.Errorf("Test MemDB can't read error: %v\n", err)
	} else {
		zap.S().Debugf("Test MemDB readed %v\n", tR)
	}

	// Update
	err = tdb.UpdateSttById(tR.ID, "delete_me")
	if err != nil {
		t.Errorf("Test MemDB can't update error: %v\n", err)
	} else {
		zap.S().Debugf("Test MemDB updated\n")
	}

	err = tdb.UpdateSttsAll("Testing", "Processed")
	if err != nil {
		t.Errorf("Test MemDB can't UpdateSttsAll error: %v\n", err)
	} else {
		zap.S().Debugf("Test MemDB UpdateSttsAll OK\n")
	}

	// List
	lst, err := tdb.List("*") // пока игнорирую статус, потом можно допилить
	if err != nil {
		t.Errorf("Test MemDB can't list error: %v\n", err)
	}

	for i, l := range lst {
		zap.S().Debugf("Test MemDB list %d:\n%v\n", i, l)
	}

	// Delete
	err = tdb.DeleteById(tR.ID)
	if err != nil {
		t.Errorf("Test MemDB can't delete, error:%v\n", err)
	} else {
		zap.S().Debugf("Test MemDB delete Ok\n")
	}

	// List
	lst2, err := tdb.List("*")
	if err != nil {
		t.Errorf("Test MemDB can't list\n")
	}

	for i, l := range lst2 {
		zap.S().Debugf("list %d:\n%v\n", i, l)
	}
}
