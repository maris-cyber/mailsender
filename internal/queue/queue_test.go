package queue

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/maris-cyber/mailsender/internal/db/mem"
	"github.com/maris-cyber/mailsender/internal/letter"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

var qH *Queue
var ctx context.Context
var tdb *mem.DB

func TestMain(m *testing.M) {

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	var err error
	ctx = context.Background()
	wg := &sync.WaitGroup{}
	mailerWG := &sync.WaitGroup{}

	ToSend := make(chan *letter.Letter, 10)
	Complete := make(chan *letter.Letter, 10)

	// канал для передачии из очереди в kafka
	chanFromQuToKfk := make(chan *letter.Letter, 10)
	// канал для передачии из kafka в очередь
	chanFrmKfkToQu := make(chan *letter.Letter, 10)

	tdb, err = mem.New(ctx)

	qH, err = New(ctx, tdb, &ToSend, &Complete, &chanFromQuToKfk, &chanFrmKfkToQu, mailerWG, wg)
	if err != nil {
		zap.S().Debugf("Can't start Queue: %v\n", err)
	}
	defer qH.Stop(ctx)

	// Запуск тестов.
	os.Exit(m.Run())
}

func Test_Put(t *testing.T) {
	var tL [3]letter.Letter

	for i := range tL {
		tL[i].Addresses = []string{"uuunet@mailto.plus", "yhuzfu@mailto.plus"}
		tL[i].Body = "Queue test туловище " + strconv.Itoa(i)
		tL[i].Subject = "Queue test тема " + strconv.Itoa(i)
		tL[i].Status = "awaiting"
		tL[i].ID = primitive.NewObjectID()
	}

	for i := range tL {
		err := qH.Put(ctx, &tL[i])
		if err != nil {
			t.Errorf("Test MemDB can't create: %v\n", err)
		}

		zap.S().Debugf("Test MemDB created %v\n", tL[i])
	}
}

func Test_Get(t *testing.T) {
	var tL letter.Letter

	err := qH.Get(ctx, &tL)
	if err != nil {
		t.Errorf("Test MemDB can't Get: %v\n", err)
	}

	zap.S().Debugf("Test MemDB Get %v\n", tL)
}
