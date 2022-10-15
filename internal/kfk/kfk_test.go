package kfk

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/maris-cyber/mailsender/internal/letter"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

var ctx context.Context
var kH *DB

func TestMain(m *testing.M) {

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	ctx = context.Background()
	// канал для передачии из очереди в kafka
	fQtK := make(chan *letter.Letter, 10)
	// канал для передачии из kafka в очередь
	fKtQ := make(chan *letter.Letter, 10)

	kH = &DB{}

	if err := kH.CfgKfk.GetConfig(); err != nil {
		zap.S().Debugf("Can't config kafka: %v\nHardcoding config for test\n", err)
		kH.CfgKfk.brokers = []string{"tb-kafka-0.tb-kafka.team15.svc.cluster.local:9092"}
	}

	kH.CfgKfk.groupId = "TEST-mailsender"
	kH.CfgKfk.topicMS = "TEST-mts-to-mailsender"
	kH.CfgKfk.topicPrf = "TEST-mts-to-profile"

	kH.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kH.CfgKfk.brokers,
		Topic:    kH.CfgKfk.topicMS,
		GroupID:  kH.CfgKfk.groupId,
		MinBytes: 10e0,
		MaxBytes: 10e6,
	})

	kH.Writer4MS = &kafka.Writer{
		Addr:     kafka.TCP(kH.CfgKfk.brokers[0]),
		Topic:    kH.CfgKfk.topicMS,
		Balancer: &kafka.LeastBytes{},
	}

	kH.Writer4Prf = &kafka.Writer{
		Addr:     kafka.TCP(kH.CfgKfk.brokers[0]),
		Topic:    kH.CfgKfk.topicPrf,
		Balancer: &kafka.LeastBytes{},
	}

	kH.fKtQ = &fKtQ
	kH.fQtK = &fQtK

	zap.S().Debugf("Kafka test config %v\n", kH.CfgKfk)

	ctx, _ = context.WithTimeout(ctx, 25*time.Second)

	defer kH.Stop(ctx)
	// Запуск тестов.
	os.Exit(m.Run())
}

func Test_WriteToMS(t *testing.T) {
	var err error
	var tL [2]letter.Letter
	for i := range tL {
		tL[i].Addresses = []string{"uuunet@mailto.plus", "yhuzfu@mailto.plus"}
		tL[i].Body = "тест kafka WriteToMS" + strconv.Itoa(i)
		tL[i].Subject = "тема kafka " + strconv.Itoa(i)
		tL[i].Status = "Testing"
		tL[i].ID = primitive.NewObjectID()
	}
	msgs, _ := json.Marshal(&tL)
	key := make([]byte, 16)
	_, err = rand.Read(key)
	if err != nil {
		zap.S().Debugf("can't generate key for Kfk: %v\n", err)
	}

	zap.S().Debugf("Kafka test WriteToMS config %v\n", kH.CfgKfk)
	err = kH.WriteToMS(ctx, key, msgs)
	if err != nil {
		t.Errorf("Test kafka can't create for MS\n")
	}
	zap.S().Debugf("Test kafka create for MS %v\n", tL)

}

func Test_ReadCommit(t *testing.T) {

	zap.S().Debugf("Test kafka Read & Commit MS\n")
	err := kH.FetchCommitMS(ctx)
	if err != nil {
		t.Errorf("Test kafka can't read\n")
	}
	zap.S().Debugf("Test kafka read\n")
}

func Test_Read(t *testing.T) {
	var err error
	fKtQ := make(chan *letter.Letter, 10)
	// чтобы прочитать, надо написать

	var tL [1]letter.Letter
	for i := range tL {
		tL[i].Addresses = []string{"uuunet@mailto.plus", "yhuzfu@mailto.plus"}
		tL[i].Body = "тест kafka 4 ReadMS" + strconv.Itoa(i)
		tL[i].Subject = "тема kafka" + strconv.Itoa(i)
		tL[i].Status = "Testing"
		tL[i].ID = primitive.NewObjectID()
	}
	msgs, _ := json.Marshal(&tL)

	key := make([]byte, 16)
	_, err = rand.Read(key)
	if err != nil {
		zap.S().Debugf("can't generate key for Kfk: %v\n", err)
	}

	err = kH.WriteToMS(ctx, key, msgs)
	if err != nil {
		t.Errorf("Test kafka can't create for Read MS\n")
	}

	zap.S().Debugf("Test kafka Read MS\n")
	err = kH.ReadMS(ctx, fKtQ)
	if err != nil {
		t.Errorf("Test kafka can't read\n")
	}
	zap.S().Debugf("Test kafka read\n")
}

// ? todo сделать красиво
func Test_WriteToPrf(t *testing.T) {
	var tL letter.Letter

	tL.Addresses = []string{"uuunet@mailto.plus", "yhuzfu@mailto.plus"}
	tL.Body = "туловище письма из kafka_test"
	tL.Subject = "тема письма из kafka_test"
	tL.Status = "Sent"
	tL.ID = primitive.NewObjectID()

	err := kH.WriteToPrf(ctx, &tL)
	if err != nil {
		t.Errorf("Test kafka can't create for Prf\n")
	}
	zap.S().Debugf("Test kafka create for Prf %v\n", tL)
}
