package kfk

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"

	"github.com/maris-cyber/mailsender/internal/letter"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type DB struct {
	CfgKfk     Config
	Reader     *kafka.Reader // читать сообщения для mailsender
	Writer4MS  *kafka.Writer // для тестов писать сообщения для mailsender
	Writer4Prf *kafka.Writer // писать сообщения для profile
	fQtK       *chan *letter.Letter
	fKtQ       *chan *letter.Letter
}

// инициализация

func New(ctx context.Context, fQtK *chan *letter.Letter, fKtQ *chan *letter.Letter) (*DB, error) {
	kH := DB{}

	if err := kH.CfgKfk.GetConfig(); err != nil {
		return &kH, fmt.Errorf("Kafka.GetConfig error: %v", err)
	}

	zap.S().Debugf("Kafka.GetConfig: %v\n", kH.CfgKfk)

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

	kH.fKtQ = fKtQ
	kH.fQtK = fQtK

	return &kH, nil
}

// обработка событий для kafka
func (kH *DB) Run(ctx context.Context) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	defer wg.Wait()

	// обработка при поступлении сообщения от очереди для передачи через kafka в profile

	go func(ctx context.Context) {
		defer wg.Done()
		go kH.RunFromQueue(ctx)
	}(ctx)

	// чтение из kafka сообщений в топике для mailsender и передача в канал для очереди

	go func(ctx context.Context) {
		defer wg.Done()
		go kH.RunFromKafka(ctx)
	}(ctx)
}

func (kH *DB) RunFromQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			zap.S().Debug("Kafka RunFromQueue ctx.Done()")

			return
		case b := <-*kH.fQtK:
			err := kH.WriteToPrf(ctx, b)
			if err != nil {
				zap.S().Errorf("kH.WriteToPrf error: %v\n", err)
			}
		}
	}
}

func (kH *DB) RunFromKafka(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			zap.S().Debug("Kafka RunFromKafka ctx.Done()")

			return
		default:
			err := kH.FetchCommitMS(ctx)
			if err != nil {
				zap.S().Infof("kH.FetchCommitMS error: %v\n", err)
			}

			runtime.Gosched()
		}
	}
}

// создавать сообщения в kafka для mailsender
// погремушка для тестирования
func (kH *DB) WriteToMS(ctx context.Context, k []byte, b []byte) error {
	var err error

	messages := []kafka.Message{
		{
			Key:   k,
			Value: b,
		},
	}

	err = kH.Writer4MS.WriteMessages(ctx, messages...)

	return err
}

// читать сообщения из кафки,
// отправлять их в DB очередь и отмечать как прочитанные
func (kH *DB) FetchCommitMS(ctx context.Context) error {
	var tL []letter.Letter

	zap.S().Debugf("Kfk Read waiting fetch\n")

	msg, err := kH.Reader.FetchMessage(ctx)
	if err != nil {
		return err
	}

	zap.S().Debugf("Kfk Read fetch %s\n", msg.Value)

	err = json.Unmarshal(msg.Value, &tL)
	if err != nil {
		zap.S().Debugf("json.Unmarshal error: %v\n", err)
	}

	keyFromKfk := string(msg.Key)

	zap.S().Debugf("Kfk Unmarshal %v\n", tL)

	for i := range tL {
		tL[i].Status = "awaiting"
		tL[i].KafkaKey = keyFromKfk
		// отправить в канал для обработчика событий очереди
		zap.S().Debugf("Kfk send to Queue chan %v\n", tL[i])
		*kH.fKtQ <- &tL[i]
	}

	err = kH.Reader.CommitMessages(context.Background(), msg)

	return err
}

func (kH *DB) ReadMS(ctx context.Context, fKtQ chan *letter.Letter) error {
	var tL []letter.Letter

	msg, err := kH.Reader.ReadMessage(ctx)
	if err != nil {
		return err
	}

	zap.S().Debugf("Kfk Read fetch %s\n", msg.Value)

	err = json.Unmarshal(msg.Value, &tL)
	if err != nil {
		zap.S().Debugf("json.Unmarshal error: %v\n", err)
	}

	keyFromKfk := string(msg.Key)

	zap.S().Debugf("Kfk Unmarshal %v\n", tL)

	for i := range tL {
		tL[i].Status = "awaiting"
		tL[i].KafkaKey = keyFromKfk
		// отправить в канал для обработчика событий очереди
		fKtQ <- &tL[i]
	}

	return err
}

// писать сообщения в кафку для prodile
// пока пишется по одному сообщению
func (kH *DB) WriteToPrf(ctx context.Context, t *letter.Letter) error {
	var (
		v   []byte
		err error
	)

	if v, err = json.Marshal(t); err != nil {
		return err
	}

	zap.S().Debugf("kafka writeToPrf %s", v)

	messages := []kafka.Message{
		{
			Key:   []byte(t.KafkaKey),
			Value: v,
		},
	}

	err = kH.Writer4Prf.WriteMessages(context.Background(), messages...)

	return err
}

// пока нет ничего
func (kH *DB) Stop(ctx context.Context) error {
	return nil
}
