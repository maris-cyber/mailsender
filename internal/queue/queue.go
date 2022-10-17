/*
queue - пакет, обеспечивающий работу с очередью и использующий ту реализацию базы данных,
которую получает на вход при создании очереди.
*/
package queue

import (
	"context"
	"runtime"
	"sync"

	"github.com/maris-cyber/mailsender/internal/letter"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

type Qdb interface {
	Create(*letter.Letter) error
	Read(*letter.Letter, string) error
	UpdateSttById(primitive.ObjectID, string) error
	Stop() error
	UpdateSttsAll(string, string) error
}

type Queue struct {
	db            Qdb
	chToProcess   *chan *letter.Letter
	chFromProcess *chan *letter.Letter
	chToKfk       *chan *letter.Letter
	chFrmKfk      *chan *letter.Letter
	mailerWG      *sync.WaitGroup
	selfWG        *sync.WaitGroup
}

// конструктор очереди
func New(ctx context.Context, db Qdb, chToPrc *chan *letter.Letter, chFromPrc *chan *letter.Letter, chToKfk *chan *letter.Letter, chFrmKfk *chan *letter.Letter, mailerWG *sync.WaitGroup, selfWG *sync.WaitGroup) (*Queue, error) {
	var err error

	qH := Queue{
		db:            db,
		chToProcess:   chToPrc,
		chFromProcess: chFromPrc,
		chToKfk:       chToKfk,
		chFrmKfk:      chFrmKfk,
		mailerWG:      mailerWG,
		selfWG:        selfWG,
	}

	return &qH, err
}

// добавить письмо в очередь
func (qH *Queue) Put(ctx context.Context, qE *letter.Letter) error {
	return qH.db.Create(qE)
}

func (qH *Queue) Get(ctx context.Context, ty *letter.Letter) error {
	return qH.db.Read(ty, "awaiting")
}

// "Обслуживание" очереди, то есть регулярная проверка наличия писем и отправка обнаруженных писем
func (qH *Queue) Run(ctx context.Context, stts string) error {
	zap.S().Debug("queue Handle BEGIN")

	defer zap.S().Debug("queue Handle END")

	var err error

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	obj := letter.New()

	for {
		select {
		case <-ctx.Done():
			zap.S().Debug("queue Handle ctx.Done()")
			// дождаться майлера и перевести письма, которые остались в состоянии "processing" в статус "awaiting"
			// это выполняем в методе Stop
			return qH.Stop(ctx)
		case frm := <-*qH.chFrmKfk:
			zap.S().Debugf("Queue from chan %v", frm)

			err = qH.Put(ctx, frm)
			if err != nil {
				zap.S().Errorf("qh.Put error: %v\n", err)
			}
		case sended := <-*qH.chFromProcess:
			err = qH.db.UpdateSttById(sended.ID, sended.Status)
			if err != nil {
				zap.S().Errorf("qH.db.UpdateSttById error: %v\n", err)
			}
			// отправить в канал для kafka
			zap.S().Debugf("в канал fQtK отправлен %v", sended)
			*qH.chToKfk <- sended
		default:
			err := qH.db.Read(obj, stts)
			if err != nil {
				runtime.Gosched()

				continue
			}

			wg.Add(1)

			err = qH.db.UpdateSttById(obj.ID, "processing")
			if err != nil {
				zap.S().Errorf("qH.db.UpdateSttById processing error: %v\n", err)
			}

			// хотел здесь просто отправлять в канал для майлера,
			// но если буфер будет полный, обработчик здесь остановится и не будет реагировать на другие события
			go qH.ProcessEl(ctx, obj)
			obj = letter.New()

			runtime.Gosched()
		}
	}
}

func (qH *Queue) ProcessEl(ctx context.Context, qE *letter.Letter) {
	// в канал почтовым воркерам
	*qH.chToProcess <- qE
}

// отключиться от базы
func (qH *Queue) Stop(ctx context.Context) error {
	var err error

	qH.mailerWG.Wait()

	err = qH.db.UpdateSttsAll("processing", "awaiting")
	if err != nil {
		zap.S().Errorf("qH.db.UpdateSttsAll error: %v\n", err)
	}

	qH.selfWG.Done()

	return qH.db.Stop()
}
