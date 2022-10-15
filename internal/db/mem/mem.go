package mem

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/maris-cyber/mailsender/internal/letter"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

type DB struct {
	Data []letter.Letter
	mu   *sync.Mutex
	ctx  context.Context
}

func New(ctx context.Context) (*DB, error) {
	qH := DB{}
	qH.ctx = ctx
	qH.mu = &sync.Mutex{}

	return &qH, nil
}

func (qH *DB) Stop() error {
	return nil
}

func (qH *DB) Create(t *letter.Letter) error {
	zap.S().Debugf("Create %v\n", t)
	qH.mu.Lock()
	defer qH.mu.Unlock()
	qH.Data = append(qH.Data, *t)

	return nil
}

func (qH *DB) Read(t *letter.Letter, stts string) error {
	zap.S().Debugf("Read with status %v\n", stts)
	qH.mu.Lock()
	defer qH.mu.Unlock()

	if len(qH.Data) == 0 {
		return fmt.Errorf("mem DB is empty")
	}

	for _, x := range qH.Data {
		if strings.Compare(x.Status, stts) == 0 {
			x.Copy(t)

			return nil
		}
	}

	return fmt.Errorf("did not find items with status %s", stts)
}

func (qH *DB) UpdateSttById(id primitive.ObjectID, stts string) error {
	zap.S().Debugf("UpdatingSttById ID %v\n", id)

	qH.mu.Lock()
	defer qH.mu.Unlock()

	for i := range qH.Data {
		if qH.Data[i].ID == id {
			qH.Data[i].Status = stts

			return nil
		}
	}

	return fmt.Errorf("can't find id %v: ", id)
}

// изменить все статусы oldstts на newstts
func (qH *DB) UpdateSttsAll(oldstts string, newstts string) error {
	zap.S().Debugf("UpdateSttsAll oldstatus %s newstatus %s\n", oldstts, newstts)
	qH.mu.Lock()
	defer qH.mu.Unlock()

	for i := range qH.Data {
		if strings.EqualFold(qH.Data[i].Status, oldstts) {
			qH.Data[i].Status = newstts
		}
	}

	return nil
}

func (qH *DB) DeleteById(id primitive.ObjectID) error {
	zap.S().Debugf("DeleteById ID %v\n", id)

	if len(qH.Data) == 0 {
		return fmt.Errorf("can't delete DB: DB empty")
	}

	qH.mu.Lock()
	defer qH.mu.Unlock()

	for i := range qH.Data {
		if qH.Data[i].ID != id {
			continue
		}

		if i == 0 {
			qH.Data = qH.Data[1:]

			return nil
		}

		if i == len(qH.Data)-1 {
			qH.Data = qH.Data[:i-1]

			return nil
		}

		qH.Data = append(qH.Data[:i], qH.Data[i+1:]...)

		return nil
	}

	return fmt.Errorf("can't delete DB: Id not found")
}

func (qH *DB) List(stts string) ([]*letter.Letter, error) {
	zap.S().Debugf("List\n")
	qH.mu.Lock()
	defer qH.mu.Unlock()

	if len(qH.Data) == 0 {
		return nil, fmt.Errorf("mem DB is empty")
	}

	rslt := make([]*letter.Letter, len(qH.Data))

	for i := range qH.Data {
		rslt[i] = &qH.Data[i]
	}

	return rslt, nil
}

func (qH *DB) SetContext(ctx context.Context) {
	qH.ctx = ctx
}
