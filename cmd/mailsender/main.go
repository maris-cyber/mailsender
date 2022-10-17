/*
Main стартует сервисы:
limiter - сервис, который регулирует rate limit для отправки почты;
mailer - сервис, который отправляет письма по smtp
параллельно несколькими воркерами;
kfk - сервис, который читает и пишет в kafka;
mng - сервис, который читает и пишет в mongodb;
queue - сервис, который делает очередь с помощью той реализации
базы данных, которую ему передадут при создании (mem или mng).

Сервисы взаимодействуют друг с другом с помощью каналов.
Параметры читаются из переменных среды окружения.
Реализова graceful shutdown.

http был прикручен для тестов, да так и остался.

P.S. Проект старый, теперь многое сделал бы иначе.
*/
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"

	"github.com/maris-cyber/mailsender/internal/db/mng"
	"github.com/maris-cyber/mailsender/internal/kfk"
	"github.com/maris-cyber/mailsender/internal/letter"
	"github.com/maris-cyber/mailsender/internal/limiter"
	"github.com/maris-cyber/mailsender/internal/mailer"
	"github.com/maris-cyber/mailsender/internal/queue"

	"sync"
)

var kH *kfk.DB
var cancelCtx context.CancelFunc
var srv http.Server
var ctx context.Context

func main() {
	var (
		ctxMng       context.Context
		cancelCtxMng context.CancelFunc
		err          error
	)

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Printf("Can't initialize logger, err: %v", err)
	}

	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	ctx, cancelCtx = context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// waitgroup для mailer'а, нужна отдельная, чтобы queue дождался завершения mailer'а
	// и привёл неотправленные письма в готовность к отправке при следующем старте
	mailerWG := &sync.WaitGroup{}

	// канал для передачии из очереди в kafka
	chanFromQuToKfk := make(chan *letter.Letter, 1)
	// канал для передачии из kafka в очередь
	chanFrmKfkToQu := make(chan *letter.Letter, 1)

	// инициализировать и запустить rate limit для пула воркеров, отправляющих почту
	var lmt *limiter.Limiter

	if lmt, err = limiter.New(); err != nil {
		zap.S().Fatalf("Can't initialize Limiter: %s", err)
	} else {
		zap.S().Debug("Limiter started")
	}

	go lmt.Run(ctx)

	// инициализировать и запустить почтовик
	var mH *mailer.Mailer

	if mH, err = mailer.New(ctx, lmt, mailerWG); err != nil {
		zap.S().Fatalf("Can't initialize Mailer: %s", err)
	} else {
		zap.S().Debug("Mailer started")
	}

	mH.Run(ctx)

	// инициализировать и запустить kafka
	if kH, err = kfk.New(ctx, &chanFromQuToKfk, &chanFrmKfkToQu); err != nil {
		zap.S().Fatalf("Can't Connect to kafka: %s", err)
	} else {
		zap.S().Debug("Kafka started")
	}

	go kH.Run(ctx)

	// создание коннектора к базе для очереди
	// отдельный контекст для монго, чтобы выключалась после всех
	ctxMng, cancelCtxMng = context.WithCancel(context.Background())

	var db *mng.DB

	db, err = mng.New(ctxMng)
	if err != nil {
		zap.S().Fatalf("Mongo error: %v\n", err)
	} else {
		zap.S().Debug("Mongo started")
	}

	// инициализировать и запустить очередь
	var qH *queue.Queue

	qH, err = queue.New(ctx, db, &mH.ToSend, &mH.Complete, &chanFromQuToKfk, &chanFrmKfkToQu, mailerWG, wg)
	if err != nil {
		zap.S().Fatalf("Queue error: %v", err)
	} else {
		zap.S().Debug("Queue started")
	}

	wg.Add(1)
	go qH.Run(ctx, "awaiting")

	// временная погремушка, чтобы принимать запросы на отправку писем
	// письма по заданию должны оказываться в mongo из bodyshop'а
	// по заданию не требуется

	MailSenderRouter := chi.NewRouter()
	MailSenderRouter.Use(middleware.Logger)
	MailSenderRouter.Use(middleware.Recoverer)

	MailSenderRouter.Route("/", func(r chi.Router) {
		r.Get("/", handler)
	})
	MailSenderRouter.Route("/post", func(r chi.Router) {
		r.Post("/", getTask)
	})

	MailSenderRouter.Route("/halt", func(r chi.Router) {
		r.Post("/", sayBye)
	})

	srv.Addr = ":8000"
	srv.Handler = MailSenderRouter

	err = srv.ListenAndServe()
	if err != http.ErrServerClosed {
		zap.S().Fatal(err)
	} else {
		zap.S().Debug("Http server closed")
	}

	// дождаться завершения очереди
	wg.Wait()
	// остановить коннектор к mongo
	cancelCtxMng()
}

func handler(w http.ResponseWriter, r *http.Request) {
	// чтобы не молчать
	w.Write([]byte("Прювет волку!\n"))
}

func sayBye(w http.ResponseWriter, r *http.Request) {
	// чтобы не молчать
	w.Write([]byte("Ну. ты заходи, если что...\n"))
	zap.S().Debugf("сказал шатдаун?")

	cancelCtx()
	srv.Shutdown(ctx)
}

func getTask(w http.ResponseWriter, r *http.Request) {
	var (
		tL  []letter.Letter
		err error
	)

	// проверить, что на вход пришёл массив писем
	dec := json.NewDecoder(r.Body)
	err = dec.Decode(&tL)

	smpl := "[{\"Subject\":\" тема\",\"Body\":\"сообщение\\n\",\"Addresses\":[\"uuunet@mailto.plus\",\"yhuzfu@mailto.plus\"]}]"

	if err != nil {
		http.Error(w, "Ожидаю МАССИВ писем, в каждом письме МАССИВ адресатов.\nОбразец:"+smpl+"\n"+err.Error(), http.StatusInternalServerError)
	} else {
		// отдать в кафку как будто задание получено из bodyshop
		msgs, err := json.Marshal(&tL)
		if err != nil {
			zap.S().Debugf("can't json.Marshal: %v", err)
		}
		key := make([]byte, 16)
		_, err = rand.Read(key)
		if err != nil {
			zap.S().Debugf("can't generate key for Kfk", err)
		}
		ctx := r.Context()
		err = kH.WriteToMS(ctx, key, msgs)
		if err != nil {
			zap.S().Errorf("kH.WriteToMS error; %v\n", err)
		}
	}
}
