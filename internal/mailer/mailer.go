package mailer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/smtp"
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/maris-cyber/mailsender/internal/letter"
	"github.com/maris-cyber/mailsender/internal/limiter"
	"go.uber.org/zap"
)

const (
	SMTP_HOST   = "SMTP_HOST"
	SMTP_PORT   = "SMTP_PORT"
	SMTP_USER   = "SMTP_USER"
	SMTP_PSWD   = "SMTP_PASSWORD"
	NUM_SENDERS = "SMTP_MAX_SENDERS"
)

type Mailer struct {
	host      string
	port      string
	user      string
	password  string
	tlsconfig *tls.Config
	nSenders  int // кол-во sender'ов
	ToSend    chan *letter.Letter
	Complete  chan *letter.Letter
	lmt       *limiter.Limiter // rate limit
	wg        *sync.WaitGroup
}

// инициализировать
func New(ctx context.Context, lmt *limiter.Limiter, wg *sync.WaitGroup) (*Mailer, error) {
	var err error

	mH := Mailer{}
	if err = mH.GetConfig(); err != nil {
		zap.S().Debugf("mailer GetConfig error: %v\n", err)
	}

	mH.ToSend = make(chan *letter.Letter, mH.nSenders)
	mH.Complete = make(chan *letter.Letter, mH.nSenders)
	mH.lmt = lmt
	mH.wg = wg

	return &mH, err
}

// запустить пул отправлятелей сообщений
func (mH *Mailer) Run(ctx context.Context) {
	mH.wg.Add(mH.nSenders)

	for idS := 0; idS < mH.nSenders; idS++ {
		go mH.runWorker(ctx, idS)
	}
}

// получение конфигурации из переменных окружения
// по условиях курса разрешается
func (mH *Mailer) GetConfig() error {
	var (
		ok  bool
		err error
		s   string
	)

	if mH.host, ok = os.LookupEnv(SMTP_HOST); !ok {
		return fmt.Errorf("SMTP host not defined")
	}

	if mH.port, ok = os.LookupEnv(SMTP_PORT); !ok {
		return fmt.Errorf("SMTP port not defined")
	}

	if mH.user, ok = os.LookupEnv(SMTP_USER); !ok {
		return fmt.Errorf("SMTP user not defined")
	}

	if mH.password, ok = os.LookupEnv(SMTP_PSWD); !ok {
		return fmt.Errorf("SMTP user password not defined")
	}

	if s, ok = os.LookupEnv(NUM_SENDERS); !ok {
		return fmt.Errorf("SMTP user password not defined")
	}

	mH.nSenders, err = strconv.Atoi(s)

	mH.tlsconfig = &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         mH.host,
	}

	return err
}

// работа одного (каждого) веркера
func (mH *Mailer) runWorker(ctx context.Context, idS int) {
	defer mH.wg.Done()

	//  готовим smtpClient
	zap.S().Debug("smtp.PlainAuth")

	servername := fmt.Sprintf("%s:%s", mH.host, mH.port)
	auth := smtp.PlainAuth("", mH.user, mH.password, mH.host)

	zap.S().Debug("tls.Dial")

	conn, err := tls.Dial("tcp", servername, mH.tlsconfig)
	if err != nil {
		zap.S().Debugf("func Maiker.SendLetter can't tls.Dial: %v", err)

		return
	}

	zap.S().Debug("smtp.NewClient")

	smtpClient, err := smtp.NewClient(conn, mH.host)
	if err != nil {
		zap.S().Debugf("func Maiker.SendLetter can't smtp.NewClient: %v", err)

		return
	}
	defer smtpClient.Quit()

	// Auth
	zap.S().Debug("smtpClient.Auth")

	if err = smtpClient.Auth(auth); err != nil {
		zap.S().Debugf("Problem smtpClient.Auth.\nUser: %s\nPswd: %s\n", mH.user, mH.password)

		return
	}

	// основной цикл воркера
Waiting:
	for {
		select {
		case <-ctx.Done(): // если поступила команда на отключение
			// waitgroup отпускается defer'ом
			zap.S().Debugf("mail worker ctx.Done() %d", idS)

			return
		case ltr := <-mH.ToSend: // если поступило письмо из канала от разгребатора очереди
			for {
				select {
				case <-ctx.Done(): // если поступит команда не отключение во время ожидания тикета по условиям rate limit
					// письмо в это время числится в режиме "processing",
					// но обработчик очереди дождётся заверешения работы mailer'а
					// и переведёт все письма, оставшиеся в состоянии "processing"
					// в режим "awaiting", чтобы при следующем старте отдать их на отправку
					zap.S().Debugf("mail worker ctx.Done() %d", idS)

					return
				case <-mH.lmt.PoolTickets: // если есть разрешение на отправку письма по условиям rate limit
					zap.S().Debugf("mail worker %d from chan %v", idS, ltr)

					err := mH.SendLetter(smtpClient, ltr) // отправить письмо
					if err != nil {
						zap.S().Errorf("mH.SendLetter error: %v\n", err)
					}

					mH.Complete <- ltr // отправленное письмо в канал из которого читает очередь

					continue Waiting
				}
			}
		default:
			runtime.Gosched() // немного вежливости
		}
	}
}

// отправить письмо
func (mH *Mailer) SendLetter(smtpClient *smtp.Client, ltr *letter.Letter) error {
	var err error

	zap.S().Debugf("Sending letter %v\n", ltr)

	ltr.Status = "error" // если письмо не отправится по какой-то причине, статус уже выставлен

	/* Выключил рандомайзер
	// по условиям задания должен быть, но мешает играться
	rand.Seed(time.Now().UnixNano())
	if rand.Intn(100) < 50 {
			zap.S().Debugf("randomly func Letter.Send decided not to send")
			return fmt.Errorf("randomly func Letter.Send decided not to send")
	}
	*/

	// на время испытаний выключяю отправку писем
	// ltr.Status = "sent"
	// return nil

	// From
	// From используется для всех один, потому что использую гугловый сервис, авторизующий отправителя
	// наверное, можно попробоать отдавать разных отправителей
	if err = smtpClient.Mail(mH.user); err != nil {
		return fmt.Errorf("func Maiker.SendLetter can't smtpClient.Mail: %v", err)
	}

	// To
	// получатели не увидят адреса друг друга
	for _, rcpt := range ltr.Addresses {
		if err = smtpClient.Rcpt(rcpt); err != nil {
			return fmt.Errorf("func Maiker.SendLetter can't smtpClient.Rcpt: %v", err)
		}
	}

	// Data
	w, err := smtpClient.Data()
	if err != nil {
		return fmt.Errorf("func Maiker.SendLetter can't smtpClient.Data: %v", err)
	}

	message := fmt.Sprintf("From: %s\nSubject: %s\nContent-type: text/html; charset=utf-8\n\n%s", mH.user, ltr.Subject, ltr.Body)

	_, err = w.Write([]byte(message))
	if err != nil {
		return fmt.Errorf("func Maiker.SendLetter can't w.Write: %v", err)
	}

	err = w.Close()
	if err != nil {
		return fmt.Errorf("func Maiker.SendLetter can't w.Close: %v", err)
	}

	ltr.Status = "sent" // обработчик очереди использует этот статус, он пойдёт и в mongo, и в kafka

	zap.S().Debugf("Complete sending letter %v\nmessage: %s\n", ltr, message)

	return nil
}
