package limiter

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"time"

	"go.uber.org/zap"
)

const (
	SMTP_RATE_PERIOD              = "SMTP_RATE_LIMIT_PERIOD"
	DEFAULT_SMTP_RATE_PERIOD      = "5s"
	SMTP_RATE_MAX_LETTERS         = "SMTP_RATE_LIMIT_MAX_LETTERS"
	DEFAULT_SMTP_RATE_MAX_LETTERS = 5
	NUM_SENDERS                   = "SMTP_MAX_SENDERS"
)

type Limiter struct {
	Period      time.Duration
	MaxLetters  int
	takeTicket  *time.Ticker   // тикер выкладывает "разрешение" для воркеров
	PoolTickets chan time.Time // воркеры читают из канала "разрешение" на отправку
}

func New() (*Limiter, error) {
	var err error

	lmt := Limiter{}

	// получаем параметры из переменных среды
	// период времени
	p, ok := os.LookupEnv(SMTP_RATE_PERIOD)
	if !ok {
		lmt.Period, err = time.ParseDuration(DEFAULT_SMTP_RATE_PERIOD) // ошибки обрабатываю обе
	} else {
		lmt.Period, err = time.ParseDuration(p)
	}

	if err != nil { // если не было переменной среды или в переменной среды некорректное написание времени
		lmt.Period = 5 * time.Second
	}

	// максимальное количество писем, разрешённых к отправке в период времени
	mx, ok := os.LookupEnv(SMTP_RATE_MAX_LETTERS)
	if !ok {
		lmt.MaxLetters = DEFAULT_SMTP_RATE_MAX_LETTERS
	} else if lmt.MaxLetters, err = strconv.Atoi(mx); err != nil {
		lmt.MaxLetters = DEFAULT_SMTP_RATE_MAX_LETTERS
	}

	// период, определяющий частоту генерации разрешений на отправку
	x := time.Duration(int64(lmt.Period) / int64(lmt.MaxLetters))
	zap.S().Debugf("ticker %v\n", x)

	lmt.takeTicket = time.NewTicker(x)
	lmt.PoolTickets = make(chan time.Time, lmt.MaxLetters)

	zap.S().Debugf("Limiter config: %v", lmt)

	return &lmt, nil
}

// основной цикл работы ограничителя
func (lmt *Limiter) Run(ctx context.Context) {
	zap.S().Debugf("Run")

	for {
		select {
		case <-ctx.Done(): // если пришла команда на отключение
			// waitgroup не нужен
			zap.S().Debugf("Limiter ctx.Done()")

			return
		case x := <-lmt.takeTicket.C:
			lmt.PoolTickets <- x // разрешение можно положить в канал, только если в канале есть место
			// если канал полон процесс подождёт
			zap.S().Debugf("Limiter положил новый тикет")
		default:
			runtime.Gosched() // жест вежливости
		}
	}
}
