package letter

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Letter struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"` // для mongo
	Addresses []string           `bson:"addresses"`
	Subject   string             `bson:"subject"`
	Body      string             `bson:"body"`
	Token     string             `bson:"token"`
	Status    string             `bson:"status"`   // sent / error / awaiting
	KafkaKey  string             `bson:"kafkakey"` // ключ из кафки, записать при получении из кафки, отправлять в кафку с ним
}

func New() *Letter {
	return new(Letter)
}

func (l *Letter) Copy(res *Letter) {
	res.ID = l.ID
	res.Addresses = l.Addresses
	res.Subject = l.Subject
	res.Body = l.Body
	res.Token = l.Token
	res.Status = l.Status
	res.KafkaKey = l.KafkaKey
}
