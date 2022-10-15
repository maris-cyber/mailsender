package mng

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

const (
	MONGODB_HOST       = "MONGO_NODEPORT_SVC_SERVICE_HOST"
	MONGODB_PORT       = "MONGO_NODEPORT_SVC_SERVICE_PORT"
	MONGODB_USER       = "MONGODB_USERNAME"
	MONGODB_PWD        = "MONGODB_PASSWORD"
	HOME_DB            = "MONGO_LOCAL"
	DB_NAME            = "mts"
	MONGODB_BASE       = "MONGODB_BASE_NAME"
	Q_Collection       = "letters"
	MONGODB_COLLECTION = "MONGODB_COLLECTION"
)

type MongoConfig struct {
	MongoDBConnectionString string
	dbName                  string
	dbCollection            string
}

func (c *MongoConfig) GetConfig() error {
	var (
		ok                         bool
		user, password, host, port string
	)

	if host, ok = os.LookupEnv(MONGODB_HOST); !ok {
		return fmt.Errorf("MongoDb host not defined")
	}

	if port, ok = os.LookupEnv(MONGODB_PORT); !ok {
		return fmt.Errorf("MongoDb port not defined")
	}

	if user, ok = os.LookupEnv(MONGODB_USER); !ok {
		return fmt.Errorf("MongoDb user not defined")
	}
	if password, ok = os.LookupEnv(MONGODB_PWD); !ok {
		return fmt.Errorf("MongoDb password not defined")
	}

	if c.dbName, ok = os.LookupEnv(MONGODB_BASE); !ok {
		c.dbName = DB_NAME
	}

	if c.dbCollection, ok = os.LookupEnv(MONGODB_COLLECTION); !ok {
		c.dbCollection = Q_Collection
	}

	if mongodb, ok := os.LookupEnv(HOME_DB); ok {
		c.MongoDBConnectionString = mongodb
	} else {
		c.MongoDBConnectionString = fmt.Sprintf("mongodb://%s:%s@%s:%s/", user, password, host, port)
	}

	zap.S().Debugf("MongoDBConnectionString: %s", c.MongoDBConnectionString)

	return nil
}
