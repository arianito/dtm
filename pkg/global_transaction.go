package dtm

import (
	"database/sql"
	"errors"
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/broker"
	"log"
	"time"
)

const GTChannel = "global-transactions"

type GTService struct {
	gt *GlobalTransaction
}

type Transaction struct {
	tx        *sql.Tx
	expiresAt time.Time
}

type Result struct {
	LastInsertedId int64
	RowsAffected   int64
}

type GlobalTransaction struct {
	srv          micro.Service
	db           *sql.DB
	transactions map[string]*Transaction
	broker       broker.Broker
	mutex        *Mutex
}

func CreateGlobalTransactionManager(srv micro.Service, db *sql.DB) (*GlobalTransaction, error) {
	trs := make(map[string]*Transaction)
	b := srv.Options().Broker
	server := srv.Server()

	gt := &GlobalTransaction{
		transactions: trs, db: db, broker: b, srv: srv,
		mutex: &Mutex{},
	}

	err := server.Handle(server.NewHandler(&GTService{gt: gt}))
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			time.Sleep(time.Second * 10)
			if unlock, err := WithLock(gt.mutex); err == nil {

				for key, value := range gt.transactions {
					if value.expiresAt.Before(time.Now()) {
						err := gt.rollbackLocal(key)
						if err != nil {
							log.Println("rollback failed -> ", key, err.Error())
						}
					}
				}
				unlock()
			} else {
				log.Println("failed to acquire lock")
			}
		}
	}()

	_, err = b.Subscribe(GTChannel, func(e broker.Event) error {
		head := e.Message().Header
		id := string(e.Message().Body)
		if head["action"] == "commit" {
			return gt.commitLocal(id)
		} else if head["action"] == "rollback" {
			return gt.rollbackLocal(id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return gt, nil

}

func (t *GlobalTransaction) Exec(id string, query string, args ...interface{}) (*Result, error) {
	if unlock, err := WithLock(t.mutex); err == nil {
		defer unlock()

		tx, ok := t.transactions[id]
		if !ok {
			txx, err := t.db.Begin()
			if err != nil {
				return nil, err
			}
			tx = &Transaction{
				tx:        txx,
				expiresAt: time.Now().Add(time.Minute * 5),
			}
			t.transactions[id] = tx
		}

		result, err := tx.tx.Exec(query, args...)
		if err != nil {
			return nil, err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		lastId, err := result.LastInsertId()
		if err != nil {
			return nil, err
		}
		return &Result{
			LastInsertedId: lastId,
			RowsAffected:   rows,
		}, nil
	} else {
		return nil, err
	}
}

func (t *GlobalTransaction) Rollback(id string) error {
	return t.broker.Publish(GTChannel, &broker.Message{
		Header: map[string]string{
			"action": "rollback",
		},
		Body: []byte(id),
	})
}
func (t *GlobalTransaction) Commit(id string) error {
	return t.broker.Publish(GTChannel, &broker.Message{
		Header: map[string]string{
			"action": "commit",
		},
		Body: []byte(id),
	})
}
func (t *GlobalTransaction) rollbackLocal(id string) error {
	if tx, ok := t.transactions[id]; ok {
		err := tx.tx.Rollback()
		if err != nil {
			return err
		}
		delete(t.transactions, id)
		return nil
	}
	return errors.New("transaction " + id + " not found")
}

func (t *GlobalTransaction) commitLocal(id string) error {
	if tx, ok := t.transactions[id]; ok {
		err := tx.tx.Commit()
		if err != nil {
			return err
		}
		delete(t.transactions, id)
		return nil
	}
	return errors.New("transaction " + id + " not found")
}
