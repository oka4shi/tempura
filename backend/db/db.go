package db

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/exp/slices"

	_ "github.com/mattn/go-sqlite3"

	"github.com/oka4shi/tempura/backend/logger"
)

const DB_PATH = "../data.db"

type dbStruct struct {
	Date int
	Temp float64
	HR   float64
}

type Data struct {
	Date []int
	Temp []float64
	HR   []float64
}

var sensors = strings.Split(os.Getenv("TEMPURA_SENSORS"), ",")

var (
	ErrAlreadyExist = errors.New("DB: A datum is exist for that date")
	ErrInvalidTarget = errors.New("DB: target table is invalid")
)
func migrate() {
	os.Remove(DB_PATH)

	db, err := sql.Open("sqlite3", DB_PATH)
	if err != nil {
		logger.Fatal(err)
	}
	defer db.Close()

	for _, s := range sensors {
		sqlStmt := fmt.Sprintf(`
		CREATE TABLE %s
		(
			date INTEGER NOT NULL PRIMARY KEY,
			temp REAL,
			hr REAL
		);
		DELETE FROM %s;
		`, s, s)
		_, err = db.Exec(sqlStmt)
		if err != nil {
			logger.Infof("%q: %s\n", err, sqlStmt)
		}
	}
}

type TrashScanner struct{}

func (TrashScanner) Scan(interface{}) error {
    return nil
}

func r(tx *sql.Tx) {
	if recover() != nil {
		tx.Rollback()
	}
}

func openDB(target string) (*sql.DB, error) {
	if !slices.Contains(sensors, target) {
		logger.Error(ErrInvalidTarget)
		return nil, ErrInvalidTarget
	}

	db, err := sql.Open("sqlite3", DB_PATH)
	if err != nil {
		logger.Error(err)
		db.Close()
		return nil, err
	}
	return db, nil
}

func startTx(db *sql.DB) (*sql.Tx, error) {
	tx, err := db.Begin()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return tx, nil

}

func AddData(target string, date int, temp float64, hr float64) error {
	db, err := openDB(target)
	if err != nil {
		logger.Error(err)
		return err
	}
	defer db.Close()

	tx, err := startTx(db)
	if err != nil {
		logger.Error(err)
		return err
	}
	defer r(tx)

	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s(date, temp, hr) VALUES(?, ?, ?)", target), date, temp, hr)
	if err != nil {
		logger.Error(err)
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

func GetData(target string, date int) (dbStruct, error) {
	db, err := openDB(target)
	if err != nil {
		logger.Error(err)
		return dbStruct{}, err
	}
	defer db.Close()

	err = db.QueryRow(fmt.Sprintf("SELECT date FROM %s WHERE date = ?", target), date).Scan(TrashScanner{})
	if err == nil {
		logger.Info(err)
		return dbStruct{}, ErrAlreadyExist
	} else if err != sql.ErrNoRows {
		logger.Error(err)
		return dbStruct{}, err
	}

	d := &dbStruct{}
	err = db.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE date = ?", target), date).Scan(&d.Date, &d.Temp, &d.HR)
	if err != nil {
		if err == sql.ErrNoRows {
			return dbStruct{}, nil
		} else {
			logger.Error(err)
			return dbStruct{}, err

		}
	}

	return *d, nil
}

func GetDataSet(target string, duration int, start int, limit int) (Data, error) {
	db, err := openDB(target)
	if err != nil {
		logger.Error(err)
		return Data{}, err
	}
	defer db.Close()

	var rows *sql.Rows
	d := Data{
		Date: []int{},
		Temp: []float64{},
		HR:   []float64{},
	}
	if limit == -1 {
		var c int
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", target)).Scan(&c)
		if err != nil {
			logger.Error(err)
			return Data{}, err
		}

		capa := c/(duration/(1000*60*5)) + 1
		logger.Info("capacity:", capa)
		d.Date = make([]int, 0, capa)
		d.Temp = make([]float64, 0, capa)
		d.HR = make([]float64, 0, capa)

		rows, err = db.Query(fmt.Sprintf("SELECT * FROM %s WHERE MOD(date, ?) = ? ORDER BY date DESC",
			target), duration, start%duration)
	} else {
		d.Date = make([]int, 0, limit)
		d.Temp = make([]float64, 0, limit)
		d.HR = make([]float64, 0, limit)

		rows, err = db.Query(fmt.Sprintf("SELECT * FROM %s WHERE MOD(date, ?) = ? ORDER BY date DESC LIMIT ?",
			target), duration, start%duration, limit)
	}

	if err != nil {
		logger.Error(err)
		return Data{}, err
	}
	defer rows.Close()

	for i := 0; rows.Next(); i++ {
		d.Date = append(d.Date, 0)
		d.Temp = append(d.Temp, 0.0)
		d.HR = append(d.HR, 0.0)
		err = rows.Scan(&d.Date[i], &d.Temp[i], &d.HR[i])
		if err != nil {
			logger.Error(err)
			return Data{}, err
		}
	}
	err = rows.Err()
	if err != nil {
		logger.Error(err)
		return Data{}, err
	}

	logger.Info("data: ", d)
	return d, nil
}
