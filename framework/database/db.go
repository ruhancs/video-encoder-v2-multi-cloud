package database

import (
	"encoder-service/domain"
	"log"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	_ "github.com/lib/pq"
)

// configura banco de dados, DB = database, DSN = string de conexao de producao
// DSNTest = string de conexao de teste, DBType = tipo do banco de dados,
// Debug = logs das querys, Automigrate = opcao para migrar automaticamente as entidades,
// Env = determina o ambiente
type Database struct {
	DB          *gorm.DB
	DSN         string
	DSNTest     string
	DBType      string
	DBTypeTest  string
	Debug       bool
	Automigrate bool
	Env         string
}

func NewDB() *Database {
	return &Database{}
}

// criar database de teste
func NewDBTest() *gorm.DB {
	dbInstance := NewDB()
	dbInstance.Env = "test"
	dbInstance.DBTypeTest = "sqlite3"
	dbInstance.DSNTest = ":memory:"
	dbInstance.Automigrate = true
	dbInstance.Debug = true

	connection, err := dbInstance.Connect()
	if err != nil {
		log.Fatalf("error to create test db: %v", err)
	}

	return connection
}

// faz a conexao do database, de teste ou producao
func (d *Database) Connect() (*gorm.DB, error) {
	var err error
	if d.Env != "test" {
		d.DB, err = gorm.Open(d.DBType, d.DSN)
	} else {
		d.DB, err = gorm.Open(d.DBTypeTest,d.DSNTest)
	}
	if err != nil {
		return nil, err
	}

	if d.Debug {
		d.DB.LogMode(true)
	}

	if d.Automigrate {
		d.DB.AutoMigrate(&domain.Video{}, &domain.Job{})
		d.DB.Model(domain.Job{}).AddForeignKey("video_id", "videos (id)", "CASCADE", "CASCADE")
	}

	return d.DB, nil
}
