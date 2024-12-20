package postgres

import (
	"database/sql"
	"fmt"
	"regexp"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type PostgresConfig struct {
	Host                string
	Port                int
	Username            string
	Password            string
	DbName              string
	CreateDbIfNotExists bool
	SchemaName          string
}

type Postgres struct {
	Db *sql.DB
}

func GetTestPostgresDatabase(cfg config.DatabaseConfig, gCfg *config.Config, l *zap.Logger) (
	string,
	*sql.DB,
	*gorm.DB,
	error,
) {
	testDbName, err := tests.GenerateTestDbName()
	if err != nil {
		return testDbName, nil, nil, err
	}
	cfg.DbName = testDbName

	pgConfig := PostgresConfigFromDbConfig(&cfg)
	pgConfig.CreateDbIfNotExists = true

	pg, err := NewPostgres(pgConfig)
	if err != nil {
		return testDbName, nil, nil, err
	}

	grm, err := NewGormFromPostgresConnection(pg.Db)
	if err != nil {
		return testDbName, nil, nil, err
	}

	migrator := migrations.NewMigrator(pg.Db, grm, l, gCfg)
	if err = migrator.MigrateAll(); err != nil {
		return testDbName, nil, nil, err
	}
	return testDbName, pg.Db, grm, nil
}

func PostgresConfigFromDbConfig(dbCfg *config.DatabaseConfig) *PostgresConfig {
	return &PostgresConfig{
		Host:       dbCfg.Host,
		Port:       dbCfg.Port,
		Username:   dbCfg.User,
		Password:   dbCfg.Password,
		DbName:     dbCfg.DbName,
		SchemaName: dbCfg.SchemaName,
	}
}

func getPostgresRootConnection(cfg *PostgresConfig) (*sql.DB, error) {
	postgresConnStr := getPostgresConnectionString(&PostgresConfig{
		Host:     cfg.Host,
		Port:     cfg.Port,
		Username: cfg.Username,
		Password: cfg.Password,
		DbName:   "postgres",
	})

	postgresDB, err := sql.Open("postgres", postgresConnStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres database: %v", err)
	}
	return postgresDB, nil
}

func getPostgresConnectionString(cfg *PostgresConfig) string {
	authString := ""
	if cfg.Username != "" {
		authString = fmt.Sprintf("%s user=%s", authString, cfg.Username)
	}
	if cfg.Password != "" {
		authString = fmt.Sprintf("%s password=%s", authString, cfg.Password)
	}
	baseString := fmt.Sprintf("host=%s %s dbname=%s port=%d sslmode=disable TimeZone=UTC",
		cfg.Host,
		authString,
		cfg.DbName,
		cfg.Port,
	)
	if cfg.SchemaName != "" {
		baseString = fmt.Sprintf("%s search_path=%s", baseString, cfg.SchemaName)
	}
	return baseString
}

func DeleteTestDatabase(cfg *PostgresConfig, dbName string) error {
	postgresDB, err := getPostgresRootConnection(cfg)
	if err != nil {
		return err
	}
	defer postgresDB.Close()

	query := fmt.Sprintf("DROP DATABASE %s", dbName)
	_, err = postgresDB.Exec(query)
	if err != nil {
		return fmt.Errorf("error dropping database: %v", err)
	}
	fmt.Printf("Database '%s' dropped successfully\n", dbName)
	return nil
}

func CreateDatabaseIfNotExists(cfg *PostgresConfig) error {
	fmt.Printf("Creating database if not exists '%s'...\n", cfg.DbName)

	postgresDB, err := getPostgresRootConnection(cfg)
	if err != nil {
		return err
	}
	defer postgresDB.Close()

	// Check if database exists
	var exists bool
	query := fmt.Sprintf(`SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = '%s');`, cfg.DbName)
	err = postgresDB.QueryRow(query).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if database exists: %v", err)
	}

	// Create database if it doesn't exist
	if !exists {
		query = fmt.Sprintf("CREATE DATABASE %s", cfg.DbName)
		_, err = postgresDB.Exec(query)
		if err != nil {
			return fmt.Errorf("error creating database: %v", err)
		}
		fmt.Printf("Database '%s' created successfully\n", cfg.DbName)
	}
	return nil
}

func NewPostgres(cfg *PostgresConfig) (*Postgres, error) {
	if cfg.CreateDbIfNotExists {
		if err := CreateDatabaseIfNotExists(cfg); err != nil {
			return nil, fmt.Errorf("Failed to create database if not exists %+v", err)
		}
	}
	connectString := getPostgresConnectionString(cfg)

	db, err := sql.Open("postgres", connectString)
	if err != nil {
		return nil, fmt.Errorf("Failed to setup database %+v", err)
	}

	return &Postgres{
		Db: db,
	}, nil
}

func NewGormFromPostgresConnection(pgDb *sql.DB) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: pgDb,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to setup database %+v", err)
	}

	return db, nil
}

func TeardownTestDatabase(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func IsDuplicateKeyError(err error) bool {
	r := regexp.MustCompile(`duplicate key value violates unique constraint`)

	return r.MatchString(err.Error())
}
