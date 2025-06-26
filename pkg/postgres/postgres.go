package postgres

import (
	"database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const defaultSSLMode = "disable"

var validSSLModes = []string{
	"disable",
	"require",
	"verify-ca",
	"verify-full",
}

// PostgresConfig contains all configuration parameters needed to establish
// a connection to a PostgreSQL database.
type PostgresConfig struct {
	// Host is the PostgreSQL server hostname or IP address
	Host string
	// Port is the PostgreSQL server port number
	Port int
	// Username for authentication with the PostgreSQL server
	Username string
	// Password for authentication with the PostgreSQL server
	Password string
	// DbName is the name of the database to connect to
	DbName string
	// CreateDbIfNotExists indicates whether to create the database if it doesn't exist
	CreateDbIfNotExists bool
	// SchemaName specifies the schema to use within the database
	SchemaName string
	// SSLMode specifies the SSL mode for the connection (disable, require, verify-ca, verify-full)
	SSLMode string
	// SSLCert is the path to the client certificate file
	SSLCert string
	// SSLKey is the path to the client private key file
	SSLKey string
	// SSLRootCert is the path to the root certificate file
	SSLRootCert string
}

// Postgres represents a connection to a PostgreSQL database.
type Postgres struct {
	// Db is the underlying SQL database connection
	Db *sql.DB
}

// GetTestPostgresDatabase creates a test database with migrations applied.
// It returns the test database name, a sql.DB connection, a gorm.DB connection,
// and any error encountered.
//
// Parameters:
//   - cfg: Database configuration
//   - gCfg: Global configuration
//   - l: Logger
//
// Returns:
//   - string: Test database name
//   - *sql.DB: SQL database connection
//   - *gorm.DB: GORM database connection
//   - error: Any error encountered
func GetTestPostgresDatabase(cfg config.DatabaseConfig, gCfg *config.Config, l *zap.Logger) (
	string,
	*sql.DB,
	*gorm.DB,
	error,
) {
	testDbName, pg, grm, err := GetTestPostgresDatabaseWithoutMigrations(cfg, l)
	if err != nil {
		return testDbName, nil, nil, err
	}

	migrator := migrations.NewMigrator(pg, grm, l, gCfg)
	if err = migrator.MigrateAll(); err != nil {
		return testDbName, nil, nil, err
	}

	return testDbName, pg, grm, nil
}

// GetTestPostgresDatabaseWithoutMigrations creates a test database without applying migrations.
// It returns the test database name, a sql.DB connection, a gorm.DB connection,
// and any error encountered.
//
// Parameters:
//   - cfg: Database configuration
//   - l: Logger
//
// Returns:
//   - string: Test database name
//   - *sql.DB: SQL database connection
//   - *gorm.DB: GORM database connection
//   - error: Any error encountered
func GetTestPostgresDatabaseWithoutMigrations(cfg config.DatabaseConfig, l *zap.Logger) (
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

	return testDbName, pg.Db, grm, nil
}

// PostgresConfigFromDbConfig converts a DatabaseConfig to a PostgresConfig.
//
// Parameters:
//   - dbCfg: Database configuration
//
// Returns:
//   - *PostgresConfig: PostgreSQL configuration
func PostgresConfigFromDbConfig(dbCfg *config.DatabaseConfig) *PostgresConfig {
	return &PostgresConfig{
		Host:        dbCfg.Host,
		Port:        dbCfg.Port,
		Username:    dbCfg.User,
		Password:    dbCfg.Password,
		DbName:      dbCfg.DbName,
		SchemaName:  dbCfg.SchemaName,
		SSLMode:     dbCfg.SSLMode,
		SSLCert:     dbCfg.SSLCert,
		SSLKey:      dbCfg.SSLKey,
		SSLRootCert: dbCfg.SSLRootCert,
	}
}

// getPostgresRootConnection establishes a connection to the PostgreSQL server's
// root 'postgres' database for administrative operations.
//
// Parameters:
//   - cfg: PostgreSQL configuration
//
// Returns:
//   - *sql.DB: Database connection
//   - error: Any error encountered
func getPostgresRootConnection(cfg *PostgresConfig) (*sql.DB, error) {
	postgresConnStr, err := getPostgresConnectionString(&PostgresConfig{
		Host:        cfg.Host,
		Port:        cfg.Port,
		Username:    cfg.Username,
		Password:    cfg.Password,
		DbName:      "postgres",
		SSLMode:     cfg.SSLMode,
		SSLCert:     cfg.SSLCert,
		SSLKey:      cfg.SSLKey,
		SSLRootCert: cfg.SSLRootCert,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection string: %v", err)
	}

	postgresDB, err := sql.Open("postgres", postgresConnStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres database: %v", err)
	}
	return postgresDB, nil
}

// getPostgresConnectionString builds a PostgreSQL connection string from the provided configuration.
//
// Parameters:
//   - cfg: PostgreSQL configuration
//
// Returns:
//   - string: Connection string
//   - error: Any error encountered during string construction
func getPostgresConnectionString(cfg *PostgresConfig) (string, error) {
	authString := ""
	sslMode := defaultSSLMode

	if cfg.Username != "" {
		authString = fmt.Sprintf("%s user=%s", authString, cfg.Username)
	}
	if cfg.Password != "" {
		authString = fmt.Sprintf("%s password=%s", authString, cfg.Password)
	}

	if cfg.SSLMode != "" {
		if !slices.Contains(validSSLModes, cfg.SSLMode) {
			return "", fmt.Errorf("invalid ssl mode: %s. Must be one of: %s", cfg.SSLMode, strings.Join(validSSLModes, ", "))
		}
		sslMode = cfg.SSLMode
	}

	baseString := fmt.Sprintf("host=%s %s dbname=%s port=%d sslmode=%s TimeZone=UTC",
		cfg.Host,
		authString,
		cfg.DbName,
		cfg.Port,
		sslMode,
	)

	if cfg.SchemaName != "" {
		baseString = fmt.Sprintf("%s search_path=%s", baseString, cfg.SchemaName)
	}

	if sslMode != defaultSSLMode {
		if cfg.SSLCert != "" {
			baseString = fmt.Sprintf("%s sslcert=%s", baseString, cfg.SSLCert)
		}
		if cfg.SSLKey != "" {
			baseString = fmt.Sprintf("%s sslkey=%s", baseString, cfg.SSLKey)
		}
		if cfg.SSLRootCert != "" {
			baseString = fmt.Sprintf("%s sslrootcert=%s", baseString, cfg.SSLRootCert)
		}
	}
	return baseString, nil
}

// DeleteTestDatabase drops a test database.
//
// Parameters:
//   - cfg: PostgreSQL configuration
//   - dbName: Name of the database to delete
//
// Returns:
//   - error: Any error encountered
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

// CreateDatabaseIfNotExists creates a new database if it doesn't already exist.
//
// Parameters:
//   - cfg: PostgreSQL configuration containing the database name to create
//
// Returns:
//   - error: Any error encountered
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

// NewPostgres creates a new Postgres instance with an established database connection.
//
// Parameters:
//   - cfg: PostgreSQL configuration
//
// Returns:
//   - *Postgres: Postgres instance with an active connection
//   - error: Any error encountered
func NewPostgres(cfg *PostgresConfig) (*Postgres, error) {
	if cfg.CreateDbIfNotExists {
		if err := CreateDatabaseIfNotExists(cfg); err != nil {
			return nil, fmt.Errorf("Failed to create database if not exists %+v", err)
		}
	}
	connectString, err := getPostgresConnectionString(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection string: %v", err)
	}

	db, err := sql.Open("postgres", connectString)
	if err != nil {
		return nil, fmt.Errorf("Failed to setup database %+v", err)
	}

	return &Postgres{
		Db: db,
	}, nil
}

// NewGormFromPostgresConnection creates a new GORM DB instance from an existing PostgreSQL connection.
//
// Parameters:
//   - pgDb: Existing PostgreSQL database connection
//
// Returns:
//   - *gorm.DB: GORM database instance
//   - error: Any error encountered
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

// TeardownTestDatabase closes the database connection and drops the test database.
//
// Parameters:
//   - dbname: Name of the test database to tear down
//   - cfg: Global configuration
//   - db: GORM database connection
//   - l: Logger
func TeardownTestDatabase(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

// IsDuplicateKeyError checks if an error is a PostgreSQL duplicate key violation error.
//
// Parameters:
//   - err: Error to check
//
// Returns:
//   - bool: True if the error is a duplicate key violation
func IsDuplicateKeyError(err error) bool {
	r := regexp.MustCompile(`duplicate key value violates unique constraint`)

	return r.MatchString(err.Error())
}
