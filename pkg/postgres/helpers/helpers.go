package helpers

import "gorm.io/gorm"

// WrapTxAndCommit executes a database function within a transaction and handles
// transaction commit or rollback based on the function's result.
//
// This is a generic function that works with any return type T. If a transaction (tx)
// is provided, it uses that transaction. Otherwise, it creates a new transaction,
// executes the function, and commits or rolls back based on whether an error occurred.
//
// Type Parameters:
//   - T: The return type of the function
//
// Parameters:
//   - fn: The function to execute within the transaction
//   - db: The database connection
//   - tx: An optional existing transaction (can be nil)
//
// Returns:
//   - T: The result from the executed function
//   - error: Any error encountered
func WrapTxAndCommit[T any](fn func(*gorm.DB) (T, error), db *gorm.DB, tx *gorm.DB) (T, error) {
	exists := tx != nil

	if !exists {
		tx = db.Begin()
	}

	res, err := fn(tx)

	if err != nil && !exists {
		tx.Rollback()
	}
	if err == nil && !exists {
		tx.Commit()
	}
	return res, err
}
