package mysql

import (
	"context"
	"database/sql"

	"github.com/flandersrin/workflow-go/persistence/internal/sqlstore"
)

// Store 是 workflow-go 的 MySQL 默认持久化实现。
type Store = sqlstore.Store

// NewStore 创建 MySQL Store。
func NewStore(db *sql.DB) *Store {
	return sqlstore.New(db, sqlstore.MySQL)
}

// InitSchema 初始化 MySQL 表结构。生产环境可以改用迁移工具执行 Schema。
func InitSchema(ctx context.Context, store *Store) error {
	return store.InitSchema(ctx, Schema)
}
