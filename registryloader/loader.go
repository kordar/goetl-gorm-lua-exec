package registryloader

import (
	"context"

	"github.com/kordar/goetl-gorm-lua-exec/scriptstore"
	"gorm.io/gorm"
)

type Loader interface {
	Load(ctx context.Context, db *gorm.DB, reg *scriptstore.Registry) error
}

