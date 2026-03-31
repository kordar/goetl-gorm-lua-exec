package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/kordar/goetl-gorm-lua-exec/registryloader"
	"github.com/kordar/goetl-gorm-lua-exec/scriptstore"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	dsn := strings.TrimSpace(os.Getenv("MYSQL_DSN"))
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "MYSQL_DSN is required")
		os.Exit(2)
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	reg := scriptstore.NewRegistry()
	ldr := registryloader.NewGormLoader()
	if err := ldr.Load(context.Background(), db, reg); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	keys := reg.Keys()
	fmt.Printf("loaded %d keys\n", len(keys))
	for _, k := range keys {
		fmt.Printf("%s: %d scripts\n", k, len(reg.Get(k)))
	}
}

