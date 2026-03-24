module github.com/kordar/goetl-gorm-lua-exec

go 1.22

require (
	github.com/kordar/goetl v0.0.2
	github.com/kordar/goetl-gorm v0.0.0
	github.com/yuin/gopher-lua v1.1.1
	gorm.io/gorm v1.25.12
)

require (
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/kordar/goetl => ../goetl

replace github.com/kordar/goetl-gorm => ../goetl-gorm
