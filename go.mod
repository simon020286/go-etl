module go-etl

go 1.23.8

replace go-etl-sdk => ./plugins/sdk

require (
	github.com/dop251/goja v0.0.0-20250531102226-cb187b08699c
	github.com/gorilla/websocket v1.5.3
	github.com/mattn/go-sqlite3 v1.14.32
	go-etl-sdk v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/dlclark/regexp2 v1.11.4 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	golang.org/x/text v0.3.8 // indirect
)
