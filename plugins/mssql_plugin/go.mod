module mssql_plugin

go 1.23.8

replace go-etl-sdk => ../sdk

require (
go-etl-sdk v0.0.0-20241022120000-000000000000
github.com/denisenkom/go-mssqldb v0.12.3
)

require (
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	golang.org/x/crypto v0.38.0 // indirect
)
