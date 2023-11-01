module mySqlMiner

go 1.20

require (
	dbMiner v0.0.0-00010101000000-000000000000
	github.com/go-sql-driver/mysql v1.7.1
)

replace dbMiner => ../db-miner
