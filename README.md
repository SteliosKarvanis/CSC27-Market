# CSC27-Market

This project represents a system of a store or market where handles multiples concurrent requests of buying and selling, requiring to verify the inventory before settle a transactions

## Run Locally on Docker
```
sudo docker-compose up --build
```
> Actually it's with a bug that the database client cound't be add to the docker, so requiring to run separately:
```
cd src && go run dbClient/main.go
``` 

# Auxiliar Scripts
* `Database Migration`: The tables are defined by the basemodels on `src/utils/dtypes/models.go`. To modify the DB doing a migration run:
```
go run migrateDb/main.go
```
* `Test Notebook`: `test.ipynb` has the logic to send requests to the endpoints
* `Query Tables`: There is an auxiliar script on `src/queryTable/main.go` that implements how to query the db, modify it for debug and analyse it
```
go run queryTable/main.go
```