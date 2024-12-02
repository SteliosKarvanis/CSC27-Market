FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.mod
COPY go.sum go.sum
COPY dbClient/ dbClient/
COPY utils/ utils/

RUN go mod download

RUN go build -o main dbClient/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/main .

CMD ["./main"]