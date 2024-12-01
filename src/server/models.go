package main

type Request struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount"`
	User   string  `json:"user"`
}