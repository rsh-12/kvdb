package types

type Iterator interface {
	HasNext() bool
	Next() (item Item, err error)
	Close() error
}

type Item struct {
	Key   string `json:"key" validate:"required"`
	Value string `json:"value" validate:"required"`
}
