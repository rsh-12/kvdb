package types

type Iterator interface {
	HasNext() bool
	Next() (item Item, err error)
	Close() error
}

type Item struct {
	Key   string
	Value string
}
