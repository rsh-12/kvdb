package errors

const (
	ErrNotFound  = LsmErr("value not found")
	ErrTombstone = LsmErr("value was deleted")
)

type LsmErr string

func (l LsmErr) Error() string {
	return string(l)
}
