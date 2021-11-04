package key_value

type Event struct {
	Sequence  uint64    //a unique record id
	EventType EventType //the action taken
	Key       string    //the key affected by this transaction
	Value     string    //the value of a put the transaction
}
type EventType byte

const (
	_                     = iota //iota == 0
	EventDelete EventType = iota //iota == 1
	EventPut                     //iota ==2; implicitily repeat
)
