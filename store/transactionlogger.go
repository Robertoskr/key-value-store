package key_value

//transaction log interface redux
type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() (<-chan Event, <-chan error)

	Run()
}
