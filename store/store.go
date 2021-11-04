package key_value

import (
	"errors"
	"fmt"
	"sync"
)

var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

func Put(key string, value string) error {
	store.RLock()
	store.m[key] = value
	fmt.Println(store.m)
	store.RUnlock()

	return nil
}

var ErrorNoSuchKey = errors.New("no such key")

func Get(key string) (string, error) {
	store.RLock()
	value, ok := store.m[key]
	store.RUnlock()
	if !ok {
		return "", ErrorNoSuchKey
	}
	return value, nil
}

func Delete(key string) error {
	store.RLock()
	delete(store.m, key)
	store.RUnlock()
	return nil
}
