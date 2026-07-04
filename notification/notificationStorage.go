package notification

import (
	"sync"
)

type NotificationStorage struct {
	mu    sync.RWMutex
	items map[int]Notification
}

func New() *NotificationStorage {
	return &NotificationStorage{items: make(map[int]Notification)}
}

func Save(storage *NotificationStorage, notification Notification) int {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	id := len(storage.items)
	storage.items[id] = notification

	return id
}

func Update(id int, storage *NotificationStorage, notification Notification) int {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	storage.items[id] = notification

	return id
}

func Get(id int, storage *NotificationStorage) (Notification, bool) {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	item, ok := storage.items[id]
	return item, ok
}

func SetStatus(id int, status string, storage *NotificationStorage) {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	item, ok := storage.items[id]

	if ok {
		item.Status = status
		storage.items[id] = item
	}

}
