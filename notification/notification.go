package notification

import (
	"fmt"
	"sync"
)

type Notification struct {
	Date    string
	Message string
	Status  string
}

type GetNotificationResponse struct {
	Notification string `json:"notification"`
}

type PostNotificationRequest struct {
	Content string `json:"content"`
}

func worker(queue <-chan int, notifications *NotificationStorage, workerGroup *sync.WaitGroup) {
	defer workerGroup.Done()
	for id := range queue {

		notification, isReqExisting := Get(int(id), notifications)
		if isReqExisting {
			fmt.Println("Notification processed, id:", id)
			notification.Status = "processed"
			Update(id, notifications, notification)
		}
	}
}

func ProcessNotifications(workerCount int, queue <-chan int, notifications *NotificationStorage) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(queue, notifications, &wg)
	}

	return &wg
}

func GetContent(notification *Notification) (content string) {

	content = notification.Message + " - Status: " + notification.Status + "[" + notification.Date + "]"
	return
}

func CreateNotificationResponse(notification Notification) (response GetNotificationResponse) {
	response.Notification = GetContent(&notification)

	return
}
