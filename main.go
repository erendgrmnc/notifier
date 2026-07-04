package main

// Notification service — write your implementation here.
//
// Goal (see ../prep/insider_interview_prep.md §3 and ../README.md):
//   POST /notifications        enqueue a notification
//   GET  /notifications/{id}    read its status
//   a pool of worker goroutines delivers queued notifications off a channel
//   graceful shutdown on Ctrl+C
//
// Concepts to exercise: goroutines, channels + select, context.Context,
// an implicit interface for the store, error handling, defer, sync.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"notifier/notification"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var notifciationChannel = make(chan int, 100)
var notificationStorage = notification.New()
var server http.Server

func enqueue(w http.ResponseWriter, req *http.Request) {
	fmt.Println("enqueue hit!")

	defer req.Body.Close()
	var requestBody notification.PostNotificationRequest
	err := json.NewDecoder(req.Body).Decode(&requestBody)

	if err != nil {
		fmt.Println("decode error:", err)
		http.Error(w, "Could not read request body", http.StatusBadRequest)
		return
	}

	var newNot notification.Notification

	newNot.Date = time.Now().String()
	newNot.Message = string(requestBody.Content)
	newNot.Status = "queued"

	notifciationId := notification.Save(notificationStorage, newNot)

	notifciationChannel <- notifciationId

	w.WriteHeader(http.StatusOK)
}

func dequeue(w http.ResponseWriter, req *http.Request) {
	fmt.Println("dequeue hit!")

	defer req.Body.Close()
	idStr := req.PathValue("id")
	id, err := strconv.Atoi(idStr)

	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	reqNotif, isReqExisting := notification.Get(id, notificationStorage)

	if !isReqExisting {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	response := notification.CreateNotificationResponse(reqNotif)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)

}

func gracefulShutdown(context context.Context, workerGroup *sync.WaitGroup) {
	fmt.Println("Application Running. Press Ctrl + C to exit...")

	<-context.Done()

	server.Shutdown(context)
	close(notifciationChannel)
	workerGroup.Wait()
	fmt.Println("Main: shutdown signal received, exiting gracefully.")
}

func startServer() {
	http.HandleFunc("POST /notifications", enqueue)
	http.HandleFunc("GET /notifications/{id}", dequeue)
	server = http.Server{Addr: ":8090"}
	err := server.ListenAndServe()

	if err != nil {
		fmt.Println(err)
	}
}

func main() {
	context, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Println("Starting server on port 8090")

	workerCount := runtime.NumCPU()
	workerGroup := notification.ProcessNotifications(workerCount, notifciationChannel, notificationStorage)

	go startServer()
	gracefulShutdown(context, workerGroup)

}
