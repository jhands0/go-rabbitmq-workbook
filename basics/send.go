package main

import (
    "context"
    "log"
    "time"
    "os"
    
    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/joho/godotenv"
)

func failOnError(err error, msg string) {
    if err != nil {
        log.Panicf("%s: %s", msg, err)
    }
}

func main() {
    // Loading RabbitMQ env variable
    err := godotenv.Load("../.env")
    failOnError(err, "Failed to load enviroment variables")
    addr := os.Getenv("RABBITMQ_ADDR")
    
    
    // Connecting to the RabbitMQ instance
    conn, err := amqp.Dial(addr)
    failOnError(err, "Failed to connect to RabbitMQ")
    defer conn.Close()
    
    // Creating a channel
    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")
    defer ch.Close()
    
    // Declaring a queue and message to send
    q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
    failOnError(err, "Failed to declare a queue")
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    body := "Hello World!"
    message := amqp.Publishing{
        ContentType: "text/plain",
        Body:        []byte(body),
    }
    err = ch.PublishWithContext(ctx, "", q.Name, false, false, message)
    failOnError(err, "Failed to publish a message")
    log.Printf(" [x] Sent %s\n", body)
}

