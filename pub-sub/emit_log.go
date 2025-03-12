package main

import (
    "context"
    "log"
    "time"
    "os"
    "strings"
    
    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/joho/godotenv"
)

func failOnError(err error, msg string) {
    if err != nil {
        log.Panicf("%s: %s", msg, err)
    }
}

func bodyFrom(args []string) string {
    var s string
    if (len(args) < 2) || args[1] == "" {
        s = "hello"
    } else {
        s = strings.Join(args[1:], " ")
    }
    return s
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
    
    // Declaring an exchange and message to send
    err = ch.ExchangeDeclare("logs", "fanout", true, false, false, false, nil)
    failOnError(err, "Failed to declare an exchange")
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    body := bodyFrom(os.Args)
    message := amqp.Publishing{
        ContentType: "text/plain",
        Body:        []byte(body),
    }
    err = ch.PublishWithContext(ctx, "logs", "", false, false, message)
    failOnError(err, "Failed to publish a message")
    log.Printf(" [x] Sent %s\n", body)
}

