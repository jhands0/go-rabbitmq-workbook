package main

import (
    "log"
    "os"
    "time"
    
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
    
    // Creating a connection, channel and declaring a channel queue
    conn, err := amqp.Dial(addr)
    failOnError(err, "Failed to connect to RabbitMQ")
    defer conn.Close()
    
    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")
    defer ch.Close()
    
    q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
    failOnError(err, "Failed to declare a queue")
    
    
    // Registered a consumer to receive messages
    msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
    failOnError(err, "Failed to register a consumer")
    
    // Creating a channel for all our threads to access
    var forever chan struct{}
    
    // Everytime a message is received, a new thread is spun up
    go func() {
        for d := range msgs {
            log.Printf("Received a message: %s", d.Body)
            dotCount := bytes.Count(d.body, []bytes("."))
            t := time.Duration(dotCount)
            time.Sleep(t * time.Second)
            log.Printf("Done")
        }
    }()
    
    log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
    <-forever
}
