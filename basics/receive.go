package main

import (
    "log"
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
    
    // Creates a second thread to respond to all messages received
    go func() {
        for d := range msgs {
            log.Printf("Received a message: %s", d.Body)
        }
    }()
    
    log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
    <-forever
    
    
}
