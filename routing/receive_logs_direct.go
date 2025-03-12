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
    
    err = ch.ExchangeDeclare("logs_direct", "direct", true, false, false, false, nil)
    failOnError(err, "Failed to declare an exchange")
    
    q, err := ch.QueueDeclare("", false, false, true, false, nil)
    failOnError(err, "Failed to declare a queue")
    
    if len(os.Args) < 2 {
        log.Printf("Usage: %s [info] [warning] [error]", os.Args[0])
        os.Exit(0)
    }
    
    for _, s := range os.Args[1:] {
        log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, "logs_direct", s)
        err = ch.QueueBind(q.Name, s, "logs_direct", false, nil)
        failOnError(err, "Failed to bind a queue")
    }
    
    // Registered a consumer to receive logs
    logs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
    failOnError(err, "Failed to register a consumer")
    
    // Creating a channel for all our threads to access
    var forever chan struct{}
    
    // Creates a second thread to respond to all logs received
    go func() {
        for d := range logs {
            log.Printf(" [x] %s", d.Body)
        }
    }()
    
    log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
    <-forever
}
