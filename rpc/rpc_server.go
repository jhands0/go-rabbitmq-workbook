package main

import (
    "log"
    "context"
    "os"
    "time"
    "strconv"
    
    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/joho/godotenv"
)

func failOnError(err error, msg string) {
    if err != nil {
        log.Panicf("%s: %s", msg, err)
    }
}

func fib(n int) int {
    if n == 0 {
        return 0
    } else if n == 1 {
        return 1
    } else {
        return fib(n - 1) + fib(n - 2)
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
    
    q, err := ch.QueueDeclare("rpc_queue", false, false, false, false, nil)
    failOnError(err, "Failed to declare a queue")
    
    err = ch.Qos(1, 0, false)
    failOnError(err, "Failed to set QoS")
    
    // Registered a consumer to receive messages
    msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
    failOnError(err, "Failed to register a consumer")
    
    // Creating a channel for all our threads to access
    var forever chan struct{}
    
    // Creates a second thread to respond to all messages received
    go func() {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        for d := range msgs {
            n, err := strconv.Atoi(string(d.Body))
            failOnError(err, "Failed to convert body to integer")
            
            log.Printf(" [.] fib(%d)", n)
            response := fib(n)
            
            message := amqp.Publishing{
                    ContentType:    "text/plain",
                    CorrelationId:  d.CorrelationId,
                    Body:           []byte(strconv.Itoa(response)),
            }
            
            err = ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, message)
            failOnError(err, "Failed to publish a message")
            
            d.Ack(false)
        }
    }()
    
    log.Printf(" [*] Awaiting RPC requests")
    <-forever
}
