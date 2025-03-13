package main

import (
    "context"
    "log"
    "math/rand"
    "time"
    "os"
    "strconv"
    "strings"
    
    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/joho/godotenv"
)

func failOnError(err error, msg string) {
    if err != nil {
        log.Panicf("%s: %s", msg, err)
    }
}

func randomString(l int) string {
    bytes := make([]byte, l)
    for i := 0; i < l; i++ {
        bytes[i] = byte(randInt(65, 90))
    }
    return string(bytes)
}

func randInt(min int, max int) int {
    return min + rand.Intn(max - min)
}

func bodyFrom(args []string) int {
    var s string
    if (len(args) < 2) || args[1] == "" {
        s = "30"
    } else {
        s = strings.Join(args[1:], " ")
    }
    n, err := strconv.Atoi(s)
    failOnError(err, "Failed to convert arg to integer")
    return n
}

func fibonacciRPC(n int, addr string) (res int, err error) {
    // Connecting to the RabbitMQ instance
    conn, err := amqp.Dial(addr)
    failOnError(err, "Failed to connect to RabbitMQ")
    defer conn.Close()
    
    // Creating a channel
    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")
    defer ch.Close()
    
    // Declaring a queue and message to send
    q, err := ch.QueueDeclare("", true, false, false, false, nil)
    failOnError(err, "Failed to declare a queue")
    
    msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
    failOnError(err, "Failed to register a consumer")
    
    corrId := randomString(32)
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    message := amqp.Publishing{
        DeliveryMode:   amqp.Persistent,
        CorrelationId:  corrId,
        ContentType:    "text/plain",
        Body:           []byte(strconv.Itoa(n)),
    }
    err = ch.PublishWithContext(ctx, "", "rpc_queue", false, false, message)
    failOnError(err, "Failed to publish a message")
    
    for d := range msgs {
        if corrId == d.CorrelationId {
            res, err = strconv.Atoi(string(d.Body))
            failOnError(err, "Failed to convert body to integer")
            break
        }
    }
    
    return
}

func main() {
    // Loading RabbitMQ env variable
    err := godotenv.Load("../.env")
    failOnError(err, "Failed to load enviroment variables")
    addr := os.Getenv("RABBITMQ_ADDR")
    
    rand.Seed(time.Now().UTC().UnixNano())
    
    n := bodyFrom(os.Args)
    
    log.Printf(" [x] Requesting fib(%d)", n)
    res, err := fibonacciRPC(n, addr)
    failOnError(err, "Failed to handle RPC request")
    
    log.Printf(" [.] Got %d", res)
}

