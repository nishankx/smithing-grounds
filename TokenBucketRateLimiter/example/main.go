package main

import (
	"fmt"
	"log"
	"time"

	tokenbucket "github.com/nishankx/smithing-grounds/TokenBucketRateLimiter"
)

func main() {
	// Create a limiter:
	// Maximum capacity: 5 tokens
	// Refill rate: 2 tokens per second.
	limiter, err := tokenbucket.NewLimiter(5, 2)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Token Bucket Rate Limiter")
	fmt.Println("-------------------------")

	// Send 10 requests.
	for i := 1; i <= 10; i++ {
		if limiter.Allow() {
			fmt.Printf("Request %02d: ALLOWED\n", i)
		} else {
			fmt.Printf("Request %02d: REJECTED\n", i)
		}
	}

	fmt.Printf("\nRemaining tokens: %.2f\n", limiter.Tokens())

	// Wait for tokens to replenish.
	fmt.Println("\nWaiting for 2 seconds...")

	time.Sleep(2 * time.Second)

	fmt.Printf("Available tokens: %.2f\n", limiter.Tokens())

	// Try another request.
	if limiter.Allow() {
		fmt.Println("New request: ALLOWED")
	} else {
		fmt.Println("New request: REJECTED")
	}

	fmt.Printf("Remaining tokens: %.2f\n", limiter.Tokens())

	// Demonstrate batch consumption.
	fmt.Println("\nAttempting batch consumption...")

	if limiter.AllowN(3) {
		fmt.Println("Batch of 3 requests: ALLOWED")
	} else {
		fmt.Println("Batch of 3 requests: REJECTED")
	}

	fmt.Printf("Final token count: %.2f\n", limiter.Tokens())
}
