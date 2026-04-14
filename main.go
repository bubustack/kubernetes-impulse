package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	sdk "github.com/bubustack/bubu-sdk-go"

	"github.com/bubustack/kubernetes-impulse/pkg/impulse"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := sdk.RunImpulse(ctx, impulse.New()); err != nil {
		log.Fatalf("kubernetes impulse failed: %v", err)
	}
}
