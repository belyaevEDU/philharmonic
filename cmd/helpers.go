package cmd

import (
	"log"
	"os"
)

func errOsExit(err error) {
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
