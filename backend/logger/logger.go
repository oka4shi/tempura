package logger

import (
	"log"
	"os"
	"strings"
)

var DEBUG = strings.ToLower(os.Getenv("TEMPURA_DEBUG"))

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func Fatal(v ...any) {
	log.Fatal("[Fatal]", v)
}
func Faltalf(format string, v ...any) {
	log.Fatalf("[Fatal]"+format, v...)
}

func Error(v ...any) {
	log.Print("[Error]", v)
}
func Errorf(format string, v ...any) {
	log.Fatalf("[Error]"+format, v...)
}

func Info(v ...any) {
	if DEBUG == "true" {
		log.Print("[Info]", v)
	}
}
func Infof(format string, v ...any) {
	if DEBUG == "true" {
		log.Printf("[Info]"+format, v...)
	}
}
