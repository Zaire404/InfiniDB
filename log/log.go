package log

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/Zaire404/ZDB/config"
	"github.com/charmbracelet/log"
)

var (
	Logger *log.Logger
	once   sync.Once
)

func Init() {
	once.Do(func() {
		initialize()
	})
}

func initialize() {
	Logger = log.NewWithOptions(os.Stderr, log.Options{
		ReportCaller:    true,
		ReportTimestamp: true,
		TimeFormat:      time.RFC3339Nano,
		Prefix:          "ZDB",
	})

	level := config.GetString("log.level")
	switch level {
	case "debug":
		Logger.SetLevel(log.DebugLevel)
	case "info":
		Logger.SetLevel(log.InfoLevel)
	case "warn":
		Logger.SetLevel(log.WarnLevel)
	case "error":
		Logger.SetLevel(log.ErrorLevel)
	default:
		Logger.SetLevel(log.DebugLevel)
	}
	Logger.Debugf("log level: %v", level)
}

func HandleError(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		logMessage := fmt.Sprintf("<%s:%s> %s", file, HighlightString(RED, strconv.Itoa(line)), err.Error())
		Logger.Error(logMessage)
	}
}
