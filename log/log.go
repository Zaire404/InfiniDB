package log

import (
	"fmt"
	"io"
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
	Logger = log.NewWithOptions(getLogOutput(), log.Options{
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

func getLogOutput() io.Writer {
	output := config.GetString("log.output")
	switch output {
	case "stdout":
		return os.Stdout
	case "stderr":
		return os.Stderr
	default:
		// Default to stderr if not specified
		return os.Stderr
	}
}

func HandleError(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		logMessage := fmt.Sprintf("<%s:%s> %s", file, HighlightString(RED, strconv.Itoa(line)), err.Error())
		Logger.Error(logMessage)
	}
}
