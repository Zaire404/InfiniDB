package config

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var (
	config *viper.Viper
	once   sync.Once
)

func Init() {
	once.Do(func() {
		initialize()
	})
}

func initialize() {
	config = viper.New()
	config.SetConfigName("conf")
	config.AddConfigPath("./conf/")
	config.AddConfigPath("./")
	config.SetConfigType("yml")
	config.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	config.SetEnvKeyReplacer(replacer)
	config.WatchConfig()
	config.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("Config file changed:", e.Name)
	})

	if err := config.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if errors.As(err, &configFileNotFoundError) {
			fmt.Println("config file not found use default config")
			config.SetDefault("log", map[string]interface{}{
				"level": "debug",
				"mode":  []string{"console", "file"},
				"path":  "./logs",
			})
		} else {
			fmt.Println("config file error")
		}
	}
}

func Get(key string) interface{} {
	return config.Get(key)
}

func GetString(key string) string {
	return config.GetString(key)
}
