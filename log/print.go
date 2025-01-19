package log

import "encoding/json"

type Color string

var colorMap = map[Color]string{
	"green":   "\033[97;42m",
	"white":   "\033[90;47m",
	"yellow":  "\033[90;43m",
	"red":     "\033[97;41m",
	"blue":    "\033[97;44m",
	"magenta": "\033[97;45m",
	"cyan":    "\033[97;46m",
	"reset":   "\033[0m",
}

const (
	GREEN   Color = "green"
	WHITE   Color = "white"
	YELLOW  Color = "yellow"
	RED     Color = "red"
	BLUE    Color = "blue"
	MAGENTA Color = "magenta"
	CYAN    Color = "cyan"
	RESET   Color = "reset"
)

func HighlightString(color Color, str string) string {
	if _, ok := colorMap[color]; !ok {
		return colorMap["green"] + str + colorMap["reset"]
	}
	return colorMap[color] + str + colorMap["reset"]
}

func StructToString(s interface{}) string {
	v, _ := json.Marshal(s)
	return string(v)
}
