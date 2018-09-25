package radix

// Logger is a generic logging interface
type Logger interface {
	Logf(level int, format string, v ...interface{})
	Warnf(format string, v ...interface{})
}

var (
	// default logger
	DefaultLogger Logger = &noOpLogger{}
)

// noOpLogger is used as a placeholder for the default logger
type noOpLogger struct{}

func (n *noOpLogger) Logf(level int, format string, v ...interface{}) {}
func (n *noOpLogger) Warnf(format string, v ...interface{})           {}
