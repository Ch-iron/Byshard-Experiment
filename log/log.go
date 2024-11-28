package log

import (
	"fmt"
	"io"
	stdlog "log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type severity int32

const (
	DEBUG severity = iota
	INFO
	WARNING
	ERROR
)

var names = []string{
	DEBUG:   "DEBUG",
	INFO:    "INFO",
	WARNING: "WARNING",
	ERROR:   "ERROR",
}

func (s *severity) Get() interface{} {
	return *s
}

func (s *severity) Set(value string) error {
	threshold := INFO
	value = strings.ToUpper(value)
	for i, name := range names {
		if name == value {
			threshold = severity(i)
		}
	}
	*s = threshold
	return nil
}

func (s *severity) String() string {
	return names[int(*s)]
}

type logger struct {
	sync.Mutex
	// buffer *buffer

	// *stdlog.Logger
	debug            *stdlog.Logger
	info             *stdlog.Logger
	votedebug        *stdlog.Logger
	decidedebug      *stdlog.Logger
	commitdebug      *stdlog.Logger
	lockdebug        *stdlog.Logger
	localdebug       *stdlog.Logger
	warning          *stdlog.Logger
	err              *stdlog.Logger
	performance_info *stdlog.Logger
	states_info      *stdlog.Logger

	shardStatistic   *stdlog.Logger
	experimentResult *stdlog.Logger

	severity severity
	dir      string
}

// the default logger
var log logger

// Setup setup log format and output file
func Setup() {
	format := stdlog.Ldate | stdlog.Ltime | stdlog.Lmicroseconds | stdlog.Lshortfile
	fname := fmt.Sprintf("%s.%d.log", filepath.Base(os.Args[0]), os.Getpid())
	f, err := os.Create(filepath.Join(log.dir, fname))
	if err != nil {
		stdlog.Fatal(err)
	}
	votename := fmt.Sprintf("%s.%d.vote.log", filepath.Base(os.Args[0]), os.Getpid())
	vote, err := os.Create(filepath.Join(log.dir, votename))
	if err != nil {
		stdlog.Fatal(err)
	}
	decidename := fmt.Sprintf("%s.%d.decide.log", filepath.Base(os.Args[0]), os.Getpid())
	decide, err := os.Create(filepath.Join(log.dir, decidename))
	if err != nil {
		stdlog.Fatal(err)
	}
	commitname := fmt.Sprintf("%s.%d.commit.log", filepath.Base(os.Args[0]), os.Getpid())
	commit, err := os.Create(filepath.Join(log.dir, commitname))
	if err != nil {
		stdlog.Fatal(err)
	}
	localname := fmt.Sprintf("%s.%d.local.log", filepath.Base(os.Args[0]), os.Getpid())
	local, err := os.Create(filepath.Join(log.dir, localname))
	if err != nil {
		stdlog.Fatal(err)
	}
	lockname := fmt.Sprintf("%s.%d.lock.log", filepath.Base(os.Args[0]), os.Getpid())
	lock, err := os.Create(filepath.Join(log.dir, lockname))
	if err != nil {
		stdlog.Fatal(err)
	}
	fname_performance := fmt.Sprintf("%s.%d.performance.log", filepath.Base(os.Args[0]), os.Getpid())
	f_performance, err := os.Create(filepath.Join(log.dir, fname_performance))
	if err != nil {
		stdlog.Fatal(err)
	}
	fname_state := fmt.Sprintf("%s.%d.state.log", filepath.Base(os.Args[0]), os.Getpid())
	f_state, err := os.Create(filepath.Join(log.dir, fname_state))
	if err != nil {
		stdlog.Fatal(err)
	}
	fnameShardStatistic := fmt.Sprintf("%s.%d.shardStatistic.log", filepath.Base(os.Args[0]), os.Getpid())
	fShardStatistic, err := os.Create(filepath.Join(log.dir, fnameShardStatistic))
	if err != nil {
		stdlog.Fatal(err)
	}
	fnameExperimentResult := fmt.Sprintf("%s.%d.experimentResult.csv", filepath.Base(os.Args[0]), os.Getpid())
	fExperimentResult, err := os.Create(filepath.Join(log.dir, fnameExperimentResult))
	if err != nil {
		stdlog.Fatal(err)
	}
	log.debug = stdlog.New(f, "[DEBUG] ", format)
	log.info = stdlog.New(f, "[INFO] ", format)
	log.votedebug = stdlog.New(vote, "[VOTEDEBUG] ", format)
	log.decidedebug = stdlog.New(decide, "[DECIDEDEBUG] ", format)
	log.commitdebug = stdlog.New(commit, "[COMMITDEBUG] ", format)
	log.localdebug = stdlog.New(local, "[LOCALDEBUG] ", format)
	log.lockdebug = stdlog.New(lock, "[LOCKDEBUG] ", format)
	multi := io.MultiWriter(f, os.Stderr)
	log.warning = stdlog.New(multi, "[WARNING] ", format)
	log.err = stdlog.New(multi, "[ERROR] ", format)
	log.performance_info = stdlog.New(f_performance, "[PERFORM-INFO]", format)
	log.states_info = stdlog.New(f_state, "[STATE-INFO]", format)
	log.shardStatistic = stdlog.New(fShardStatistic, "[SHARD1STATISTIC]", format)
	log.experimentResult = stdlog.New(fExperimentResult, "", 0)

}

func Debug(v ...interface{}) {
	if log.severity == DEBUG {
		_ = log.debug.Output(2, fmt.Sprint(v...))
	}
}

func Debugf(format string, v ...interface{}) {
	if log.severity == DEBUG {
		_ = log.debug.Output(2, fmt.Sprintf(format, v...))
	}
}

func Info(v ...interface{}) {
	if log.severity <= INFO {
		_ = log.info.Output(2, fmt.Sprint(v...))
	}
}

func Infof(format string, v ...interface{}) {
	if log.severity <= INFO {
		_ = log.info.Output(2, fmt.Sprintf(format, v...))
	}
}

func VoteDebugf(format string, v ...interface{}) {
	if log.severity == DEBUG {
		_ = log.votedebug.Output(2, fmt.Sprintf(format, v...))
	}
}

func DecideDebugf(format string, v ...interface{}) {
	if log.severity == DEBUG {
		_ = log.decidedebug.Output(2, fmt.Sprintf(format, v...))
	}
}

func CommitDebugf(format string, v ...interface{}) {
	if log.severity == DEBUG {
		_ = log.commitdebug.Output(2, fmt.Sprintf(format, v...))
	}
}

func LocalDebugf(format string, v ...interface{}) {
	if log.severity == DEBUG {
		_ = log.localdebug.Output(2, fmt.Sprintf(format, v...))
	}
}

func LockDebugf(format string, v ...interface{}) {
	if log.severity == DEBUG {
		_ = log.lockdebug.Output(2, fmt.Sprintf(format, v...))
	}
}

func ShardStatisticf(format string, v ...interface{}) {
	if log.severity <= INFO {
		_ = log.shardStatistic.Output(2, fmt.Sprintf(format, v...))
	}
}

func PerformanceInfo(v ...interface{}) {
	if log.severity <= INFO {
		_ = log.performance_info.Output(2, fmt.Sprint(v...))
	}
}

func PerformanceInfof(format string, v ...interface{}) {
	if log.severity <= INFO {
		_ = log.performance_info.Output(2, fmt.Sprintf(format, v...))
	}
}

func StateInfo(v ...interface{}) {
	if log.severity <= INFO {
		_ = log.states_info.Output(2, fmt.Sprint(v...))
	}
}

func StateInfof(format string, v ...interface{}) {
	if log.severity <= INFO {
		_ = log.states_info.Output(2, fmt.Sprintf(format, v...))
	}
}

func ExperimentResult(v ...interface{}) {
	_ = log.experimentResult.Output(2, fmt.Sprint(v...))
}

func ExperimentResultf(format string, v ...interface{}) {
	_ = log.experimentResult.Output(2, fmt.Sprintf(format, v...))
}

func Warning(v ...interface{}) {
	if log.severity <= WARNING {
		_ = log.warning.Output(2, fmt.Sprint(v...))
	}
}

func Warningf(format string, v ...interface{}) {
	if log.severity <= WARNING {
		_ = log.warning.Output(2, fmt.Sprintf(format, v...))
	}
}

func Error(v ...interface{}) {
	_ = log.err.Output(2, fmt.Sprint(v...))
}

func Errorf(format string, v ...interface{}) {
	_ = log.err.Output(2, fmt.Sprintf(format, v...))
}

func Fatal(v ...interface{}) {
	_ = log.err.Output(2, fmt.Sprint(v...))
	stdlog.Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	_ = log.err.Output(2, fmt.Sprintf(format, v...))
	stdlog.Fatalf(format, v...)
}

func Testf(format string, v ...interface{}) {
	if log.severity <= INFO {
		_ = log.info.Output(3, fmt.Sprintf(format, v...))
	}
}

type TestLog[T any] struct {
	Title       string
	Description string
	Expected    T
	Actual      T
	Error       error
}

// setting the case is normal or abnormal
func (tl *TestLog[T]) IsNormal(normal bool) *TestLog[T] {
	if normal {
		tl.Title = "Normal Case"
	} else {
		tl.Title = "Abnormal Case"
	}
	return tl
}

// set description
// then, return its instance to be used for print
func (tl *TestLog[T]) SetCaseDesc(description string) *TestLog[T] {
	tl.Description = description
	return tl
}

// print what this testcase is (title and description)
func (tl *TestLog[T]) PrintCase() {
	Testf("%-15v - %v", tl.Title, tl.Description)
}

// print what this testcase is (title and description)
func (tl *TestLog[T]) PrintRawResultFor(result string) {
	Testf("%-15v - %v", "CORRECT", result)
}

// set values for expected and actual
// then, return its instance to be used for print
func (tl *TestLog[T]) SetValues(expected T, actual T) *TestLog[T] {
	tl.Expected = expected
	tl.Actual = actual
	return tl
}

// print expected and actual values for some variable
func (tl *TestLog[T]) PrintResultFor(object string) {
	Testf("%-15v - expected: %-5v | actual: %-5v values are same for %v", "CORRECT", tl.Expected, tl.Actual, object)
}

// print expected and actual values are same or not
func (tl *TestLog[T]) PrintIsEqual(result string) {
	Testf("%-15v - values are same: %v", "CORRECT", result)
}

// // print expected and actual values are different or not
func (tl *TestLog[T]) PrintIsNotEqual(result string) {
	Testf("%-15v - values are different: %v", "CORRECT", result)
}

// set error
// then, return its instance to be used for print
func (tl *TestLog[T]) SetError(err error) *TestLog[T] {
	tl.Error = err
	return tl
}

// print error is existed
func (tl *TestLog[T]) PrintIsError() {
	Testf("%-15v - error is occured: %v", "CORRECT", tl.Error)
}

// print error is not existed
func (tl *TestLog[T]) PrintIsNoError() {
	Testf("%-15v - error is not occured", "CORRECT")
}
