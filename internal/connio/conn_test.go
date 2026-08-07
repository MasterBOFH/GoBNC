package connio

import (
	"bufio"
	"errors"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

func TestReadLimitedLineOK(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("PRIVMSG #c :hi\r\nNEXT\n"))
	line, err := ReadLimitedLine(r, 64)
	if err != nil || line != "PRIVMSG #c :hi" {
		t.Fatalf("got %q %v", line, err)
	}
	line, err = ReadLimitedLine(r, 64)
	if err != nil || line != "NEXT" {
		t.Fatalf("got %q %v", line, err)
	}
}

func TestReadLimitedLineTooLong(t *testing.T) {
	// max=8 including \n; payload without newline exceeds max then continues to \n
	payload := strings.Repeat("A", 10) + "\nOK\n"
	r := bufio.NewReader(strings.NewReader(payload))
	_, err := ReadLimitedLine(r, 8)
	if !errors.Is(err, irc.ErrLineTooLong) {
		t.Fatalf("want ErrLineTooLong, got %v", err)
	}
	line, err := ReadLimitedLine(r, 8)
	if err != nil || line != "OK" {
		t.Fatalf("after drain got %q %v", line, err)
	}
}

func TestReadLimitedLineExactMax(t *testing.T) {
	// 7 bytes + \n = 8
	body := strings.Repeat("B", 7)
	r := bufio.NewReader(strings.NewReader(body + "\n"))
	line, err := ReadLimitedLine(r, 8)
	if err != nil || line != body {
		t.Fatalf("got %q %v", line, err)
	}
}
