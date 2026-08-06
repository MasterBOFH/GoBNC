package irc

import "testing"

func TestCaseMappingASCII(t *testing.T) {
	c := CaseASCII
	if !c.Equal("Nick", "nick") {
		t.Fatal("ascii fold")
	}
	if c.Equal("[A]", "{a}") {
		t.Fatal("ascii should not fold brackets")
	}
}

func TestCaseMappingRFC1459(t *testing.T) {
	c := CaseRFC1459
	if !c.Equal("[\\]~", "{|}^") {
		t.Fatalf("got %q %q", c.Fold("[\\]~"), c.Fold("{|}^"))
	}
	if !c.Equal("Nick", "nick") {
		t.Fatal()
	}
}

func TestCaseMappingStrict(t *testing.T) {
	c := CaseStrictRFC1459
	if !c.Equal("[\\]", "{|}") {
		t.Fatal()
	}
	if c.Equal("~", "^") {
		t.Fatal("strict should not fold ~")
	}
}

func TestParseCaseMapping(t *testing.T) {
	if ParseCaseMapping("ascii") != CaseASCII {
		t.Fatal()
	}
	if ParseCaseMapping("strict-rfc1459") != CaseStrictRFC1459 {
		t.Fatal()
	}
	if ParseCaseMapping("") != CaseRFC1459 {
		t.Fatal()
	}
}
