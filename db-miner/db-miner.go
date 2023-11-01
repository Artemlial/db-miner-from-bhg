package dbMiner

import (
	"fmt"
	"regexp"
)

type DatabaseMiner interface {
	GetScema() (*Scema, error)
}

type Scema struct {
	Databases []Database
}

type Database struct {
	Name   string
	Tables []Table
}

type Table struct {
	Name     string
	Columnns []string
}

func Search(m DatabaseMiner) error{
	s,err:= m.GetScema()
	if err!=nil{
		return err
	}

	re:=getRegex()
	for _,database:=range s.Databases{
		for _,table := range database.Tables{
			for _,field:=range table.Columnns{
				for _,r:=range re{
					if r.MatchString(field){
						fmt.Println(database)
						fmt.Printf("[+] HIT: %s\n",field)
					}
				}
			}
		}
	}
	return nil
}

func getRegex() []*regexp.Regexp{
	return []*regexp.Regexp{
		regexp.MustCompile(`(?i)social`),
		regexp.MustCompile(`(?i)ssn`),
		regexp.MustCompile(`(?i)pass(word)?`),
		regexp.MustCompile(`(?i)hash`),
		regexp.MustCompile(`(?i)ccnum`),
		regexp.MustCompile(`(?i)card`),
		regexp.MustCompile(`(?i)security`),
		regexp.MustCompile(`(?i)key`),
	}
}
