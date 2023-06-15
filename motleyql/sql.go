package motleyql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Parse takes a string representing a SQL query and parses it into a Query struct. It may fail.
func Parse(sqls string) (Plan, error) {
	qs, err := ParseMany([]string{sqls})
	if len(qs) == 0 {
		return Plan{}, err
	}
	return qs[0], err
}

// ParseMany takes a string slice representing many SQL queries and parses them into a Query struct slice.
// It may fail. If it fails, it will stop at the first failure.
func ParseMany(sqls []string) ([]Plan, error) {
	var qs []Plan
	for _, sql := range sqls {
		q, err := parse(sql)
		if err != nil {
			return qs, err
		}
		qs = append(qs, q)
	}
	return qs, nil
}

func parse(sql string) (Plan, error) {
	return (&parser{0, strings.TrimSpace(sql), stepType, Plan{}, nil, ""}).parse()
}

type step int

const (
	stepType step = iota
	stepSelectField
	stepSelectComma
	stepSelectFrom
	stepSelectFromModelZoo
	stepWhere
	stepWhereField
	stepWhereOperator
	stepWhereValue
	stepWhereAnd
	stepOrder
	stepBy
	stepOrderByField
	stepLimit
	stepCount
	stepUsing
	stepData
	stepRespond
	stepIn
	stepMode
)

type parser struct {
	i               int
	sql             string
	step            step
	query           Plan
	err             error
	nextUpdateField string
}

func (p *parser) parse() (Plan, error) {
	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

func (p *parser) doParse() (Plan, error) {
	for {
		if p.i >= len(p.sql) {
			return p.query, p.err
		}
		switch p.step {
		case stepType:
			switch strings.ToUpper(p.peek()) {
			case "SELECT":
				p.query.Type = Select
				p.pop()
				p.step = stepSelectField
			default:
				return p.query, fmt.Errorf("invalid query type")
			}
		case stepSelectField:
			identifier := p.peek()
			if !isIdentifierOrAsterisk(identifier) {
				return p.query, fmt.Errorf("at SELECT: expected field to SELECT")
			}
			// TODO: handle '*'
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop()
			maybeFrom := p.peek()
			// TODO: implement 'AS'
			if strings.ToUpper(maybeFrom) == "AS" {
				p.pop()
				alias := p.peek()
				if !isIdentifier(alias) {
					return p.query, fmt.Errorf("at SELECT: expected field alias for \"" + identifier + " as\" to SELECT")
				}
				p.pop()
				maybeFrom = p.peek()
			}
			if strings.ToUpper(maybeFrom) == "FROM" {
				p.step = stepSelectFrom
				continue
			}
			p.step = stepSelectComma
		case stepSelectComma:
			commaRWord := p.peek()
			if commaRWord != "," {
				return p.query, fmt.Errorf("at SELECT: expected comma or FROM")
			}
			p.pop()
			p.step = stepSelectField
		case stepSelectFrom:
			fromRWord := p.peek()
			if strings.ToUpper(fromRWord) != "FROM" {
				return p.query, fmt.Errorf("at SELECT: expected FROM")
			}
			p.pop()
			p.step = stepSelectFromModelZoo
		case stepSelectFromModelZoo:
			zooName := p.peek()
			if len(zooName) == 0 {
				return p.query, fmt.Errorf("at SELECT: expected quoted model zoo name")
			}
			p.query.ModelZoo = zooName
			p.pop()
			next := p.peek()
			if strings.ToUpper(next) == "ORDER" {
				p.step = stepOrder
				continue
			}
			p.step = stepWhere
		case stepWhere:
			whereRWord := p.peek()
			if strings.ToUpper(whereRWord) != "WHERE" {
				return p.query, fmt.Errorf("expected WHERE")
			}
			p.pop()
			p.step = stepWhereField
		case stepWhereField:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at WHERE: expected field")
			}
			p.query.Conditions = append(p.query.Conditions, Condition{Operand1: identifier, Operand1IsField: true})
			p.pop()
			p.step = stepWhereOperator
		case stepWhereOperator:
			operator := p.peek()
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			switch operator {
			case "=":
				currentCondition.Operator = Eq
			case ">":
				currentCondition.Operator = Gt
			case ">=":
				currentCondition.Operator = Gte
			case "<":
				currentCondition.Operator = Lt
			case "<=":
				currentCondition.Operator = Lte
			case "!=":
				currentCondition.Operator = Ne
			default:
				return p.query, fmt.Errorf("at WHERE: unknown operator")
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()
			p.step = stepWhereValue
		case stepWhereValue:
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			identifier := p.peek()
			if isIdentifier(identifier) {
				currentCondition.Operand2 = identifier
				currentCondition.Operand2IsField = true
			} else {
				quotedValue, ln := p.peekQuotedStringWithLength()
				if ln == 0 {
					return p.query, fmt.Errorf("at WHERE: expected quoted value")
				}
				currentCondition.Operand2 = quotedValue
				currentCondition.Operand2IsField = false
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()
			next := p.peek()
			if strings.ToUpper(next) == "ORDER" {
				p.step = stepOrder
				continue
			}
			if strings.ToUpper(next) == "USING" {
				p.step = stepUsing
				continue
			}
			p.step = stepWhereAnd
		case stepWhereAnd:
			andRWord := p.peek()
			if strings.ToUpper(andRWord) != "AND" {
				return p.query, fmt.Errorf("expected AND")
			}
			p.pop()
			p.step = stepWhereField
		case stepOrder:
			orderRWord := p.peek()
			if strings.ToUpper(orderRWord) != "ORDER" {
				return p.query, fmt.Errorf("expected ORDER")
			}
			p.pop()
			p.step = stepBy
		case stepBy:
			byRWord := p.peek()
			if strings.ToUpper(byRWord) != "BY" {
				return p.query, fmt.Errorf("expected BY")
			}
			p.pop()
			p.step = stepOrderByField
		case stepOrderByField:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at ORDER BY: expected expression")
			}
			p.query.OrderByItems = append(p.query.OrderByItems, identifier)
			p.pop()
			next := p.peek()
			// Ascending or descending, default is ascending
			if strings.ToUpper(next) == "ASC" {
				p.pop()
			} else if strings.ToUpper(next) == "DESC" {
				p.query.Desc = true
				p.pop()
			}
			next = p.peek()
			if strings.ToUpper(next) == "LIMIT" {
				p.step = stepLimit
				continue
			}
			p.step = stepUsing
		case stepLimit:
			limitRWord := p.peek()
			if strings.ToUpper(limitRWord) != "LIMIT" {
				return p.query, fmt.Errorf("expected LIMIT")
			}
			p.pop()
			p.step = stepCount
		case stepCount:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at LIMIT: expected count")
			}
			count, err := strconv.Atoi(identifier)
			if err != nil || count <= 0 {
				return p.query, fmt.Errorf("at LIMIT: expected count")
			}
			p.query.Count = count
			p.pop()
			p.step = stepUsing
		case stepUsing:
			usingRWord := p.peek()
			if strings.ToUpper(usingRWord) != "USING" {
				return p.query, fmt.Errorf("expected USING")
			}
			p.pop()
			p.step = stepData
		case stepData:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at USING: expected data")
			}
			p.query.Data = identifier
			p.pop()
			p.step = stepRespond
		case stepRespond:
			respondRWord := p.peek()
			if strings.ToUpper(respondRWord) != "RESPOND" {
				return p.query, fmt.Errorf("expected RESPOND")
			}
			p.pop()
			p.step = stepIn
		case stepIn:
			inRWord := p.peek()
			if strings.ToUpper(inRWord) != "IN" {
				return p.query, fmt.Errorf("expected IN")
			}
			p.pop()
			p.step = stepMode
		case stepMode:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at RESPOND IN: expected mode")
			}
			p.query.Mode = identifier
			p.pop()
		}
	}
}

func (p *parser) peek() string {
	peeked, _ := p.peekWithLength()
	return peeked
}

func (p *parser) pop() string {
	peeked, len := p.peekWithLength()
	p.i += len
	p.popWhitespace()
	return peeked
}

func (p *parser) popWhitespace() {
	for ; p.i < len(p.sql) && p.sql[p.i] == ' '; p.i++ {
	}
}

var reservedWords = []string{
	"(", ")", ">=", "<=", "!=", ",", "=", ">", "<", "SELECT", "FROM", "WHERE",
	"ORDER", "BY", "ASC", "DESC", "LIMIT", "USING", "RESPOND", "IN",
}

func (p *parser) peekWithLength() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}
	for _, rWord := range reservedWords {
		token := strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+len(rWord))])
		if token == rWord {
			return token, len(token)
		}
	}
	if p.sql[p.i] == '\'' { // Quoted string
		return p.peekQuotedStringWithLength()
	}
	return p.peekIdentifierWithLength()
}

func (p *parser) peekQuotedStringWithLength() (string, int) {
	if len(p.sql) < p.i || p.sql[p.i] != '\'' {
		return "", 0
	}
	for i := p.i + 1; i < len(p.sql); i++ {
		if p.sql[i] == '\'' && p.sql[i-1] != '\\' {
			return p.sql[p.i+1 : i], len(p.sql[p.i+1:i]) + 2 // +2 for the two quotes
		}
	}
	return "", 0
}

func (p *parser) peekIdentifierWithLength() (string, int) {
	for i := p.i; i < len(p.sql); i++ {
		if matched, _ := regexp.MatchString(`[a-zA-Z0-9_*]`, string(p.sql[i])); !matched {
			return p.sql[p.i:i], len(p.sql[p.i:i])
		}
	}
	return p.sql[p.i:], len(p.sql[p.i:])
}

func (p *parser) validate() error {
	if len(p.query.Conditions) == 0 && p.step == stepWhereField {
		return fmt.Errorf("at WHERE: empty WHERE clause")
	}
	if p.query.Type == UnknownType {
		return fmt.Errorf("query type cannot be empty")
	}
	if p.query.ModelZoo == "" {
		return fmt.Errorf("model zoo name cannot be empty")
	}
	for _, c := range p.query.Conditions {
		if c.Operator == UnknownOperator {
			return fmt.Errorf("at WHERE: condition without operator")
		}
		if c.Operand1 == "" && c.Operand1IsField {
			return fmt.Errorf("at WHERE: condition with empty left side operand")
		}
		if c.Operand2 == "" && c.Operand2IsField {
			return fmt.Errorf("at WHERE: condition with empty right side operand")
		}
	}
	if p.query.Count < 0 {
		return fmt.Errorf("count cannot be negative")
	}
	return nil
}

func (p *parser) logError() {
	if p.err == nil {
		return
	}
	fmt.Println(p.sql)
	fmt.Println(strings.Repeat(" ", p.i) + "^")
	fmt.Println(p.err)
}

func isIdentifier(s string) bool {
	for _, rw := range reservedWords {
		if strings.ToUpper(s) == rw {
			return false
		}
	}
	matched, _ := regexp.MatchString("[a-zA-Z_0-9][a-zA-Z_0-9]*", s)
	return matched
}

func isIdentifierOrAsterisk(s string) bool {
	return isIdentifier(s) || s == "*"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
