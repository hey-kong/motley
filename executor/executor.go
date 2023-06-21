package executor

import "github.com/hey-kong/motley/motleyql"

func processType(t motleyql.Type) error {
	return nil
}

func processFields(fields []string) error {
	return nil
}

func processModelZoo(zoo string) error {
	return nil
}

func processConditions(conditions []motleyql.Condition) error {
	return nil
}

func processOrderByItems(items []string) error {
	return nil
}

func processDesc(desc bool) error {
	return nil
}

func processCount(count int) error {
	return nil
}

func processData(data string) error {
	return nil
}

func processMode(mode string) error {
	return nil
}

func execute(p *motleyql.Plan) error {
	if err := processType(p.Type); err != nil {
		return err
	}

	if err := processFields(p.Fields); err != nil {
		return err
	}

	if err := processModelZoo(p.ModelZoo); err != nil {
		return err
	}

	if err := processConditions(p.Conditions); err != nil {
		return err
	}

	if err := processOrderByItems(p.OrderByItems); err != nil {
		return err
	}

	if err := processDesc(p.Desc); err != nil {
		return err
	}

	if err := processCount(p.Count); err != nil {
		return err
	}

	if err := processData(p.Data); err != nil {
		return err
	}

	if err := processMode(p.Mode); err != nil {
		return err
	}

	return nil
}
