package querify

type Query interface {
	Query() Table
}

type Join interface {
	Join(left Query) Table
}

type Condition interface {
	Condition(record GroupedRecord) (bool, error)
}

type GroupBy interface {
	GroupBy() (GroupingSets, error)
}

type Select interface {
	Select(table SelectedTable) (string, []Value, error)
}

type Variable interface {
	Variable(record SelectedRecord) (Value, error)
}

type OrderBy interface {
	OrderBy(i, j SelectedRecord) (int, error)
}
