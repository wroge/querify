package querify

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

const (
	null = "null"
)

type Value interface{}

func Copy(value Value) (Value, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	return gjson.ParseBytes(b).Value(), nil
}

type Record struct {
	Err     error
	Columns []string
	Values  []Value
}

func (r Record) Scan(dest interface{}) error {
	if r.Err != nil {
		return r.Err
	}

	b, err := r.MarshalJSON()
	if err != nil {
		return err
	}

	return json.Unmarshal(b, dest)
}

func (r Record) ScanColumn(column string, dest interface{}) error {
	if r.Err != nil {
		return r.Err
	}

	value, err := Ident(column).Variable(SelectedRecord{Source: r})
	if err != nil {
		return err
	}

	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, dest)
}

func (r Record) MarshalJSON() ([]byte, error) {
	if r.Err != nil {
		return nil, r.Err
	}

	m := map[string]interface{}{}

	for i, c := range r.Columns {
		if i < len(r.Values) {
			m[c] = r.Values[i]
		}
	}

	return json.Marshal(m)
}

func (r *Record) UnmarshalJSON(b []byte) error {
	m := map[string]interface{}{}

	err := json.Unmarshal(b, &m)
	if err != nil {
		return err
	}

	r.Columns = make([]string, len(m))
	r.Values = make([]Value, len(m))

	n := 0

	for c, v := range m {
		r.Columns[n] = c
		r.Values[n] = v
		n++
	}

	return nil
}

func (r Record) Set(column string, value Value) Record {
	if r.Err != nil {
		return Record{Err: r.Err}
	}

	found := false

	for i, c := range r.Columns {
		if c == column {
			found = true
			if len(r.Values) <= i {
				r.Values = append(r.Values, make([]Value, i-len(r.Values)+1)...)
			}
			r.Values[i] = value
		}
	}

	if !found {
		return Record{Err: fmt.Errorf("querify: column '%s' not found", column)}
	}

	return r
}

func From(table interface{}) Table {
	b, err := json.Marshal(table)
	if err != nil {
		return Table{Err: err}
	}

	var t Table

	err = json.Unmarshal(b, &t)
	if err != nil {
		return Table{Err: err}
	}

	return t
}

type Table struct {
	Err     error
	Columns []string
	Data    [][]Value
}

func (t Table) Copy() Table {
	if t.Err != nil {
		return Table{Err: t.Err}
	}
	var err error
	data := make([][]Value, len(t.Data))

	for i, d := range t.Data {
		data[i] = make([]Value, len(t.Columns))
		for j, v := range d {
			data[i][j], err = Copy(v)
			if err != nil {
				return Table{Err: err}
			}
		}
	}

	return Table{
		Columns: append([]string{}, t.Columns...),
		Data:    data,
	}
}

func (t Table) As(name string) Table {
	if t.Err != nil {
		return Table{Err: t.Err}
	}

	for i, c := range t.Columns {
		if name == "" {
			t.Columns[i] = c
			continue
		}
		t.Columns[i] = name + "." + c[strings.LastIndex(c, ".")+1:]
	}

	return t
}

func (t Table) Records() []Record {
	r := make([]Record, len(t.Data))

	for i, d := range t.Data {
		r[i] = Record{Columns: t.Columns, Values: d, Err: t.Err}
	}

	return r
}

func (t Table) Scan(dest interface{}) error {
	if t.Err != nil {
		return t.Err
	}

	b, err := t.MarshalJSON()
	if err != nil {
		return err
	}

	return json.Unmarshal(b, dest)
}

func (t Table) ScanColumn(column string, dest interface{}) error {
	if t.Err != nil {
		return t.Err
	}

	_, values, err := Ident(column).Select(SelectedTable{Source: t})
	if err != nil {
		return err
	}

	b, err := json.Marshal(values)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, dest)
}

func (t Table) MarshalJSON() ([]byte, error) {
	if t.Err != nil {
		return nil, t.Err
	}

	arr := []byte("[")

	for i, d := range t.Data {
		if i != 0 {
			arr = append(arr, ',')
		}
		b, err := json.Marshal(Record{Columns: t.Columns, Values: d})
		if err != nil {
			return nil, err
		}
		arr = append(arr, b...)
	}

	arr = append(arr, ']')

	return arr, nil
}

func (t *Table) UnmarshalJSON(b []byte) error {
	arr := []map[string]interface{}{}

	err := json.Unmarshal(b, &arr)
	if err != nil {
		return err
	}

	columnMap := map[string]int{}

	t.Err = nil
	t.Columns = nil
	t.Data = make([][]Value, len(arr))

	for i, each := range arr {
		l := len(each)
		if len(columnMap) > l {
			l = len(columnMap)
		}

		t.Data[i] = make([]Value, l)
		for c, v := range each {
			if _, ok := columnMap[c]; !ok {
				columnMap[c] = len(t.Columns)
				t.Columns = append(t.Columns, c)
			}

			t.Data[i][columnMap[c]] = v
		}
	}

	return nil
}

func (t Table) Query() Table {
	return t
}

func (t Table) Record(index int) Record {
	if t.Err != nil {
		return Record{Err: t.Err}
	}

	record := Record{
		Columns: append([]string{}, t.Columns...),
	}

	if index < len(t.Data) {
		record.Values = t.Data[index]
	}

	return record
}

func (t Table) UnionAll(table Table) Table {
	columnMap := map[string]int{}
	transform := map[int]int{}

	for i, c := range t.Columns {
		columnMap[c] = i
	}

	for i, c := range table.Columns {
		if _, ok := columnMap[c]; !ok {
			columnMap[c] = len(t.Columns)
			t.Columns = append(t.Columns, c)
		}

		transform[i] = columnMap[c]
	}

	for _, d := range table.Data {
		values := make([]Value, len(t.Columns))
		for j, v := range d {
			if _, ok := transform[j]; ok {
				values[transform[j]] = v
			}
		}
		t.Data = append(t.Data, values)
	}

	return t
}

func (t Table) Join(joins ...Join) Table {
	if t.Err != nil {
		return t
	}

	for _, j := range joins {
		t = j.Join(t)
		if t.Err != nil {
			return Table{Err: t.Err}
		}
	}

	return t
}

func (t Table) Where(condition Condition) Table {
	if t.Err != nil {
		return Table{Err: t.Err}
	}

	n := 0

	for _, d := range t.Data {
		keep, err := condition.Condition(GroupedRecord{Source: Record{Columns: t.Columns, Values: d}})
		if err != nil {
			return Table{Err: err}
		}

		if keep {
			t.Data[n] = d
			n++
		}
	}

	t.Data = t.Data[:n]

	return t
}

func (t Table) GroupBy(groups ...GroupBy) GroupedTable {
	if t.Err != nil {
		return GroupedTable{Err: t.Err}
	}

	var groupingSets GroupingSets

	for i, g := range groups {
		gs, err := g.GroupBy()
		if err != nil {
			return GroupedTable{Err: err}
		}
		if i == 0 {
			groupingSets = gs
			continue
		}

		groupingSets = groupingSets.Cart(gs)
	}

	var union GroupedTable

	for i, s := range groupingSets {
		gt := groupByColumns(t.Copy(), s)
		if gt.Err != nil {
			return GroupedTable{Err: gt.Err}
		}

		if i == 0 {
			union = gt
			continue
		}

		union = union.UnionAll(gt)
		if union.Err != nil {
			return GroupedTable{Err: union.Err}
		}
	}

	return GroupedTable{Source: union.Source, Grouped: union.Grouped}
}

func (t Table) Select(selects ...Select) SelectedTable {
	return GroupedTable{Err: t.Err, Source: t}.Select(selects...)
}

func (t Table) Insert(record Record) Table {
	if t.Err != nil {
		return Table{Err: t.Err}
	}

	n := Record{
		Columns: t.Columns,
		Values:  make([]Value, len(t.Columns)),
	}

	for i, c := range record.Columns {
		if i < len(record.Values) {
			n = n.Set(c, record.Values[i])
		}
	}

	if n.Err != nil {
		return Table{Err: n.Err}
	}

	t.Data = append(t.Data, n.Values)

	return t
}

func (t Table) Update(record Record, where Condition) Table {
	if t.Err != nil {
		return Table{Err: t.Err}
	}

	for i, d := range t.Data {
		upd, err := where.Condition(GroupedRecord{Source: Record{Columns: t.Columns, Values: d}})
		if err != nil {
			return Table{Err: err}
		}

		if upd {
			r := t.Record(i)

			for i, c := range record.Columns {
				if i < len(record.Values) {
					r = r.Set(c, record.Values[i])
				}
			}

			if r.Err != nil {
				return Table{Err: r.Err}
			}

			t.Data[i] = r.Values
		}
	}

	return t
}

func (t Table) Delete(where Condition) Table {
	if t.Err != nil {
		return Table{Err: t.Err}
	}

	for i, d := range t.Data {
		del, err := where.Condition(GroupedRecord{Source: Record{Columns: t.Columns, Values: d}})
		if err != nil {
			return Table{Err: err}
		}

		if del {
			copy(t.Data[i:], t.Data[i+1:])
			t.Data[len(t.Data)-1] = nil
			t.Data = t.Data[:len(t.Data)-1]
		}
	}

	return t
}

type GroupedRecord struct {
	Err     error
	Source  Record
	Grouped Table
}

type GroupedTable struct {
	Err     error
	Source  Table
	Grouped []Table
}

func (t GroupedTable) Copy() GroupedTable {
	if t.Err != nil {
		return GroupedTable{Err: t.Err}
	}

	grouped := make([]Table, len(t.Grouped))

	for i, g := range t.Grouped {
		grouped[i] = g.Copy()
	}

	return GroupedTable{
		Source:  t.Source.Copy(),
		Grouped: grouped,
	}
}

func (t GroupedTable) UnionAll(table GroupedTable) GroupedTable {
	if t.Err != nil {
		return GroupedTable{Err: t.Err}
	}

	columnMap := map[string]int{}
	transform := map[int]int{}

	for i, c := range t.Source.Columns {
		columnMap[c] = i
	}

	for i, c := range table.Source.Columns {
		if _, ok := columnMap[c]; !ok {
			columnMap[c] = len(t.Source.Columns)
			t.Source.Columns = append(t.Source.Columns, c)
		}

		transform[i] = columnMap[c]
	}

	if len(t.Grouped) != len(t.Source.Data) {
		t.Grouped = append(t.Grouped, make([]Table, len(t.Source.Data)-len(t.Grouped))...)
	}

	for _, d := range table.Source.Data {
		values := make([]Value, len(t.Source.Columns))
		for j, v := range d {
			if _, ok := transform[j]; ok {
				values[transform[j]] = v
			}
		}
		t.Source.Data = append(t.Source.Data, values)
	}

	t.Grouped = append(t.Grouped, table.Grouped...)

	return t
}

func (t GroupedTable) Having(condition Condition) GroupedTable {
	if t.Err != nil {
		return GroupedTable{Err: t.Err}
	}

	n := 0

	for i, d := range t.Source.Data {
		var grouped Table
		if i < len(t.Grouped) {
			grouped = t.Grouped[i]
		}

		keep, err := condition.Condition(
			GroupedRecord{
				Source:  Record{Columns: t.Source.Columns, Values: d},
				Grouped: grouped})
		if err != nil {
			return GroupedTable{Err: err}
		}

		if keep {
			t.Source.Data[n] = d

			if n < len(t.Grouped) {
				t.Grouped[n] = grouped
			}

			n++
		}
	}

	t.Source.Data = t.Source.Data[:n]

	if n < len(t.Grouped) {
		t.Grouped = t.Grouped[:n]
	}

	return t
}

func (t GroupedTable) Select(selects ...Select) SelectedTable {
	if t.Err != nil {
		return SelectedTable{Err: t.Err}
	}

	if len(selects) == 0 {
		return SelectedTable{
			Source:   t.Source,
			Grouped:  t.Grouped,
			Selected: t.Source,
		}
	}

	if len(selects) == 1 && len(t.Grouped) == 0 {
		t.Grouped = []Table{t.Source}
	}

	cols := make([]string, len(selects))
	data := make([][]Value, len(t.Source.Data))

	for i, s := range selects {
		col, values, err := s.Select(SelectedTable{Source: t.Source, Grouped: t.Grouped})
		if err != nil {
			return SelectedTable{Err: err}
		}

		cols[i] = col

		for j, v := range values {
			if len(data[j]) == 0 {
				data[j] = make([]Value, len(selects))
			}
			data[j][i] = v
		}
	}

	return SelectedTable{
		Source:  t.Source,
		Grouped: t.Grouped,
		Selected: Table{
			Columns: cols,
			Data:    data,
		},
	}
}

type SelectedRecord struct {
	Err      error
	Source   Record
	Grouped  Table
	Selected Record
}

type SelectedTable struct {
	Err      error
	Source   Table
	Grouped  []Table
	Selected Table
}

func (t SelectedTable) Record(index int) SelectedRecord {
	record := SelectedRecord{
		Source: Record{
			Columns: t.Source.Columns,
		},
		Selected: Record{
			Columns: t.Selected.Columns,
		},
		Err: t.Err,
	}

	if index < len(t.Source.Data) {
		record.Source.Values = t.Source.Data[index]
	}

	if index < len(t.Selected.Data) {
		record.Selected.Values = t.Selected.Data[index]
	}

	if index < len(t.Grouped) {
		record.Grouped = t.Grouped[index]
	}

	return record
}

func (t SelectedTable) Distinct() SelectedTable {
	if t.Err != nil {
		return SelectedTable{Err: t.Err}
	}

	distinct := map[string][]Value{}

	for _, d := range t.Selected.Data {
		b, err := json.Marshal(d)
		if err != nil {
			return SelectedTable{Err: err}
		}

		if _, ok := distinct[string(b)]; ok {
			continue
		}

		distinct[string(b)] = d
	}

	n := 0

	for _, r := range distinct {
		if n < len(t.Selected.Data) {
			t.Selected.Data[n] = r
		}

		n++
	}

	if n < len(t.Selected.Data) {
		t.Selected.Data = t.Selected.Data[:n]
	}

	return t
}

func (t SelectedTable) OrderBy(orders ...OrderBy) SelectedTable {
	if t.Err != nil {
		return SelectedTable{Err: t.Err}
	}

	qt := SelectedTable{Source: t.Source, Selected: t.Selected, Grouped: t.Grouped}

	arr := make([]SelectedRecord, len(t.Selected.Data))

	for i := range t.Selected.Data {
		arr[i] = qt.Record(i)
	}

	var err error
	var comp int

	sort.Slice(arr, func(i, j int) bool {
		if err != nil {
			return false
		}
		for _, o := range orders {
			comp, err = o.OrderBy(arr[i], arr[j])
			if err != nil {
				return false
			}

			if comp == 0 {
				continue
			}

			return comp < 0
		}

		return false
	})

	if err != nil {
		return SelectedTable{Err: err}
	}

	table := SelectedTable{
		Selected: Table{
			Columns: t.Selected.Columns,
			Data:    make([][]Value, len(arr)),
		},
	}

	for i, r := range arr {
		table.Selected.Data[i] = r.Selected.Values
	}

	table.Source = Table{}
	table.Grouped = nil

	return table
}

func (t SelectedTable) Limit(limit uint64) SelectedTable {
	if t.Err != nil {
		return SelectedTable{Err: t.Err}
	}

	t.Source = Table{}
	t.Grouped = nil

	if t.Err != nil {
		return t
	}

	if int(limit) > len(t.Selected.Data) {
		return t
	}

	t.Selected.Data = t.Selected.Data[:limit]

	return t
}

func (t SelectedTable) Offset(offset uint64) SelectedTable {
	if t.Err != nil {
		return SelectedTable{Err: t.Err}
	}

	t.Source = Table{}
	t.Grouped = nil

	if t.Err != nil {
		return t
	}

	if int(offset) > len(t.Selected.Data) {
		return SelectedTable{Selected: Table{Columns: append([]string{}, t.Selected.Columns...)}}
	}

	t.Selected.Data = t.Selected.Data[offset:]

	return t
}

func (t SelectedTable) Query() Table {
	if t.Err != nil {
		return Table{Err: t.Err}
	}

	return Table{
		Columns: t.Selected.Columns,
		Data:    t.Selected.Data,
	}
}

func (t SelectedTable) Scan(dest interface{}) error {
	if t.Err != nil {
		return t.Err
	}

	return t.Selected.Scan(dest)
}

func (t SelectedTable) ScanColumn(column string, dest interface{}) error {
	if t.Err != nil {
		return t.Err
	}

	return t.Selected.ScanColumn(column, dest)
}

func (t SelectedTable) First() Record {
	if t.Err != nil {
		return Record{Err: t.Err}
	}

	if len(t.Selected.Data) == 0 {
		return Record{Err: fmt.Errorf("querify: no rows")}
	}

	return Record{Columns: t.Selected.Columns, Values: t.Selected.Data[0]}
}

func groupByColumns(table Table, columns []string) GroupedTable {
	if len(columns) == 0 {
		grouped := make([]Table, len(table.Data))

		for i, d := range table.Data {
			grouped[i] = Table{
				Columns: table.Columns,
				Data:    [][]Value{d},
			}
		}

		return GroupedTable{
			Source: Table{
				Data: [][]Value{nil},
			},
			Grouped: []Table{
				{
					Columns: table.Columns,
					Data:    table.Data,
				},
			},
		}
	}

	out := GroupedTable{
		Source: Table{
			Columns: append([]string{}, columns...),
			Data:    [][]Value{},
		},
		Grouped: []Table{},
	}

	m := map[string]int{}

	for _, d := range table.Data {
		unique := make([]Value, len(columns))
		for j, c := range columns {
			v, err := Ident(c).Variable(SelectedRecord{Source: Record{Columns: table.Columns, Values: d}})
			if err != nil {
				return GroupedTable{Err: err}
			}
			unique[j] = v
		}

		b, err := json.Marshal(unique)
		if err != nil {
			return GroupedTable{Err: err}
		}

		position, ok := m[string(b)]
		if !ok {
			m[string(b)] = len(out.Source.Data)
			out.Source.Data = append(out.Source.Data, unique)
			out.Grouped = append(out.Grouped, Table{Columns: table.Columns, Data: [][]Value{d}})
		} else {
			out.Grouped[position].Data = append(out.Grouped[position].Data, d)
		}
	}

	return out
}

type Literal struct {
	Value Value
}

func (l Literal) Variable(record SelectedRecord) (Value, error) {
	return l.Value, nil
}

func (l Literal) Select(table SelectedTable) (string, []Value, error) {
	out := make([]Value, len(table.Source.Data))

	for i := range out {
		out[i] = l
	}

	return "literal", out, nil
}

type Ident string

func (i Ident) Variable(record SelectedRecord) (Value, error) {
	source := record.Source

	index := -1

	for j, c := range record.Source.Columns {
		if c == string(i) || c[:strings.LastIndex(c, ".")+1] == string(i) {
			if index != -1 {
				return nil, fmt.Errorf("querify: ident '%s' is ambiguous", i)
			}

			index = j
		}
	}

	if index < 0 {
		source = record.Selected
		for j, c := range record.Selected.Columns {
			if c == string(i) || c[:strings.LastIndex(c, ".")+1] == string(i) {
				if index != -1 {
					return nil, fmt.Errorf("querify: ident '%s' is ambiguous", i)
				}

				index = j
			}
		}
	}

	if index < 0 {
		return nil, fmt.Errorf("querify: ident '%s' not found", i)
	}

	if index < len(source.Values) {
		return source.Values[index], nil
	}

	return nil, nil
}

func (i Ident) Select(table SelectedTable) (string, []Value, error) {
	source := table.Source

	index := -1

	for j, c := range table.Source.Columns {
		if c == string(i) || c[:strings.LastIndex(c, ".")+1] == string(i) {
			if index != -1 {
				return "", nil, fmt.Errorf("querify: ident '%s' is ambiguous", i)
			}

			index = j
		}
	}

	if index < 0 {
		source = table.Selected
		for j, c := range table.Selected.Columns {
			if c == string(i) || c[:strings.LastIndex(c, ".")+1] == string(i) {
				if index != -1 {
					return "", nil, fmt.Errorf("querify: ident '%s' is ambiguous", i)
				}

				index = j
			}
		}
	}

	if index < 0 {
		return "", nil, fmt.Errorf("querify: ident '%s' not found", i)
	}

	values := make([]Value, len(source.Data))

	for j, d := range source.Data {
		if index < len(d) {
			values[j] = d[index]
		}
	}

	return source.Columns[index], values, nil
}

func (i Ident) GroupBy() (GroupingSets, error) {
	return [][]string{{string(i)}}, nil
}

type Concat []Variable

func (c Concat) Variable(record SelectedRecord) (Value, error) {
	b := strings.Builder{}

	for _, v := range c {
		value, err := v.Variable(record)
		if err != nil {
			return nil, err
		}

		js, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}

		s, err := strconv.Unquote(string(js))
		if err != nil {
			return nil, err
		}

		b.WriteString(s)
	}

	return b.String(), nil
}

func (c Concat) Select(table SelectedTable) (string, []Value, error) {
	out := make([]Value, len(table.Source.Data))

	for i := range table.Source.Data {
		value, err := c.Variable(table.Record(i))
		if err != nil {
			return "", nil, err
		}

		out[i] = value
	}

	return "concat", out, nil
}

type CountAll struct{}

func (CountAll) Variable(record SelectedRecord) (Value, error) {
	return len(record.Grouped.Data), nil
}

func (CountAll) Select(table SelectedTable) (string, []Value, error) {
	out := make([]Value, len(table.Grouped))

	for i, g := range table.Grouped {
		out[i] = len(g.Data)
	}

	return "count", out, nil
}

type Count string

func (c Count) Variable(record SelectedRecord) (Value, error) {
	_, values, err := Ident(c).Select(SelectedTable{Source: record.Grouped})
	if err != nil {
		return nil, err
	}

	return len(values), nil
}

func (c Count) Select(table SelectedTable) (string, []Value, error) {
	out := make([]Value, len(table.Grouped))

	for i, g := range table.Grouped {
		_, values, err := Ident(c).Select(SelectedTable{Source: g})
		if err != nil {
			return "", nil, err
		}
		out[i] = len(values)
	}

	return "count", out, nil
}

type ArrayAgg struct {
	Distinct   bool
	Expression Select
}

func (a ArrayAgg) Variable(record SelectedRecord) (Value, error) {
	_, values, err := a.Expression.Select(SelectedTable{Source: record.Grouped})
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	if a.Distinct {
		unique := map[string]Value{}

		for _, v := range values {
			b, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}

			if _, ok := unique[string(b)]; !ok {
				unique[string(b)] = v
			}
		}

		out := make([]Value, len(unique))

		n := 0

		for _, v := range unique {
			out[n] = v
			n++
		}

		return out, nil
	}

	return values, nil
}

func (a ArrayAgg) Select(table SelectedTable) (string, []Value, error) {
	values := make([]Value, len(table.Grouped))

	for i, g := range table.Grouped {
		value, err := a.Variable(SelectedRecord{Grouped: g})
		if err != nil {
			return "", nil, err
		}

		values[i] = value
	}

	return "array_agg", values, nil
}

type As struct {
	Name       string
	Expression Select
}

func (a As) Select(table SelectedTable) (string, []Value, error) {
	_, values, err := a.Expression.Select(table)
	return a.Name, values, err
}

type GroupingSets [][]string

func (gs GroupingSets) GroupBy() (GroupingSets, error) {
	return gs, nil
}

func (gs GroupingSets) Cart(bb GroupingSets) GroupingSets {
	p := make([][]string, len(gs)*len(bb))
	i := 0
	for _, a := range gs {
		for _, b := range bb {
			p[i] = append(append([]string{}, a...), b...)
			i++
		}
	}

	return p
}

type Cube []string

func (c Cube) GroupBy() (GroupingSets, error) {
	subsets := GroupingSets{}

	length := uint(len(c))

	for subsetBits := 1; subsetBits < (1 << length); subsetBits++ {
		var subset []string

		for object := uint(0); object < length; object++ {
			if (subsetBits>>object)&1 == 1 {
				subset = append(subset, c[object])
			}
		}
		subsets = append(subsets, subset)
	}

	subsets = append(subsets, []string{})

	return subsets, nil
}

type And []Condition

func (a And) Condition(record GroupedRecord) (bool, error) {
	for _, c := range a {
		keep, err := c.Condition(record)
		if err != nil {
			return false, err
		}

		if !keep {
			return false, nil
		}
	}

	return true, nil
}

type Or [2]Condition

func (o Or) Condition(record GroupedRecord) (bool, error) {
	for _, c := range o {
		keep, err := c.Condition(record)
		if err != nil {
			return false, err
		}

		if keep {
			return true, nil
		}
	}

	return false, nil
}

type Equals [2]Variable

func (e Equals) Condition(record GroupedRecord) (bool, error) {
	v0, err := e[0].Variable(SelectedRecord{Source: record.Source, Grouped: record.Grouped})
	if err != nil {
		return false, err
	}

	b0, err := json.Marshal(v0)
	if err != nil {
		return false, err
	}

	v1, err := e[1].Variable(SelectedRecord{Source: record.Source, Grouped: record.Grouped})
	if err != nil {
		return false, err
	}

	b1, err := json.Marshal(v1)
	if err != nil {
		return false, err
	}

	return string(b0) == string(b1), nil
}

type Greater [2]Variable

func (g Greater) Condition(record GroupedRecord) (bool, error) {
	v0, err := g[0].Variable(SelectedRecord{Source: record.Source, Grouped: record.Grouped})
	if err != nil {
		return false, err
	}

	b0, err := json.Marshal(v0)
	if err != nil {
		return false, err
	}

	v1, err := g[1].Variable(SelectedRecord{Source: record.Source, Grouped: record.Grouped})
	if err != nil {
		return false, err
	}

	b1, err := json.Marshal(v1)
	if err != nil {
		return false, err
	}

	if string(b0) == null || string(b1) == null {
		return false, nil
	}

	r0 := gjson.ParseBytes(b0)
	r1 := gjson.ParseBytes(b1)

	if r0.Type != r1.Type {
		return false, nil
	}

	return r1.Less(r0, true), nil
}

type Less [2]Variable

func (l Less) Condition(record GroupedRecord) (bool, error) {
	v0, err := l[0].Variable(SelectedRecord{Source: record.Source, Grouped: record.Grouped})
	if err != nil {
		return false, err
	}

	b0, err := json.Marshal(v0)
	if err != nil {
		return false, err
	}

	v1, err := l[1].Variable(SelectedRecord{Source: record.Source, Grouped: record.Grouped})
	if err != nil {
		return false, err
	}

	b1, err := json.Marshal(v1)
	if err != nil {
		return false, err
	}

	if string(b0) == null || string(b1) == null {
		return false, nil
	}

	r0 := gjson.ParseBytes(b0)
	r1 := gjson.ParseBytes(b1)

	if r0.Type != r1.Type {
		return false, fmt.Errorf("querify: cannot compare types '%s' and '%s'", r0.Type, r1.Type)
	}

	return r0.Less(r1, true), nil
}

type Asc struct {
	Expression Variable
	NullsLast  bool
}

func (a Asc) OrderBy(i, j SelectedRecord) (int, error) {
	vi, err := a.Expression.Variable(i)
	if err != nil {
		return 0, err
	}

	bi, err := json.Marshal(vi)
	if err != nil {
		return 0, err
	}

	vj, err := a.Expression.Variable(j)
	if err != nil {
		return 0, err
	}

	bj, err := json.Marshal(vj)
	if err != nil {
		return 0, err
	}

	if string(bi) == null {
		if string(bj) == null {
			return 0, nil
		}
		if a.NullsLast {
			return 1, nil
		}

		return -1, nil
	}

	if string(bj) == null {
		if a.NullsLast {
			return -1, nil
		}

		return 1, nil
	}

	ri := gjson.ParseBytes(bi)
	rj := gjson.ParseBytes(bj)

	if ri.Type != rj.Type {
		return 0, fmt.Errorf("querify: cannot compare types '%s' and '%s'", ri.Type, rj.Type)
	}

	if ri.Less(rj, true) {
		return -1, nil
	}

	return 1, nil
}

type Desc struct {
	Expression Variable
	NullsLast  bool
}

func (d Desc) OrderBy(i, j SelectedRecord) (int, error) {
	vi, err := d.Expression.Variable(i)
	if err != nil {
		return 0, err
	}

	bi, err := json.Marshal(vi)
	if err != nil {
		return 0, err
	}

	vj, err := d.Expression.Variable(j)
	if err != nil {
		return 0, err
	}

	bj, err := json.Marshal(vj)
	if err != nil {
		return 0, err
	}

	if string(bi) == null {
		if string(bj) == null {
			return 0, nil
		}
		if d.NullsLast {
			return 1, nil
		}

		return -1, nil
	}

	if string(bj) == null {
		if d.NullsLast {
			return -1, nil
		}

		return 1, nil
	}

	ri := gjson.ParseBytes(bi)
	rj := gjson.ParseBytes(bj)

	if ri.Type != rj.Type {
		return 0, fmt.Errorf("querify: cannot compare types '%s' and '%s'", ri.Type, rj.Type)
	}

	if !ri.Less(rj, true) {
		return -1, nil
	}

	return 1, nil
}

type LeftJoin struct {
	Right Query
	On    Condition
}

func (lj LeftJoin) Join(left Query) Table {
	l := left.Query()
	if l.Err != nil {
		return Table{Err: l.Err}
	}

	r := lj.Right.Query()
	if r.Err != nil {
		return Table{Err: r.Err}
	}

	if len(r.Columns) == 0 {
		return l
	}

	columns := append([]string{}, l.Columns...)
	columns = append(columns, r.Columns...)
	data := make([][]Value, 0, len(l.Data))

	var err error
	var ok bool

	for _, dl := range l.Data {
		found := false
		for _, dr := range r.Data {
			ok, err = lj.On.Condition(GroupedRecord{
				Source: Record{
					Columns: columns,
					Values:  append(dl, dr...),
				},
			})
			if err != nil {
				return Table{Err: err}
			}

			if ok {
				found = true
				data = append(data, append(dl, dr...))
			}
		}

		if !found {
			data = append(data, append(dl, make([]Value, len(r.Columns))...))
		}
	}

	return Table{
		Columns: columns,
		Data:    data,
	}
}
