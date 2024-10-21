package godb

// interface for an aggregation state
type AggState interface {
	// Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState struct {
	// TODO: some code goes here
	alias string
	expr  Expr
	sum   int64
}

func (a *SumAggState) Copy() AggState {
	// TODO: some code goes here
	return &SumAggState{a.alias, a.expr, a.sum}
}

func intAggGetter(v DBValue) int64 {
	// TODO: some code goes here
	return v.(IntField).Value
}

func stringAggGetter(v DBValue) string {
	// TODO: some code goes here
	return v.(StringField).Value
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.alias = alias
	a.expr = expr
	a.sum = 0
	return nil
}

func (a *SumAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	val, err := a.expr.EvalExpr(t)
	if err == nil {
		a.sum += intAggGetter(val)
	}
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *SumAggState) Finalize() *Tuple {
	// TODO: some code goes here
	td := a.GetTupleDesc()
	f := IntField{int64(a.sum)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	// TODO: some code goes here
	alias string
	expr  Expr
	sum   int64
	count int64
}

func (a *AvgAggState) Copy() AggState {
	// TODO: some code goes here
	return &AvgAggState{a.alias, a.expr, a.sum, a.count}
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.alias = alias
	a.expr = expr
	a.sum = 0
	a.count = 0
	return nil
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err == nil {
		a.sum += intAggGetter(val)
		a.count++
	}
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *AvgAggState) Finalize() *Tuple {
	// TODO: some code goes here
	td := a.GetTupleDesc()
	f := IntField{a.sum / a.count}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	// TODO: some code goes here
	alias string
	expr  Expr
	max any
}

func (a *MaxAggState) Copy() AggState {
	// TODO: some code goes here
	return &MaxAggState{a.alias, a.expr, a.max}
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.alias = alias
	a.expr = expr
	a.max = nil
	return nil
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	val, err := a.expr.EvalExpr(t)
	if err == nil {
		switch val.(type) {
		case IntField:
			if a.max == nil || intAggGetter(val) > a.max.(int64) {
				a.max = intAggGetter(val)
			}
		case StringField:
			if a.max == nil || stringAggGetter(val) > a.max.(string) {
				a.max = stringAggGetter(val)
			}
		}
	}
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	var ft FieldType
	switch a.max.(type) {
	case int64:
		ft = FieldType{a.alias, "", IntType}
	case string:
		ft = FieldType{a.alias, "", StringType}
	}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MaxAggState) Finalize() *Tuple {
	// TODO: some code goes here
	td := a.GetTupleDesc()
	var f DBValue
	switch a.max.(type) {
	case int64:
		f = IntField{a.max.(int64)}
	case string:
		f = StringField{a.max.(string)}
	}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	// TODO: some code goes here
	alias string
	expr  Expr
	min any
}

func (a *MinAggState) Copy() AggState {
	// TODO: some code goes here
	return &MinAggState{a.alias, a.expr, a.min}
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.alias = alias
	a.expr = expr
	a.min = nil
	return nil
}

func (a *MinAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err == nil {
		switch val.(type) {
		case IntField:
			if a.min == nil || intAggGetter(val) < a.min.(int64) {
				a.min = intAggGetter(val)
			}
		case StringField:
			if a.min == nil || stringAggGetter(val) < a.min.(string) {
				a.min = stringAggGetter(val)
			}
		}
	}
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	var ft FieldType
	switch a.min.(type) {
	case int64:
		ft = FieldType{a.alias, "", IntType}
	case string:
		ft = FieldType{a.alias, "", StringType}
	}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MinAggState) Finalize() *Tuple {
	// TODO: some code goes here
	td := a.GetTupleDesc()
	var f DBValue
	switch a.min.(type) {
	case int64:
		f = IntField{a.min.(int64)}
	case string:
		f = StringField{a.min.(string)}
	}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}
