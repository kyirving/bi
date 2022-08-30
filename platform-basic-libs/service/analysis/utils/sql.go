package utils

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/1340691923/xwl_bi/engine/db"
	"github.com/1340691923/xwl_bi/platform-basic-libs/request"
	"github.com/1340691923/xwl_bi/platform-basic-libs/util"
	"github.com/Masterminds/squirrel"
)

type sqlI interface {
	ToSql() (string, []interface{}, error)
	Append(sqlizer squirrel.Sqlizer)
}

type Or struct {
	or db.Or
}

func (this *Or) Append(sqlizer squirrel.Sqlizer) {
	this.or = append(this.or, sqlizer)
}

func (this *Or) ToSql() (string, []interface{}, error) {
	return this.or.ToSql()
}

type And struct {
	and db.And
}

func (this *And) Append(sqlizer squirrel.Sqlizer) {
	this.and = append(this.and, sqlizer)
}

func (this *And) ToSql() (string, []interface{}, error) {
	return this.and.ToSql()
}

const COMPOUND = "COMPOUND"
const SIMPLE = "SIMPLE"
const AND = "且"
const OR = "或"

var noValueSymbolArr = []string{"isNotNull", "isNull"}
var rangeSymbolArr = []string{"range"}
var rangeTimeSymbolArr = []string{"rangeTime"}

func GetWhereSql(anlysisFilter request.AnalysisFilter) (SQL string, Args []interface{}, Cols []string, err error) {
	//sqlI为接口，有ToSql和Append 方法待实现

	var arrP sqlI
	colArr := []string{}

	//判断条件是且还是或
	switch anlysisFilter.Relation {
	case AND:
		arrP = &And{}
	case OR:
		arrP = &Or{}
	default:
		return "", nil, nil, errors.New("错误的连接类型:" + anlysisFilter.Relation)
	}

	//循环anlysisFilter.Filts
	for _, v := range anlysisFilter.Filts {
		if v.FilterType == SIMPLE {
			//追加列
			colArr = append(colArr, v.ColumnName)
			arrP.Append(getExpr(v.ColumnName, v.Comparator, v.Ftv))
		} else {
			var arrC sqlI
			switch v.Relation {
			case AND:
				arrC = &And{}
			case OR:
				arrC = &Or{}
			default:
				return "", nil, nil, errors.New("错误的连接类型")
			}

			for _, v2 := range v.Filts {
				colArr = append(colArr, v2.ColumnName)
				arrC.Append(getExpr(v2.ColumnName, v2.Comparator, v2.Ftv))
			}
			arrP.Append(arrC)
		}
	}
	sql, args, err := arrP.ToSql()

	return sql, args, colArr, err
}

func getArgMax(col string) string {
	return fmt.Sprintf(" argMax(%s, %s) %s ", col, ReplacingMergeTreeKey, col)
}

const ReplacingMergeTreeKey = "xwl_update_time"

var SpecialCloArr = []string{"xwl_distinct_id", "xwl_update_time"}

func GetUserTableView(tableId int, fields []string) string {

	colArr := []string{}

	for i, field := range fields {
		if util.InstrArr(SpecialCloArr, field) {
			fields = append(fields[:i], fields[i:]...)
			continue
		}
		colArr = append(colArr, getArgMax(field))
	}

	if len(colArr) > 0 {
		return " (select xwl_distinct_id," + strings.Join(colArr, ",") + " from xwl_user" + strconv.Itoa(tableId) + " xu group by xwl_distinct_id) "
	}

	return " (select xwl_distinct_id from xwl_user" + strconv.Itoa(tableId) + " xu group by xwl_distinct_id) "
}

/*
	columnName 字段名称
	comparator 操作符
	ftv 值
*/
func getExpr(columnName, comparator string, ftv interface{}) squirrel.Sqlizer {

	//操作符等于有值或者无值时
	if util.InstrArr(noValueSymbolArr, comparator) {
		//Expr 从 SQL 片段和参数构建表达式 ： isNotNull(columnName) or isNull(columnName)
		return squirrel.Expr(fmt.Sprintf("%v(%v)", comparator, columnName))
	}

	//操作符等于range时
	if util.InstrArr(rangeSymbolArr, comparator) {
		return squirrel.Expr(fmt.Sprintf(" ( %v >= ? and %v <= ? ) ", columnName, columnName), ftv.([]interface{})[0], ftv.([]interface{})[1])
	}

	//操作符在 rangeTime时，也就是日期区间
	if util.InstrArr(rangeTimeSymbolArr, comparator) {
		//参数不足
		if len(ftv.([]interface{})) != 2 {
			return squirrel.Expr(" 1 = 1 ")
		}

		//toDateTime : clickhouse 时间日期函数
		return squirrel.Expr(fmt.Sprintf(" ( %v >= toDateTime(?) and %v <= toDateTime(?) ) ", columnName, columnName), ftv.([]interface{})[0], ftv.([]interface{})[1])
	}

	//正则匹配： clickhouse match函数 字符串正则匹配，返回0或1
	if comparator == "match" {
		return squirrel.Expr(fmt.Sprintf("match(%v,?) = 1", columnName), ftv)
	}
	if comparator == "notmatch" {
		return squirrel.Expr(fmt.Sprintf("match(%v,?) = 0", columnName), ftv)
	}

	if comparator == "=" {
		return db.Eq{columnName: ftv}
	}
	if comparator == "!=" {
		return db.NotEq{columnName: ftv}
	}
	return squirrel.Expr(fmt.Sprintf("%v %v ?", columnName, comparator), ftv)
}
