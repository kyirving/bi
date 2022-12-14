package analysis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/1340691923/xwl_bi/engine/db"
	"github.com/1340691923/xwl_bi/engine/logs"
	"github.com/1340691923/xwl_bi/platform-basic-libs/my_error"
	"github.com/1340691923/xwl_bi/platform-basic-libs/request"
	"github.com/1340691923/xwl_bi/platform-basic-libs/service/analysis/utils"
	jsoniter "github.com/json-iterator/go"
)

type Event struct {
	sql                 string
	args                []interface{}
	eventNameDisplayArr []string
	req                 request.EventReqData
	divisorIndex        int32
}

func (this *Event) getDivisorName() string {
	this.divisorIndex = atomic.AddInt32(&this.divisorIndex, 1)
	return "divisor" + strconv.Itoa(int(this.divisorIndex))
}

func (this *Event) GetList() (interface{}, error) {

	SQL, args, err := this.GetExecSql()
	fmt.Println("GetList.GetExecSql() SQL = ", SQL)
	fmt.Println("GetList.GetExecSql() args = ", SQL)
	fmt.Println()

	if err != nil {
		return nil, err
	}

	logs.Logger.Sugar().Infof("sql", SQL, args)

	rows, err := db.ClickHouseSqlx.Query(SQL, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	for index, _ := range cache {
		var a interface{}
		cache[index] = &a
	}
	var list []map[string]interface{}

	for rows.Next() {
		err := rows.Scan(cache...)
		if err != nil {
			return nil, err
		}
		item := make(map[string]interface{})
		for i, data := range cache {
			item[columns[i]] = *data.(*interface{})
		}
		list = append(list, item)
	}
	return map[string]interface{}{"alldata": list, "use_group": len(this.req.GroupBy) > 0, "len": len(this.req.ZhibiaoArr), "groupby": this.req.GroupBy, "eventNameDisplayArr": this.eventNameDisplayArr}, nil
}

const (
	ByDay    = "按天"
	ByMinute = "按分钟"
	ByHour   = "按小时"
	ByWeek   = "按周"
	Monthly  = "按月"
	ByTotal  = "合计"
)

const (
	Zhibiao = 1
	Formula = 2
)

/*
	index 下标
	sql 条件段
	args 条件段参数
*/
func (this *Event) getSqlByZhibiao(index int, sql string, args []interface{}) (SQL string, allArgs []interface{}, err error) {
	zhibiao := this.req.ZhibiaoArr[index]

	//获取指标sql段
	eventSql, eventArgs := this.whereInZhibiaoEvent(zhibiao)

	fmt.Println("eventSql = ", eventSql)
	fmt.Println("eventArgs = ", eventArgs)
	fmt.Println()

	sql = sql + eventSql

	args = append(args, eventArgs...)
	fmt.Println("sql = ", sql)
	fmt.Println("args = ", args)
	fmt.Println()

	//"  date_group ", "formatDateTime(xwl_part_date,'%Y年%m月%d日') as date_group "
	dateGroupSql, dateGroupCol := this.GetGroupDateSql()

	//获取分组
	groupArr, groupCol := this.GetGroupSql()
	fmt.Println("groupArr = ", groupArr)
	fmt.Println("groupCol = ", groupCol)
	fmt.Println()

	copyGroupArr := groupArr

	if dateGroupCol != "" {
		groupCol = append(groupCol, dateGroupCol)
	}

	if dateGroupSql != "" {
		groupArr = append(groupArr, dateGroupSql)
	}
	groupBySql := ""
	if len(groupArr) > 0 {
		groupBySql = " group by "
	}

	whereSql := ""
	whereArgs := []interface{}{}

	withSql := ""
	argsWith := []interface{}{}
	switch zhibiao.Typ {
	//指标
	case Zhibiao:
		if len(zhibiao.SelectAttr) == 0 {
			return "", nil, errors.New("请选择维度")
		}

		whereSql, whereArgs, _, err = utils.GetWhereSql(zhibiao.Relation)
		if err != nil {
			return "", nil, err
		}

		selectAttr := this.req.ZhibiaoArr[index].SelectAttr

		// col = toString("count()") or toString(fmt.Sprintf("count(%s)", col))
		col := fmt.Sprintf(" (%s) as %s ", utils.CountTypMap[selectAttr[1]](selectAttr[0]), "amount")
		fmt.Println("col = ", col)

		groupCol = append(groupCol, col)
	//公式
	case Formula:
		if len(zhibiao.One.SelectAttr) == 0 || len(zhibiao.Two.SelectAttr) == 0 {
			return "", nil, errors.New("请选择维度")
		}

		_, sqlOne, _, argsOne, err := this.getFormulaSql(zhibiao.One, false, zhibiao.DivisorNoGrouping)
		if err != nil {
			return "", nil, err
		}
		sqlTwo := ""
		argsTwo := []interface{}{}

		withSql, sqlTwo, argsWith, argsTwo, err = this.getFormulaSql(zhibiao.Two, true, zhibiao.DivisorNoGrouping)

		if err != nil {
			return "", nil, err
		}
		argsTmp := []interface{}{}
		argsTmp = append(argsTmp, argsOne...)
		argsTmp = append(argsTmp, argsTwo...)

		args = append(argsTmp, args...)

		switch zhibiao.ScaleType {
		case utils.TwoDecimalPlaces:
			fmtStr := fmt.Sprintf("  %v(%v,%v) ", zhibiao.Operate, utils.ToFloat32OrZero(sqlOne), utils.ToFloat32OrZero(sqlTwo))
			groupCol = append(groupCol, fmt.Sprintf("toString(%s)   amount", utils.Round(utils.NaN2Zero(fmtStr))))
		case utils.Percentage:
			fmtStr := fmt.Sprintf("  %v(%v,%v) ", zhibiao.Operate, utils.ToFloat32OrZero(sqlOne), utils.ToFloat32OrZero(sqlTwo))
			groupCol = append(groupCol, fmt.Sprintf(" concat(toString( %v *100),%s)   amount", utils.Round(utils.NaN2Zero(fmtStr)), `'%'`))
		case utils.Rounding:
			fmtStr := fmt.Sprintf("  %v(%v,%v) ", zhibiao.Operate, utils.ToFloat32OrZero(sqlOne), utils.ToFloat32OrZero(sqlTwo))
			groupCol = append(groupCol, fmt.Sprintf("toString(round(%s,0))   amount", utils.NaN2Zero(fmtStr)))
		}

	default:
		return "", nil, errors.New("未知指标类型")
	}

	args = append(argsWith, args...)

	args = append(args, whereArgs...)

	if whereSql != "" {
		whereSql = " and " + whereSql
	}

	SQL = ` from ( ` + withSql + `  select ` + strings.Join(groupCol, ",") + ` from xwl_event` + strconv.Itoa(this.req.Appid) + ` prewhere ` + sql + whereSql + this.sql + groupBySql + strings.Join(groupArr, ",") + ` order by date_group ` + `) `

	if len(copyGroupArr) > 0 {
		SQL = SQL + " group by " + strings.Join(copyGroupArr, ",")
	}

	copyGroupArr = append(copyGroupArr, `arrayMap((x, y) -> (x, y),groupArray(date_group),groupArray(amount)) as data_group`)
	eventNameDisplay := "" + zhibiao.EventNameDisplay + "(" + strconv.Itoa(index+1) + ")"
	this.eventNameDisplayArr = append(this.eventNameDisplayArr, eventNameDisplay)
	copyGroupArr = append(copyGroupArr, "'"+eventNameDisplay+"'"+" as eventNameDisplay ", "count(1)  group_num", ``+strconv.Itoa(index+1)+` as serial_number`)

	SQL = `select ` + strings.Join(copyGroupArr, ",") + SQL
	SQL = SQL + ` limit 1000 `

	allArgs = append(args, this.args...)
	return
}

func (this *Event) GetExecSql() (SQL string, allArgs []interface{}, err error) {

	//获取 req.whereFilter 条件段
	whereSql, whereArgs, _, err := utils.GetWhereSql(this.req.WhereFilter)
	fmt.Println("whereSql = ", whereSql)
	fmt.Println("whereArgs = ", whereArgs)
	fmt.Println()

	if err != nil {
		return "", nil, err
	}

	//获取 req.date 日期sql条件段
	filterDateSql, filterDateArgs := this.GetFilterDateSql()
	fmt.Println("filterDateSql = ", filterDateSql)

	//获取 req.whereFilterByUser 用户sql条件段
	usersql, userArgs, err := getUserfilterSqlArgs(this.req.WhereFilterByUser, this.req.Appid)
	fmt.Println("usersql = ", usersql)
	fmt.Println("userArgs = ", userArgs)
	fmt.Println()

	if err != nil {
		return "", nil, err
	}

	//sql 拼接
	sql := whereSql + usersql + filterDateSql
	args := []interface{}{}
	args = append(args, whereArgs...)
	args = append(args, userArgs...)
	args = append(args, filterDateArgs...)

	fmt.Println("sql = ", sql)
	fmt.Println("args = ", args)
	fmt.Println()

	sqlArr := []string{}

	//循环指标， 单个指标代表一个字段
	for index := range this.req.ZhibiaoArr {

		fmt.Println("index = ", index)
		fmt.Println("sql = ", sql)
		fmt.Println("args = ", args)
		sql, args, err := this.getSqlByZhibiao(index, sql, args)
		if err != nil {
			return "", nil, err
		}
		sqlArr = append(sqlArr, sql)
		allArgs = append(allArgs, args...)
	}

	orderBY := []string{}

	if len(this.req.GroupBy) > 0 {
		orderBY = append(orderBY, this.req.GroupBy...)
	} else {
		orderBY = append(orderBY, "serial_number")
	}

	fmt.Println("order by  = ", orderBY)
	return `select * from (` + strings.Join(sqlArr, " union all ") + `) order by ` + strings.Join(orderBY, ","), allArgs, err
}

func (this *Event) GetFilterDateSql() (SQL string, args []interface{}) {

	startTime := this.req.Date[0] + " 00:00:00"
	endTime := this.req.Date[1] + " 23:59:59"
	args = append(args, startTime)
	args = append(args, endTime)

	SQL = ` and xwl_part_date >= toDateTime(?) and xwl_part_date <= toDateTime(?) `

	return
}

/*
	zhibiao 指标对象
*/
func (this *Event) whereInZhibiaoEvent(zhibiao request.EventZhibiao) (SQL string, args []interface{}) {

	colsArr := []interface{}{}

	switch zhibiao.Typ {
	case Zhibiao:
		colsArr = append(colsArr, zhibiao.EventName)
	case Formula:
		arr := []interface{}{}
		arr = append(arr, zhibiao.One.EventName)
		if !zhibiao.DivisorNoGrouping {
			arr = append(arr, zhibiao.Two.EventName)
		}

		colsArr = append(colsArr, arr)
	}

	if len(colsArr) == 0 {
		return "", nil
	}

	return " and xwl_part_event in (?) ", colsArr
}

func (this *Event) getFormulaSql(dimension request.FormulaDimension, isDivisor, divisorNoGrouping bool) (withSql, sql string, withArgs, args []interface{}, err error) {

	eventFilter := " xwl_part_event = ? "

	args = append(args, dimension.EventName)

	whereSql, whereArgs, _, err := utils.GetWhereSql(dimension.Relation)

	if err != nil {
		return
	}

	args = append(args, whereArgs...)

	if whereSql != "" {
		eventFilter = eventFilter + " and " + whereSql
	}

	if divisorNoGrouping && isDivisor {

		filterDateSql, filterDateArgs := this.GetFilterDateSql()

		args = append(args, filterDateArgs...)
		groupSql, groupCol := this.GetGroupDateSql()
		fmtStr := ` with ( select cast((groupArray(date_group),groupArray(tmp)) AS Map(String, String)) as withDataMap from (select %v from xwl_event` + strconv.Itoa(this.req.Appid) + ` prewhere %v  group by %v)  ) as %v `
		divisorName := this.getDivisorName()
		withSql = fmt.Sprintf(fmtStr, utils.CountTypMap[dimension.SelectAttr[1]](dimension.SelectAttr[0])+" as tmp ,"+groupCol, eventFilter+filterDateSql, groupSql, divisorName)

		sql = ` toFloat64OrZero(mapValues(` + divisorName + `)[indexOf(mapKeys(` + divisorName + `), ` + groupSql + `)])  `
		withArgs = args
		args = nil

	} else {
		if dimension.SelectAttr[0] == utils.Default {
			switch dimension.SelectAttr[1] {
			case utils.AllCount:
				sql = utils.CountTypMap[dimension.SelectAttr[1]](`if(` + eventFilter + `, 1, null)`)
			case utils.ClickUserNum:
				sql = utils.CountTypMap[dimension.SelectAttr[1]](`if(` + eventFilter + `,xwl_distinct_id, null)`)
			case utils.AvgCount:
				sql = utils.CountTypMap[dimension.SelectAttr[1]](`if(` + eventFilter + `, 1, null)` + utils.SPLIT + `if(` + eventFilter + `, xwl_distinct_id, null)`)
			}
		} else {
			sql = utils.CountTypMap[dimension.SelectAttr[1]](`if(` + eventFilter + `, ` + dimension.SelectAttr[1] + `, null)`)
		}
		sql = utils.ToFloat32OrZero(sql)
	}

	return
}

func (this *Event) GetGroupDateSql() (groupSQL string, groupCol string) {

	switch this.req.WindowTimeFormat {
	case ByDay:
		return "  date_group ", "formatDateTime(xwl_part_date,'%Y年%m月%d日') as date_group "
	case ByHour:
		return "  date_group ", " formatDateTime(xwl_part_date,'%Y年%m月%d日 %H点') as date_group "
	case ByMinute:
		return "  date_group ", " formatDateTime(xwl_part_date,'%Y年%m月%d日 %H点%M分') as date_group "
	case ByWeek:
		return "  date_group ", " formatDateTime(xwl_part_date,'%Y年%m月 星期%u')  as date_group "
	case Monthly:
		return "  date_group ", " formatDateTime(xwl_part_date,'%Y年%m月') as date_group"
	case ByTotal:
		return " date_group ", " '合计' as date_group "
	}

	return
}

/*
	分组
*/
func (this *Event) GetGroupSql() (groupSql []string, groupCol []string) {

	for _, groupby := range this.req.GroupBy {
		groupSql = append(groupSql, groupby)
		groupCol = append(groupCol, fmt.Sprintf(" %s as %s ", groupby, groupby))
	}

	return
}

func NewEvent(reqData []byte) (Ianalysis, error) {
	obj := &Event{}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err := json.Unmarshal(reqData, &obj.req)
	if err != nil {
		return nil, err
	}

	//时间参数不正确
	if len(obj.req.Date) < 2 {
		return nil, my_error.NewBusiness(ERROR_TABLE, TimeError)
	}

	//指标参数不正确
	if len(obj.req.ZhibiaoArr) <= 0 {
		return nil, my_error.NewBusiness(ERROR_TABLE, ZhiBiaoNumError)
	}

	//检测分组字段
	for _, groupby := range obj.req.GroupBy {
		if groupby == "" {
			return nil, my_error.NewBusiness(ERROR_TABLE, GroupEmptyError)
		}
	}
	obj.sql, obj.args, err = utils.GetUserGroupSqlAndArgs(obj.req.UserGroup, obj.req.Appid)
	if err != nil {
		return nil, err
	}

	fmt.Println("NewEvent() obj.sql = ", obj.sql)
	fmt.Println()
	fmt.Println("NewEvent() obj.args = ", obj.args)

	return obj, nil
}
