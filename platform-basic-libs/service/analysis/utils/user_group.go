package utils

import (
	"fmt"
	"strings"

	"github.com/1340691923/xwl_bi/engine/db"
	"github.com/1340691923/xwl_bi/model"
	"github.com/1340691923/xwl_bi/platform-basic-libs/util"
	"github.com/Masterminds/squirrel"
)

func GetUserGroupSqlAndArgs(ids []int, appid int) (SQL string, Args []interface{}, err error) {
	if len(ids) == 0 {
		return " and ( 1 = 1 ) ", nil, err
	}

	//SqlBuilder 用于生产sql语句
	sql, args, err := db.
		SqlBuilder.
		Select("user_list").
		From("user_group").
		Where(db.Eq{"appid": appid, "id": ids}).
		ToSql()

	fmt.Println("GetUserGroupSqlAndArgs() sql = ", sql)
	fmt.Println("GetUserGroupSqlAndArgs() args = ", args)

	//用于保存数据
	var userGroupList []model.UserGroup

	//选择使用此数据库。任何占位符参数都将替换为提供的参数
	err = db.Sqlx.Select(&userGroupList, sql, args...)

	if err != nil {
		return "", nil, err
	}

	or := squirrel.Or{}

	for index := range userGroupList {
		idStr, err := util.GzipUnCompress(userGroupList[index].UserList)
		if err != nil {
			return "", nil, err
		}
		id := strings.Split(idStr, ",")
		or = append(or, db.Eq{"xwl_distinct_id": [][]string{id}})
	}

	SQL, Args, err = or.ToSql()
	SQL = " and " + SQL
	return SQL, Args, err
}
