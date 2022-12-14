package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"
	"time"

	"github.com/1340691923/xwl_bi/application"
	"github.com/1340691923/xwl_bi/cmd/sinker/action"
	"github.com/1340691923/xwl_bi/cmd/sinker/geoip"
	"github.com/1340691923/xwl_bi/engine/logs"
	"github.com/1340691923/xwl_bi/model"
	"github.com/1340691923/xwl_bi/platform-basic-libs/service/consumer_data"
	"github.com/1340691923/xwl_bi/platform-basic-libs/sinker"
	parser "github.com/1340691923/xwl_bi/platform-basic-libs/sinker/parse"
	"github.com/1340691923/xwl_bi/platform-basic-libs/util"
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

var (
	configFileDir  string
	configFileName string
	configFileExt  string
)

func init() {
	flag.StringVar(&configFileDir, "configFileDir", "config", "配置文件夹名")
	flag.StringVar(&configFileName, "configFileName", "config", "配置文件名")
	flag.StringVar(&configFileExt, "configFileExt", "json", "配置文件后缀")
	flag.Parse()

}

//核心逻辑都在sinker这边
/*
1 实时数据入es 也就是RealTimeWarehousing   					√
2 获取数据类型和ck的，把没有的字段记录下来 并且入mysql
3 判断数据类型 不正确看能不能转化 入 ReportAcceptStatus
4 最终数据入 ck
*/

func main() {

	//异常处理
	defer func() {
		if r := recover(); r != nil {
			//打印调用栈信息
			buf := make([]byte, 2048)
			n := runtime.Stack(buf, false)
			stackInfo := fmt.Sprintf("%s", buf[:n])
			logs.Logger.Sugar().Errorf("panic stack info %s", stackInfo)
			logs.Logger.Sugar().Errorf("--->consumer_data Error:", r)
		}
	}()

	//服务注册
	app := application.NewApp(
		"sinker",
		application.WithConfigFileDir(configFileDir),
		application.WithConfigFileName(configFileName),
		application.WithConfigFileExt(configFileExt),
		application.RegisterInitFnObserver(application.InitLogs),
		application.RegisterInitFnObserver(application.InitMysql),
		application.RegisterInitFnObserver(application.InitClickHouse),
		application.RegisterInitFnObserver(application.InitRedisPool),
	)

	err := app.InitConfig().
		NotifyInitFnObservers().
		Error()

	if err != nil {
		logs.Logger.Error("Sinker 初始化失败", zap.Error(err))
		panic(err)
	}

	defer app.Close()

	geoip2, err := geoip.NewGeoip(geoip.GeoipMmdbByte)
	fmt.Println("geoip.NewGeoip = ", geoip2)

	if err != nil {
		logs.Logger.Error("Geoip 初始化失败", zap.Error(err))
		panic(err)
	}

	defer geoip2.Close()

	//开启服务
	go func() {
		if model.GlobConfig.Sinker.PprofHttpPort != 0 {
			httpPort := ":" + strconv.Itoa(int(model.GlobConfig.Sinker.PprofHttpPort))
			if err := http.ListenAndServe(httpPort, nil); err != nil {
				logs.Logger.Info("err", zap.Error(err))
			}
		}
	}()

	log.Println(fmt.Sprintf("sinker 服务启动成功,性能检测入口为: http://127.0.0.1:%v", model.GlobConfig.Sinker.PprofHttpPort))

	sinkerC := model.GlobConfig.Sinker

	//实时数据仓库
	realTimeWarehousing := consumer_data.NewRealTimeWarehousing(sinkerC.RealTimeWarehousing)
	//上报状态
	reportAcceptStatus := consumer_data.NewReportAcceptStatus(sinkerC.ReportAcceptStatus)
	//上报数据到clickhouse
	reportData2CK := consumer_data.NewReportData2CK(sinkerC.ReportData2CK)

	//kafka数据流
	realTimeDataSarama := sinker.NewKafkaSarama()
	reportData2CKSarama := realTimeDataSarama.Clone()

	//开启协程，读取metaAttrRelationChan、attributeChan、metaEventChan通道，执行DDL操作
	go action.MysqlConsumer()
	//开启协程，每30分钟，删除sync.map集合数据以及缓存
	go sinker.ClearDimsCacheByTime(time.Minute * 30)
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	//初始化kafka
	err = realTimeDataSarama.Init(
		model.GlobConfig.Comm.Kafka,
		model.GlobConfig.Comm.Kafka.ReportTopicName,
		model.GlobConfig.Comm.Kafka.RealTimeDataGroup,
		func(msg model.InputMessage, markFn func()) {
			//ETL
			var kafkaData model.KafkaData
			err = json.Unmarshal(msg.Value, &kafkaData)
			fmt.Println("realTimeDataSarama.Init kafkaData = ", kafkaData)
			if err != nil {
				logs.Logger.Error("json.Unmarshal Err", zap.Error(err))
				markFn()
				return
			}
			appid, err := strconv.Atoi(kafkaData.TableId)
			if err != nil {
				logs.Logger.Error("strconv.Atoi(kafkaData.TableId) Err", zap.Error(err))
				markFn()
				return
			}
			//添加实时数据
			err = realTimeWarehousing.Add(&consumer_data.RealTimeWarehousingData{
				Appid:      int64(appid),
				EventName:  kafkaData.EventName,
				CreateTime: kafkaData.ReportTime,
				Data:       kafkaData.ReqData,
			})

			if err != nil {
				logs.Logger.Error("AddRealTimeData err", zap.Error(err))
			}
			markFn()

		}, func() {})

	if err != nil {
		panic(err)
	}

	err = reportData2CKSarama.Init(
		model.GlobConfig.Comm.Kafka,
		model.GlobConfig.Comm.Kafka.ReportTopicName,
		model.GlobConfig.Comm.Kafka.ReportData2CKGroup,
		func(msg model.InputMessage, markFn func()) {

			var kafkaData model.KafkaData

			err = json.Unmarshal(msg.Value, &kafkaData)
			if err != nil {
				logs.Logger.Error("json.Unmarshal Err", zap.Error(err))
				markFn()
				return
			}
			fmt.Println("reportData2CKSarama.Init kafkaData = ", kafkaData)

			kafkaData.Offset = msg.Offset
			kafkaData.ConsumptionTime = msg.Timestamp.Format(util.TimeFormat) //格式化时间

			//gjson 获取json串里的值
			gjsonArr := gjson.GetManyBytes(kafkaData.ReqData, "xwl_distinct_id", "xwl_client_time")

			xwlDistinctId := gjsonArr[0].String()

			xwlClientTime := gjsonArr[1].String()

			//获取tableid
			tableId, _ := strconv.Atoi(kafkaData.TableId)

			if kafkaData.EventName == "" {
				markFn()
				return
			}

			//记录不合法信息
			if xwlDistinctId == "" {
				logs.Logger.Error("xwl_distinct_id 为空", zap.String("kafkaData.ReqData", util.Bytes2str(kafkaData.ReqData)))

				var eventType = ""

				switch kafkaData.ReportType {
				case model.UserReportType:
					eventType = "用户属性类型不合法"
				case model.EventReportType:
					eventType = "事件属性类型不合法"
				}
				reportAcceptStatus.Add(&consumer_data.ReportAcceptStatusData{
					PartDate:       kafkaData.ReportTime,
					TableId:        tableId,
					ReportType:     eventType,
					DataName:       kafkaData.EventName,
					ErrorReason:    "xwl_distinct_id 不能为空",
					ErrorHandling:  "丢弃数据",
					ReportData:     util.Bytes2str(kafkaData.ReqData),
					XwlKafkaOffset: kafkaData.Offset,
					Status:         consumer_data.FailStatus,
				})
				markFn()
				return
			}

			//通过ip设置地址信息
			if kafkaData.Ip != "" {
				province, city, err := geoip2.GetAreaFromIP(kafkaData.Ip)
				if err != nil {
					logs.Logger.Sugar().Errorf("err", err)
				}
				if province != "" {
					kafkaData.ReqData, _ = sjson.SetBytes(kafkaData.ReqData, "xwl_province", province)
				}
				if city != "" {
					kafkaData.ReqData, _ = sjson.SetBytes(kafkaData.ReqData, "xwl_city", city)
				}
				kafkaData.ReqData, _ = sjson.SetBytes(kafkaData.ReqData, "xwl_ip", kafkaData.Ip)
			}
			clinetT := util.Str2Time(xwlClientTime, util.TimeFormat)
			serverT := util.Str2Time(kafkaData.ReportTime, util.TimeFormat)

			//上报时间差
			if math.Abs(serverT.Sub(clinetT).Minutes()) > 10 {
				reportAcceptStatus.Add(&consumer_data.ReportAcceptStatusData{
					PartDate:       kafkaData.ReportTime,
					TableId:        tableId,
					ReportType:     kafkaData.GetReportTypeErr(),
					DataName:       kafkaData.EventName,
					ErrorReason:    "客户端上报时间误差大于十分钟",
					ErrorHandling:  "丢弃数据",
					ReportData:     util.Bytes2str(kafkaData.ReqData),
					XwlKafkaOffset: kafkaData.Offset,
					Status:         consumer_data.FailStatus,
				})
				logs.Logger.Sugar().Errorf("客户端上报时间误差大于十分钟", xwlClientTime, kafkaData.ReportTime)
				markFn()
				return
			}

			//设置信息
			kafkaData.ReqData, _ = sjson.SetBytes(kafkaData.ReqData, "xwl_part_event", kafkaData.EventName)
			kafkaData.ReqData, _ = sjson.SetBytes(kafkaData.ReqData, "xwl_part_date", xwlClientTime)
			kafkaData.ReqData, _ = sjson.SetBytes(kafkaData.ReqData, "xwl_server_time", kafkaData.ReportTime)
			kafkaData.ReqData, _ = sjson.SetBytes(kafkaData.ReqData, "xwl_kafka_offset", msg.Offset)
			kafkaData.ReqData, _ = sjson.SetBytes(kafkaData.ReqData, "xwl_kafka_partition", msg.Partition)
			pp := parser.FastjsonParser{}

			//metric = kafkaData.ReqData的 fastjson.Value类型
			metric, err := pp.Parse(kafkaData.ReqData)
			fmt.Println("metric = ", metric)

			//解析开发者上报的json数据
			if err != nil {
				logs.Logger.Error("ParseKafkaData err", zap.Error(err))
				markFn()
				return
			}

			//生成表名 通过上报report_type判断时event还是user
			tableName := kafkaData.GetTableName()

			//新增表结构
			if err := action.AddTableColumn(
				kafkaData,
				func(data consumer_data.ReportAcceptStatusData) { reportAcceptStatus.Add(&data) },
				tableName,
				metric,
			); err != nil {
				logs.Logger.Error("addTableColumn err", zap.String("tableName", tableName), zap.Error(err))
				markFn()
				return
			}

			//添加元数据
			if err := action.AddMetaEvent(kafkaData); err != nil {
				logs.Logger.Error("addMetaEvent err", zap.Error(err))
			}

			//入库成功
			if err := reportAcceptStatus.Add(&consumer_data.ReportAcceptStatusData{
				PartDate:       kafkaData.ReportTime,
				TableId:        tableId,
				DataName:       kafkaData.EventName,
				XwlKafkaOffset: kafkaData.Offset,
				Status:         consumer_data.SuccessStatus,
			}); err != nil {
				logs.Logger.Error("reportAcceptStatus Add SuccessStatus err", zap.Error(err))
			}
			//添加数据到ck用于后台统计
			if err := reportData2CK.Add(consumer_data.FastjsonMetricData{
				TableName:      tableName,
				FastjsonMetric: metric,
			}); err != nil {
				logs.Logger.Error("reportData2CK err", zap.Error(err))
				markFn()
				return
			}
			markFn()

			//logs.Logger.Info("链路所花时长", zap.String("time", time.Now().Sub(startT).String()))

		}, func() {})

	if err != nil {
		panic(err)
	}

	go reportData2CKSarama.Run()
	go realTimeDataSarama.Run()

	app.WaitForExitSign(func() {
		if err := reportData2CKSarama.Stop(); err != nil {
			logs.Logger.Sugar().Infof("reportData2CKSarama 停止失败", err)
		}
		if err := realTimeDataSarama.Stop(); err != nil {
			logs.Logger.Sugar().Infof("realTimeDataSarama 停止失败", err)
		}
	}, func() {
		if err := reportData2CK.FlushAll(); err != nil {
			logs.Logger.Sugar().Infof("清理 reportData2CK FlushAll 失败", err)
		} else {
			logs.Logger.Sugar().Infof("清理reportData2CK完毕")
		}
	}, func() {
		if err := realTimeWarehousing.FlushAll(); err != nil {
			logs.Logger.Sugar().Infof("清理 realTimeWarehousing 失败", err)
		} else {
			logs.Logger.Sugar().Infof("清理realTimeWarehousing 完毕")
		}
	}, func() {
		if err := reportAcceptStatus.FlushAll(); err != nil {
			logs.Logger.Sugar().Infof("清理 reportAcceptStatus 失败", err)
		} else {
			logs.Logger.Sugar().Infof("清理reportAcceptStatus 完毕")
		}
	})
}
