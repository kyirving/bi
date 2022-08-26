//路由层
package router

import (
	. "github.com/1340691923/xwl_bi/controller"
	. "github.com/1340691923/xwl_bi/middleware"
	"github.com/1340691923/xwl_bi/views"
	. "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	jsoniter "github.com/json-iterator/go"
)

func Init() *App {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	//路由服务
	app := New(Config{
		AppName:     "铸龙-BI",
		JSONDecoder: json.Unmarshal,
		JSONEncoder: json.Marshal,
	})

	//压缩中间件
	app.Use(compress.New(compress.Config{
		Level: compress.LevelBestCompression,
	}))

	//文件系统中间件
	app.Use("/", filesystem.New(filesystem.Config{
		Root: views.GetFileSystem(),
	}))

	app.Use(
		cors.New(),
		pprof.New(),
	)

	app.Post("/api/gm_user/login", ManagerUserController{}.Login)
	routerWebsocket(app)
	app.Use(
		Timer,
		JwtMiddleware,
		Rbac,
	)

	return runRouterGroupFn(
		app,
		runOperaterLog,
		runGmUser,
		runRealData,
		runMetaData,
		runAnalysis,
		runPannel,
		runApp, //应用管理模块
		runUserGroup,
	)
}

type routerGroupFn func(app *App)

func runRouterGroupFn(app *App, fns ...routerGroupFn) *App {
	for _, fn := range fns {
		fn(app)
	}
	return app
}
