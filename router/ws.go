package router

import (
	. "github.com/1340691923/xwl_bi/controller"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
)

func routerWebsocket(app *fiber.App) {
	app.Use("/ws", func(c *fiber.Ctx) error {

		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get("/ws", websocket.New(DebugDataWs))
}
