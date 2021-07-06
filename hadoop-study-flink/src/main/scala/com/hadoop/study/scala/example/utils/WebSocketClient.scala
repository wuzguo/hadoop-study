package com.hadoop.study.scala.example.utils


import org.slf4j.LoggerFactory

import java.io.IOException
import java.net.URI
import javax.websocket._

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/6 15:51
 */

@ClientEndpoint
case class WebSocketClient(url: String) {

    private val log = LoggerFactory.getLogger(classOf[WebSocketClient])

    private var session: Session = _

    private val uri: URI = new URI(url)

    @OnOpen def open(session: Session): Unit = {
        log.info("Websocket正在开启会话")
        this.session = session
    }

    @OnMessage def onMessage(message: String): Unit = {
        log.info("Websocket接收信息:" + message)
    }

    @OnClose def onClose(): Unit = {
        log.info("Websocket已关闭")
    }

    @OnError def onError(session: Session, t: Throwable): Unit = {
        log.error("Websocket报错: {}", t)
    }

    //初始化
    def init(): Unit = {
        val container = ContainerProvider.getWebSocketContainer
        try container.connectToServer(this, uri)
        catch {
            case e: Exception =>
                log.error("Websocket初始化报错: {}", e)
        }
    }

    //发送字符串
    def send(message: String): Unit = {
        if (session == null || !session.isOpen) {
            log.error("Websocket状态异常,不能发送信息 - 尝试稍后连接")
            init()
        }
        else this.session.getAsyncRemote.sendText(message)
    }

    //关闭连接
    def close(): Unit = {
        if (this.session.isOpen) try this.session.close()
        catch {
            case e: IOException =>
                log.error("Websocket关闭报错: {}", e)
        }
    }
}