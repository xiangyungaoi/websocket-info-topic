package com.zwxt.websocket2mt53.server;

import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.*;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by gaoyx on 2019/5/7.
 */
@Component
@Data
@ServerEndpoint(value = "/websocket/{reqid}")
public class Websocket2App {
    static Logger log = LoggerFactory.getLogger(Websocket2App.class);
    // 静态变量，用来记录当前在线连接数.应该把它设计成线程安全的.
    private static int onlineCount = 0;
    // concurrent包的线程安全Set,用来存放每个客户端对应的Websocket2App
    private static ConcurrentHashMap<String, Websocket2App> webSocketToApp2S = new ConcurrentHashMap<>();
    // 与某个客户端的连接会话，需要通过它来给客户端发送数据
    private Session session;
    // 存放前端需要的货币类型，用来过滤货币
    private String symbol;
    // 用于创建队列的通道
    private Channel channel;
    // 客户端的标识
    private String reqid;
    // 创建队列返回的结果
    private AMQP.Queue.DeclareOk declareOk;
    // 与MQ服务器的连接
    private static Connection connection;
    @Autowired
    public Websocket2App(Connection connection){
        this.connection = connection;
    }

    public Websocket2App() { }





    /**连接建立成功调用的方法
     * @param session hhh
     *
     */
    @OnOpen
    public  void onOpent(Session session, @PathParam(value = "reqid") String reqid){
        try {
            webSocketToApp2S.put(reqid, this);
            // 创建队列
            createQueue(reqid);
            this.session = session;
            // 连接数量+1
            addOnlineCount();
            log.info("前端连接后台WebSocket服务成功");
            sendMessage("连接成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** 声明队列
     * @param reqid
     * @throws IOException
     */
    private void createQueue(@PathParam(value = "reqid") String reqid) throws IOException {
        if (declareOk == null) {
            // 每次连接成功，都创建一个队列(消息过期时间为4s)，去接受fanout交换器中的消息,然后返回给前端App
            channel = Websocket2App.connection.createChannel(false);
            Map<String, Object> argss = new HashMap<>();
            // x-message-ttl队列中所有消息的过期时间
            argss.put("x-message-ttl", 4000);
            // x-expires 控制队列被自动删除前处于未使用状态的时间 10分钟
            // 未使用的意思是队列上没有任何的消费者，队列也没有被重新声明，并
            // 且在过期时间段 内也未调用过 Basic Get 命令。
            //  argss.put("x-expires", 600000);
            // 队列绑定fanout类型的交换器
            AMQP.Queue.DeclareOk declareOk = channel.queueDeclare(reqid, false, false, false, argss);
            this.declareOk = declareOk;
            channel.queueBind(reqid, "fanoutExchange", "");
        }
    }

    /*
     * 连接关闭调用的方法*/
    @OnClose
    public void onClose(){
        subOnlineCount();
        log.info("有一连接关闭！当前连接数为" + getOnlineCount());
    }

    /**收到客户端消息后调用的方法
     * @param message 客户端发送过来的消息
     * @param session 与客户端的会话
     */
    @OnMessage
    public void onMessage(String message, Session session){
        Map map = (Map) JSON.parseObject(message);
        this.reqid = (String) map.get("reqid");
        // 获取到要过滤货币的值
        this.symbol = (String) map.get("symbolname");
        try {
            channel.basicConsume(reqid, new Consumer() {
                @Override
                public void handleConsumeOk(String consumerTag) {

                }

                @Override
                public void handleCancelOk(String consumerTag) {

                }

                @Override
                public void handleCancel(String consumerTag) throws IOException {

                }

                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {

                }

                @Override
                public void handleRecoverOk(String consumerTag) {

                }

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body,"utf-8");
                    log.info(message);
                    Map messageMap = JSON.parseObject(message);
                    if (symbol.equals(messageMap.get("symbol"))){
                        webSocketToApp2S.get(reqid).sendMessage(message);
                    }
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @OnError
    public void onError(Session session, Throwable error){
        log.error("发生错误");
        error.printStackTrace();
    }




    /**服务器向客户端主动推送消息
     * @param message 消息
     */
    public void sendMessage(String message) throws IOException {
        if (this.session != null){
            this.session.getBasicRemote().sendText(message);
        }
    }

    public static synchronized int getOnlineCount() {
        return onlineCount;
    }

    public static synchronized void addOnlineCount() {
        Websocket2App.onlineCount++;
    }

    public static synchronized void subOnlineCount() {
        Websocket2App.onlineCount--;
    }

}
