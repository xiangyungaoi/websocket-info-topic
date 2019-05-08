package com.zwxt.websocket2mt53.config;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by gaoyx on 2019/5/7.
 */
@Configuration
@Component
public class WebsocketConif {
    private static Logger log = LoggerFactory.getLogger(WebsocketConif.class);

    @Autowired
    private RabbitTemplate rabbitTemplate;
    /**
     * 开启WebSocket支持
     * ServerEndpointExporter作用
     * 这个Bean会自动注册使用@ServerEndpoint注解声明的websocket endpoint
     *
     * @return 这个Bean会自动注册使用@ServerEndpoint注解声明的websocket endpoint
     */
    @Bean
    public ServerEndpointExporter serverEndpointExporter() {
        return new ServerEndpointExporter();
    }

    @Bean
    public WebSocketClient webSocketClient(){
        WebSocketClient webSocketClient = null;
        try {
            webSocketClient = new WebSocketClient(new URI("ws://api.digiexclub.com:8091/"), new Draft_6455()) {
                @Override
                public void onOpen(ServerHandshake serverHandshake) {
                    log.info("[websocket] 连接mt5服务器成功");
                }

                @Override
                public void onMessage(String message) {
                 /*   log.info("收到mt5返回的消息"+ message);*/
                   // 将消息发送给fanout类型的交换器fanoutExchange,让交换器fanoutExchange发送给与之绑定的队列
                   rabbitTemplate.send("fanoutExchange","",new Message(message.getBytes(),new MessageProperties()));
                }

                @Override
                public void onClose(int code, String reason, boolean b) {
                    System.out.println(code);
                    System.out.println(reason);
                    log.info("退出mt5服务器连接");
                }

                @Override
                public void onError(Exception e) {
                    log.info("连接错误={}",e.getMessage());
                }
            };
            webSocketClient.connect();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return webSocketClient;
    }

}
