package com.zwxt.websocket2mt53.config;

import lombok.Data;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Created by gaoyx on 2019/5/7.
 *
 *
 * *Broker:它提供一种传输服务,它的角色就是维护一条从生产者到消费者的路线，保证数据能按照指定的方式进行传输,
 *Exchange：消息交换机,它指定消息按什么规则,路由到哪个队列。
 *Queue:消息的载体,每个消息都会被投到一个或多个队列。
 *Binding:绑定，它的作用就是把exchange和queue按照路由规则绑定起来.
 *Routing Key:路由关键字,exchange根据这个关键字进行消息投递。
 *vhost:虚拟主机,一个broker里可以有多个vhost，用作不同用户的权限分离。
 *Producer:消息生产者,就是投递消息的程序.
 *Consumer:消息消费者,就是接受消息的程序.
 *Channel:消息通道,在客户端的每个连接里,可建立多个channel.
 */
@Configuration
@ConfigurationProperties(prefix = "spring.rabbitmq")
@Data
public class RabbitMqConfig {
    // 连接MQ服务器的host地址
    private String host;
    // MQ服务器的端口号
    private int port;
    // 连接的用户名
    private String userName;
    // 连接的密码
    private String password;

    // 连接广播模式队列的交换器
    public static String EXCHANGE_QUEUE_BROADCAST = "exchange2Queuebroadcast";
    // 广播模式的mq服务器
    public static String QUEUE_BROADCAST = "broadcastQueue";

    //路由键
    public static String ROUTINGKEY_QUEUE_BROADCAST = "broadcastQueue";




    @Bean
    public ConnectionFactory connectionFactory(){
        CachingConnectionFactory factory = new CachingConnectionFactory(); //CachingConnectionFactory RabbitMQ的缓存连接池
        factory.setHost(host);
        factory.setPort(port);
        factory.setVirtualHost("/");
        factory.setUsername(userName);
        factory.setPassword(password);
        //开启生产者发布确认,即生产者在发送消息给mq的时候，mq会返回数据告诉生产者 消息是否发送成功
        //生产者需要添加ConfirmCallback，mq返回数据的时候执行ConfirmCallback
        factory.setPublisherConfirms(true);
        return factory;
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    //必须是prototype类型
    public RabbitTemplate rabbitTemplate(){ return new RabbitTemplate(connectionFactory());}

    @Bean
    public Connection connection(){ return connectionFactory().createConnection(); }
}
