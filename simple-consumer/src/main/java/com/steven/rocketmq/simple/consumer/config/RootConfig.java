package com.steven.rocketmq.simple.consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Steven
 */
@Configuration
@Slf4j
public class RootConfig {

	@Bean
	public DefaultMQPushConsumer defaultMQPushConsumer() throws MQClientException {

		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("crm-order-consumer");

		consumer.setNamesrvAddr("10.174.44.181:9876;10.174.44.182:9876");
		consumer.subscribe("topic-simple", "*");

		consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
			System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		});

		consumer.start();
		log.info("消费者: DefaultMQPushConsumer 启动成功");
		return consumer;
	}

}
