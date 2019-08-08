package com.steven.rocketmq.schedule.consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author Steven
 */
@Configuration
@Slf4j
public class RootConfig {

	@Bean
	public DefaultMQPushConsumer defaultMQPushConsumer() throws MQClientException {

		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("schedule-consumer");

		consumer.setNamesrvAddr("10.174.44.181:9876;10.174.44.182:9876");
		consumer.subscribe("topic-schedule", "*");

		consumer.registerMessageListener(new MessageListenerConcurrently() {

			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				for (MessageExt message : msgs) {
					// Print approximate delay time period
					log.info("Receive message[msgId=" + message.getMsgId() + "] " + (System.currentTimeMillis() - message.getBornTimestamp()) + "ms later");
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}

		});

		consumer.start();
		log.info("消费者: DefaultMQPushConsumer 启动成功");
		return consumer;
	}

}
