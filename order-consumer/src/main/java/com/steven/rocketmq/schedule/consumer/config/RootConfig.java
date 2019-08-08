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

		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order-consumer");

		consumer.setNamesrvAddr("10.174.44.181:9876;10.174.44.182:9876");
		consumer.subscribe("topic-order", "TagA || TagC || TagD");

		consumer.registerMessageListener(new MessageListenerOrderly() {

			Random random = new Random();

			@Override
			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {

				context.setAutoCommit(true);
				for (MessageExt msg : msgs) {
					// 可以看到每个 queue 有唯一的 consume 线程来消费, 订单对每个 queue(分区) 有序
					System.out.println("consumeThread=" + Thread.currentThread().getName() + "queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
				}

				try {
					// 模拟业务逻辑处理中...
					TimeUnit.SECONDS.sleep(random.nextInt(10));
				} catch (Exception e) {
					e.printStackTrace();
				}
				return ConsumeOrderlyStatus.SUCCESS;
			}
		});

		consumer.start();
		log.info("消费者: DefaultMQPushConsumer 启动成功");
		return consumer;
	}

}
