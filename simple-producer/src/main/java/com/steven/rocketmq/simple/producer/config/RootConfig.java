package com.steven.rocketmq.simple.producer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Steven
 */
@Configuration
@Slf4j
public class RootConfig {

	@Bean
	public DefaultMQProducer defaultMQProducer() throws MQClientException {
		DefaultMQProducer producer = new DefaultMQProducer("simple-producer");
		producer.setNamesrvAddr("10.174.44.181:9876;10.174.44.182:9876");
		producer.setRetryTimesWhenSendAsyncFailed(0);
		producer.start();
		log.info("生产者(同步): DefaultMQProducer 启动成功。");
		return producer;
	}
}
