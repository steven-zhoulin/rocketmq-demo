package com.steven.rocketmq.schedule.producer.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 应用场景: 比如电商里，提交了一个订单就可以发送一个延时消息，1h后去检查这个订单的状态，如果还是未付款就取消订单释放库存。
 *
 * @author Steven
 */
@Slf4j
@Component
public class Task {

	@Autowired
	private DefaultMQProducer defaultMQProducer;

	@Scheduled(fixedDelay = 1000 * 1000)
	public void orderProducer() {

		try {

			int totalMessagesToSend = 100;
			for (int i = 0; i < totalMessagesToSend; i++) {
				Message message = new Message("topic-schedule", ("Hello scheduled message " + i).getBytes());
				// This message will be delivered to consumer 10 seconds later.
				message.setDelayTimeLevel(3);
				// Send the message
				defaultMQProducer.send(message);
				log.info("发送: " + message);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
