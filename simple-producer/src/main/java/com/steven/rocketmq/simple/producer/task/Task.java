package com.steven.rocketmq.simple.producer.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * @author Steven
 */
@Slf4j
@Component
public class Task {

	@Autowired
	private DefaultMQProducer defaultMQProducer;

	/**
	 * 同步发送，广泛用于重要的通知消息、短信通知、短信营销等场景。
	 */
	@Scheduled(initialDelay = 10000, fixedRate = 6000)
	public void syncProducer() {

		try {

			Message msg = new Message(
				"topic-simple",
				"tag-sync",
				("Simple Sync " + UUID.randomUUID().toString()).getBytes(RemotingHelper.DEFAULT_CHARSET)
			);

			SendResult sendResult = defaultMQProducer.send(msg);
			System.out.printf("%s%n", sendResult);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 异步发送，常用语对时间敏感的业务场景。
	 */
	@Scheduled(initialDelay = 10000, fixedRate = 4000)
	public void asyncProducer() {

		try {

			Message msg = new Message(
				"topic-simple",
				"tag-async",
				("Simple Async " + UUID.randomUUID().toString()).getBytes(RemotingHelper.DEFAULT_CHARSET)
			);

			defaultMQProducer.send(msg, new SendCallback() {
				@Override
				public void onSuccess(SendResult sendResult) {
					System.out.printf("OK %s %n", sendResult.getMsgId());
				}

				@Override
				public void onException(Throwable e) {
					System.out.printf("Exception %s %n", e);
					e.printStackTrace();
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 单向传输，主要用于对可靠性要求不是很严格的场景，比如日志传输。
	 */
	@Scheduled(initialDelay = 10000, fixedRate = 2000)
	public void oneWayProducer() {

		try {

			Message msg = new Message(
				"topic-simple",
				"tag-oneway",
				("Simple Oneway " + UUID.randomUUID().toString()).getBytes(RemotingHelper.DEFAULT_CHARSET)
			);

			defaultMQProducer.sendOneway(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
