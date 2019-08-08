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
 * @author Steven
 */
@Slf4j
@Component
public class Task {

	@Autowired
	private DefaultMQProducer defaultMQProducer;

	private String[] tags = new String[]{"TagA", "TagC", "TagD"};

	@Scheduled(fixedDelay = 1000 * 1000)
	public void orderProducer() {

		try {

			// 订单列表
			List<OrderStep> orderList = buildOrders();

			Date date = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String dateStr = sdf.format(date);

			for (int i = 0; i < 10; i++) {
				// 加个时间前缀
				String body = dateStr + " Hello RocketMQ " + orderList.get(i);
				Message msg = new Message("topic-order", tags[i % tags.length], "KEY_" + i, body.getBytes());

				// 按订单号来选择 Queue
				SendResult sendResult = defaultMQProducer.send(msg, new MessageQueueSelector() {

					/**
					 * 根据订单 id 选择发送 Queue
					 *
					 * @param messageQueues
					 * @param msg
					 * @param arg
					 * @return
					 */
					@Override
					public MessageQueue select(List<MessageQueue> messageQueues, Message msg, Object arg) {
						Long id = (Long) arg;
						long index = id % messageQueues.size();
						return messageQueues.get((int) index);
					}

				}, orderList.get(i).getOrderId());

				System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
					sendResult.getSendStatus(),
					sendResult.getMessageQueue().getQueueId(),
					body));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 订单的步骤
	 */
	private static class OrderStep {
		private long orderId;
		private String desc;

		public long getOrderId() {
			return orderId;
		}

		public void setOrderId(long orderId) {
			this.orderId = orderId;
		}

		public String getDesc() {
			return desc;
		}

		public void setDesc(String desc) {
			this.desc = desc;
		}

		@Override
		public String toString() {
			return "OrderStep{" +
				"orderId=" + orderId +
				", desc='" + desc + '\'' +
				'}';
		}
	}

	/**
	 * 生成模拟订单数据
	 */
	private static List<OrderStep> buildOrders() {

		List<OrderStep> orderList = new ArrayList<>();

		OrderStep orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111039L);
		orderDemo.setDesc("创建");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111065L);
		orderDemo.setDesc("创建");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111039L);
		orderDemo.setDesc("付款");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103117235L);
		orderDemo.setDesc("创建");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111065L);
		orderDemo.setDesc("付款");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103117235L);
		orderDemo.setDesc("付款");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111065L);
		orderDemo.setDesc("完成");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111039L);
		orderDemo.setDesc("推送");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103117235L);
		orderDemo.setDesc("完成");
		orderList.add(orderDemo);

		orderDemo = new OrderStep();
		orderDemo.setOrderId(15103111039L);
		orderDemo.setDesc("完成");
		orderList.add(orderDemo);

		return orderList;
	}

}
