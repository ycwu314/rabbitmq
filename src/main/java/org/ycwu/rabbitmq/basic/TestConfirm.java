package org.ycwu.rabbitmq.basic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class TestConfirm {

	private static String host = "192.168.157.128";
	private static int port = 5672;
	private static String username = "rabbit";
	private static String password = "123456";
	private static ConnectionFactory factory;

	private static String queue = "queue-confirm-test";
	private static String exchange = "exchange-confirm-test";
	private static String routingKey = "misc";

	private static int count = 100;

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		init();
		new Thread(new Producer()).start();
		new Thread(new Consumer()).start();
	}

	static void init() {
		factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setPort(port);
		factory.setUsername(username);
		factory.setPassword(password);
	}

	static class Producer implements Runnable {

		public void run() {
			// TODO Auto-generated method stub
			try {
				Connection conn = factory.newConnection();
				Channel channel = conn.createChannel();
				channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT);
				channel.queueDeclare(queue, false, false, false, null);
				channel.queueBind(queue, exchange, routingKey);

				channel.addConfirmListener(new ConfirmListener() {

					public void handleNack(long deliveryTag, boolean multiple) throws IOException {
						// TODO Auto-generated method stub
						System.out.println("\tnack:" + deliveryTag);
					}

					public void handleAck(long deliveryTag, boolean multiple) throws IOException {
						// TODO Auto-generated method stub
						System.out.println("ack:" + deliveryTag);
					}
				});

				channel.confirmSelect();
				
				for (int i = 1; i <= count; i++) {

					channel.basicPublish(exchange, routingKey, null,
							String.valueOf(i).getBytes(StandardCharsets.UTF_8));

					// 1. confirm for each publish
					// 2. bulk confirm for every xx publish
					if (i % 10 == 0) {
						channel.waitForConfirms();
					}

					System.out.println("produce:" + i);
				}

				channel.waitForConfirmsOrDie();

				channel.close();
				conn.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	static class Consumer implements Runnable {

		public void run() {
			// TODO Auto-generated method stub
			try {
				Connection conn = factory.newConnection();
				Channel channel = conn.createChannel();
				channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT);
				channel.queueDeclare(queue, false, false, false, null);
				channel.queueBind(queue, exchange, routingKey);

				DefaultConsumer consumer = new DefaultConsumer(channel) {

					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
							byte[] body) throws IOException {
						// TODO Auto-generated method stub
						System.out.println("\tConsume:" + new String(body, StandardCharsets.UTF_8));
					}
				};

				// return consumerTag
				boolean autoAck = true;
				channel.basicConsume(queue, autoAck, consumer);

				channel.close();
				conn.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
