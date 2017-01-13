package org.ycwu.rabbitmq.basic;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class TestTransaction {
	private static String host = "192.168.157.128";
	private static int port = 5672;
	private static String username = "rabbit";
	private static String password = "123456";
	private static ConnectionFactory factory;

	private static String exchange = "exchange.transaction";
	private static String queue = "queue.transaction";
	private static String routingKey = "trans";

	static void init() {
		factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setPort(port);
		factory.setUsername(username);
		factory.setPassword(password);
	}

	public static void main(String[] args) {
		new Thread(new Producer()).start();
		new Thread(new Consumer()).start();
	}

	static class Producer implements Runnable {

		public void run() {
			// TODO Auto-generated method stub
			Connection connection;
			Channel channel;
			try {
				connection = factory.newConnection();
				channel = connection.createChannel();
				channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT);
				channel.queueDeclare(queue, false, false, false, null);

				try {
					channel.txSelect();
					for (int i = 0; i < 100; i++) {
						channel.basicPublish(exchange, routingKey, null, String.valueOf(i).getBytes("utf8"));
						System.out.println("produce:" + i);
					}
					channel.txCommit();
				} catch (IOException e) {
					// TODO: handle exception
					channel.txRollback();
				}

				channel.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();

			} catch (TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	static class Consumer implements Runnable {

		public void run() {
			// TODO Auto-generated method stub
			Connection connection;
			Channel channel;
			try {
				connection = factory.newConnection();
				channel = connection.createChannel();
				channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT);
				channel.queueDeclare(queue, false, false, false, null);

				
				channel.basicConsume(queue, new DefaultConsumer(channel) {

					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
							byte[] body) throws IOException {
						// TODO Auto-generated method stub
						System.out.println("\tconsume:" + new String(body, "utf8"));
					}

				});

				channel.close();
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
