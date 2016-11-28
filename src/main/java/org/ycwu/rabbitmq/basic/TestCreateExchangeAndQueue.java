package org.ycwu.rabbitmq.basic;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class TestCreateExchangeAndQueue {

	private String host = "192.168.157.128";
	private int port = 5672;
	private String username = "rabbit";
	private String password = "123456";

	private String exchangeName = "test.exchange";
	private String queueName = "test.queue";
	private String routingKey = "rkey";

	private ConnectionFactory connectionFactory;

	@Before
	public void init() {
		connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(host);
		connectionFactory.setPort(port);
		connectionFactory.setUsername(username);
		connectionFactory.setPassword(password);
	}

	private void closeChannel(Channel channel) {
		if (channel != null) {
			try {
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

	private void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testCreateExchangeAndQueue() {
		Connection connection = null;
		Channel channel = null;

		try {
			connection = connectionFactory.newConnection();
			channel = connection.createChannel();
			channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT);
			// 3rd parma exclusive: exclusive true if we are declaring an
			// exclusive queue
			// (restricted to this connection)
			channel.queueDeclare(queueName, false, false, false, null);
			channel.queueBind(queueName, exchangeName, routingKey);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			closeChannel(channel);
			closeConnection(connection);
		}
	}

	@Test
	public void testBasicPublish() {
		Connection connection = null;
		Channel channel = null;

		try {
			connection = connectionFactory.newConnection();
			channel = connection.createChannel();

			String msg = "hello world";
			channel.basicPublish(exchangeName, routingKey, null, msg.getBytes());
			System.out.println("message:" + msg);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			closeChannel(channel);
			closeConnection(connection);
		}
	}

	@Test
	public void testBasicConsume() {
		Connection connection = null;
		Channel channel = null;

		try {
			connection = connectionFactory.newConnection();
			channel = connection.createChannel();

			channel.basicConsume(queueName, new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					System.out.println("*** consumer ***");
					System.out.println("consumerTag:" + consumerTag);
					System.out.println("envelop:" + envelope.toString());
					String msg = new String(body);
					System.out.println("message:" + msg);
					System.out.println();
				}

			});

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			closeChannel(channel);
			closeConnection(connection);
		}
	}
}
