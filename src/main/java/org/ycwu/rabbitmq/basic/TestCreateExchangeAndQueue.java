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
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class TestCreateExchangeAndQueue {

	private String host = "192.168.157.128";
	private int port = 5672;
	private String username = "rabbit";
	private String password = "123456";

	private String exchangeName = "test.direct";
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
			// 3rd param exclusive: exclusive true if we are declaring an
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

	@Test
	public void testFanoutExchange() {
		Channel channel = null;
		Connection connection = null;

		String fanoutExchange = "test.fanout";
		String queue = "test.fanout.queue";
		String msg = "hello moto";
		try {
			connection = connectionFactory.newConnection();
			channel = connection.createChannel();
			channel.exchangeDeclare(fanoutExchange, BuiltinExchangeType.FANOUT);
			channel.queueDeclare(queue, false, false, false, null);
			channel.queueBind(queue, fanoutExchange, "");

			Consumer consumer = new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					String s = new String(body);
					System.out.println("consume:" + s);
				}
			};

			channel.basicPublish(fanoutExchange, "", null, msg.getBytes());

			channel.basicConsume(queue, consumer);
			channel.basicConsume(queue, consumer);

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
	public void testTopicExchange() {
		Channel channel = null;
		Connection connection = null;

		try {
			connection = connectionFactory.newConnection();
			channel = connection.createChannel();

			String userTopicExchange = "test.topic.user";
			channel.exchangeDeclare(userTopicExchange, BuiltinExchangeType.TOPIC);

			// event queue
			final String userCreatedQueue = "user-created-queue";
			final String userDeletedQueue = "user-deleted-queue";
			final String userUpdatedQueue = "user-updated-queue";
			final String userActionsQueue = "user-actions-queue";

			channel.queueDeclare(userCreatedQueue, false, false, false, null);
			channel.queueDeclare(userDeletedQueue, false, false, false, null);
			channel.queueDeclare(userUpdatedQueue, false, false, false, null);
			channel.queueDeclare(userActionsQueue, false, false, false, null);

			// command queue
			String userCreateCmdQueue = "user-create-cmd-queue";
			channel.queueDeclare(userCreateCmdQueue, false, false, false, null);

			// topic exchange acts like direct exchange when routing key
			// contains no * or #
			channel.queueBind(userCreatedQueue, userTopicExchange, "user.event.created");
			channel.queueBind(userDeletedQueue, userTopicExchange, "user.event.deleted");
			channel.queueBind(userUpdatedQueue, userTopicExchange, "user.event.updated");

			// a typical topic
			channel.queueBind(userActionsQueue, userTopicExchange, "user.event.*");

			channel.queueBind(userCreateCmdQueue, userTopicExchange, "user.cmd.create");

			// messages sent to routing_key="user.event.deleted" will be
			// actually sent to userDeletedQueue and userActionsQueue
			String msg = "{uid:1, status:'deleted'}";
			channel.basicPublish(userTopicExchange, "user.event.deleted", null, msg.getBytes());

			boolean autoAck = true;
			channel.basicConsume(userActionsQueue, autoAck, new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					System.out.println("queue:" + userActionsQueue);
					System.out.println(new String(body));
					System.out.println();
				}

			});

			channel.basicConsume(userDeletedQueue, autoAck, new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					System.out.println("queue:" + userDeletedQueue);
					System.out.println(new String(body));
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

	/**
	 * one queue can be bind with different routing keys<br>
	 * this demo is from
	 * https://www.rabbitmq.com/tutorials/tutorial-five-java.html
	 */
	@Test
	public void testTopicExchange2() {
		Channel channel = null;
		Connection connection = null;

		try {
			connection = connectionFactory.newConnection();
			channel = connection.createChannel();

			String exchange = "animal.topic";
			channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC);
			channel.queueDeclare("q1", false, false, false, null);
			channel.queueDeclare("q2", false, false, false, null);

			// "<speed>.<color>.<species>"
			channel.queueBind("q1", exchange, "*.orange.*");
			channel.queueBind("q2", exchange, "*.*.rabbit");
			channel.queueBind("q2", exchange, "lazy.#");

			Consumer q1_consumer = new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					System.out.println("q1:" + new String(body));
					System.out.println();
				}
			};
			Consumer q2_consumer = new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					System.out.println("q2:" + new String(body));
					System.out.println();
				}
			};

			boolean autoAck = true;
			String msg = "lazy orange rabbit";
			channel.basicPublish(exchange, "lazy.orange.rabbit", null, msg.getBytes());
			channel.basicConsume("q1", autoAck, q1_consumer);
			channel.basicConsume("q2", autoAck, q2_consumer);

			// this message matches 2 bindings of q2, but wil only be sent once
			String msg2 = "lazy pink rabbit";
			channel.basicPublish(exchange, "lazy.pink.rabbit", null, msg2.getBytes());
			channel.basicConsume("q1", autoAck, q1_consumer);
			channel.basicConsume("q2", autoAck, q2_consumer);
			channel.basicConsume("q2", autoAck, q2_consumer);

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
