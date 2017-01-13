package org.ycwu.rabbitmq.rpc;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MQUtils {

	public static String host = "192.168.157.128";
	public static int port = 5672;
	public static String username = "rabbit";
	public static String password = "123456";
	public static ConnectionFactory factory;

	public static ConnectionFactory connectionFactory() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setPort(port);
		factory.setUsername(username);
		factory.setPassword(password);
		return factory;
	}

	public static void close(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void close(Channel channel) {
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

}
