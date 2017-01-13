package org.ycwu.rabbitmq.rpc;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RPCServer {

	private static String queueRpc = "QUEUE.RPC";
	private Connection connection = null;
	private Channel channel = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		RPCServer server = new RPCServer();
		server.run();
	}

	public void run() {
		// TODO Auto-generated method stub
		try {
			ConnectionFactory factory = MQUtils.connectionFactory();
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.basicQos(1);
			channel.queueDeclare(queueRpc, false, false, false, null);

			System.out.println("*** rcp server starts");
			channel.basicConsume(queueRpc, false, new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					String msg = new String(body, "utf8");

					BasicProperties replyProps = new BasicProperties().builder()
							.correlationId(properties.getCorrelationId()).build();

					int i = Integer.valueOf(msg);
					String reply = String.valueOf(i * i);
					channel.basicPublish("", properties.getReplyTo(), replyProps, reply.getBytes("utf8"));
					channel.basicAck(envelope.getDeliveryTag(), false);					
				}

			});

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}

}
