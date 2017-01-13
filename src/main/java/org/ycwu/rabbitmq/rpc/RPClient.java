package org.ycwu.rabbitmq.rpc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RPClient {

	private static String queueRpc = "QUEUE.RPC";

	private Connection connection = null;
	private Channel channel = null;
	private String replyQueue = null;

	private Map<String, Integer> result = new HashMap<String, Integer>();

	public static void main(String[] args) throws UnsupportedEncodingException, IOException, TimeoutException {
		// TODO Auto-generated method stub
		RPClient client = new RPClient();
		client.init();

		for (int i = 1; i <= 5; i++) {
			System.out.println("call i=" + i);
			Integer rs = client.compute(i);
			System.out.println("i=" + i + " returns " + rs);
		}
	}

	public void init() {
		try {
			ConnectionFactory factory = MQUtils.connectionFactory();
			connection = factory.newConnection();
			channel = connection.createChannel();

			// create anonymous queue as reply queue per client, instead of per request
			replyQueue = channel.queueDeclare().getQueue();
			channel.queueDeclare(queueRpc, false, false, false, null);

			channel.basicConsume(replyQueue, new DefaultConsumer(channel) {

				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					Integer rs = Integer.valueOf(new String(body, "utf8"));
					result.put(properties.getCorrelationId(), rs);
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

	public Integer compute(int i) throws UnsupportedEncodingException, IOException {

		final String correlationId = UUID.randomUUID().toString();
		BasicProperties props = new BasicProperties().builder().replyTo(replyQueue).correlationId(correlationId)
				.build();
		channel.basicPublish("", queueRpc, props, String.valueOf(i).getBytes("utf8"));

		// timeout
		int count = 0;
		while (count++ < 5) {
			// check result by correlationId
			Integer rs = result.remove(correlationId);
			if (rs != null) {
				return rs;
			}

			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return null;
	}

}
