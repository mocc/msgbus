package com.yonyou.msgbus.example.queue;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class QueueConsumer {

	/**
	 * @param args
	 */	
	
	static class MyCallback implements Callback<ResponseBody> {
		@Override
		public void operationFailed(Object ctx, PubSubException exception) {
			// no-op
		}

		public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
			System.out.println("Hubs: "+(String)ctx);		
		}		
	}

	public static void main(String[] args) throws Exception {
		java.security.Security.setProperty("networkaddress.cache.ttl", "0");
		final int numMessages = 10000;		
		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);

		long start = System.currentTimeMillis();
		String path = "F:/Java Projects2/msgbus/hw_client.conf";
		final String queueName = "test";
		final MsgBusClient client = new MsgBusClient(path);
		//client.getAvailableHubs(new MyCallback());		
		
		final MessageQueueClient mqClient=client.getMessageQueueClient();
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(100).build();
		mqClient.createQueue(queueName);
		// client.getTime();
		// client.deleteQueue(queueName);
		System.out.println(numMessages);
		mqClient.startDelivery(queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				System.out.println(msg.getBody().toStringUtf8());
				System.out.println(msg.getMsgId().getLocalComponent());
				

				if (numMessages == numReceived.incrementAndGet()) {
					System.out.println("Last Seq: "+msg.getMsgId());
					receiveLatch.countDown();
				}	
				
				try {
					mqClient.consumeMessage(queueName, msg.getMsgId());
				} catch (ClientNotSubscribedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		}, options);
		assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(3000, TimeUnit.SECONDS));
		mqClient.stopDelivery(queueName);
		mqClient.closeSubscription(queueName);	
	
		System.out.println("receiving finished.");
		long end = System.currentTimeMillis();
		System.out.println("Time cost for receiving is " + (end - start) + " ms.");
	}

	/**
	 * This api is for test
	 * 
	 * @param args
	 * @throws Exception	
	 */
	public void recv(String[] args) throws Exception {
		java.security.Security.setProperty("networkaddress.cache.ttl", "0");
		final String queueName = args[0];
		final int numMessages = Integer.parseInt(args[2]);
		new CountDownLatch(1);
		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);

		long start = System.currentTimeMillis();
		String path = "F:/Java Projects2/msgbus/hw_client.conf";

		final MsgBusClient client = new MsgBusClient(path);
		final MessageQueueClient mqClient=client.getMessageQueueClient();
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(1000).build();
		mqClient.createQueue(queueName);
		// client.getTime();
		// client.deleteQueue(queueName);
		System.out.println(numMessages);

		mqClient.startDelivery(queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				System.out.println(msg.getBody().toStringUtf8());
				System.out.println(msg.getMsgId().getLocalComponent());

				if (numMessages == numReceived.incrementAndGet()) {
					receiveLatch.countDown();
				}
			
				try {
					mqClient.consumeMessage(queueName, msg.getMsgId());
				} catch (ClientNotSubscribedException e) {					
					e.printStackTrace();
				}
				
			}
		},options);
		assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(300, TimeUnit.SECONDS));
		mqClient.stopDelivery(queueName);
		mqClient.closeSubscription(queueName);
		long end = System.currentTimeMillis();

		System.out.println("receiving finished.");
		System.out.println("Time cost for receiving is " + (end - start) + " ms.");
	}
}
