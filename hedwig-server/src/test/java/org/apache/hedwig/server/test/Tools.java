package org.apache.hedwig.server.test;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.util.Callback;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class Tools {

	/**
	 * @param args
	 */
	static AtomicInteger numReceived = new AtomicInteger(0);
	static CountDownLatch receiveLatch = new CountDownLatch(1);

	/**
	 * This api is for test
	 * 
	 * @param args
	 * @throws Exception
	 */
	public void recv(String[] args) throws Exception {
		System.out.println("enter.........................rec");
		// java.security.Security.setProperty("networkaddress.cache.ttl", "0");
		final String queueName = args[0];
		final int numMessages = Integer.parseInt(args[1]);

		if ("0" == args[2])
			System.out.println(args[2] + "........................");
		String path = "F:/conf/hw_client.conf";
		final MsgBusClient client = new MsgBusClient(path);
		final MessageQueueClient mqClient = client.getMessageQueueClient();
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
		// ///////////////////////////////////////////
		System.out.println("numMessages:::::" + numMessages);
		mqClient.createQueue(queueName);
		long start = System.currentTimeMillis();
		mqClient.startDelivery(queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				System.out.println(Thread.currentThread().getName()
						+ "::deliver.." + msg.getMsgId().getLocalComponent()
						+ "::" + msg.getBody().toStringUtf8());

				if (numMessages == numReceived.incrementAndGet()) {
					receiveLatch.countDown();
				}

				try {
					mqClient.consumeMessage(queueName, msg.getMsgId());
					System.out.println(Thread.currentThread().getName()
							+ ":: consume.."
							+ msg.getMsgId().getLocalComponent() + "::"
							+ msg.getBody().toStringUtf8());
				} catch (ClientNotSubscribedException e) {
					e.printStackTrace();
				}

			}
		}, options);
		if (args[2] == "0") {
			assertTrue("Timed out waiting on callback for messages.",
					receiveLatch.await(10, TimeUnit.SECONDS));
			mqClient.stopDelivery(queueName);
			mqClient.closeSubscription(queueName);
		}

		else {
			TimeUnit.MILLISECONDS.sleep(50);
			client.close();
			System.out.println(".........one client quit..............."
					+ Thread.currentThread().getName());
		}

		long end = System.currentTimeMillis();
		System.out.println(Thread.currentThread().getName()
				+ "..........receiving finished.");
		System.out.println(Thread.currentThread().getName()
				+ "::Time cost for receiving is " + (end - start) + " ms.");
	}

}
