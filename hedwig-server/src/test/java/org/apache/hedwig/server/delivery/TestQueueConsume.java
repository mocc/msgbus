package org.apache.hedwig.server.delivery;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.util.Callback;
import org.junit.Before;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class TestQueueConsume extends HedwigHubTestBase {

	MsgBusClient msgBusClient;
	MessageQueueClient client;

	public void QueueProducer(String path) throws MalformedURLException,
			ConfigurationException {
		msgBusClient = new MsgBusClient(path);
		client = msgBusClient.getMessageQueueClient();

	}

	static class MyCallback implements Callback<ResponseBody> {
		@Override
		public void operationFailed(Object ctx, PubSubException exception) {
			// no-op
		}

		public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
			System.out.println("Hubs: " + (String) ctx);
		}
	}

	@Override
	@Before
	public void setUp() throws Exception {
		System.out.println("1");
		System.setProperty("build.test.dir", "F:\\test1");

		super.setUp();
	}

	public void testQueueXXX() throws Exception {

		String path = "D:/workspaces0/msgbus/hedwig-client/conf/hw_client.conf";
		final String queueName = "test";
		ByteString topic = ByteString.copyFromUtf8("test");
		ByteString subscriberId = ByteString.copyFromUtf8("msgbus1");

		TestQueueConsume tqc = new TestQueueConsume();
		tqc.QueueProducer(path);

		String[] params = { "test", "30", "100" };
		if (params.length < 3) {
			System.out.println("Parameters: queueName msgLen num");
			return;
		}
		// /publish messages begin
		tqc.asyncSendWithResponse(params);

		// //subscribe messages
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(100).build();

		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);
		final int numMessages = Integer.parseInt(params[2]);

		long start = System.currentTimeMillis();
		client.createQueue(queueName);
		client.startDelivery(queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				System.out.println(msg.getBody().toStringUtf8());
				System.out.println(msg.getMsgId().getLocalComponent());

				if (numMessages == numReceived.incrementAndGet()) {
					System.out.println("Last Seq: " + msg.getMsgId());
					receiveLatch.countDown();
				}

				try {
					client.consumeMessage(queueName, msg.getMsgId());
				} catch (ClientNotSubscribedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, options);
		assertTrue("Timed out waiting on callback for messages.",
				receiveLatch.await(3000, TimeUnit.SECONDS));
		client.stopDelivery(queueName);
		client.closeSubscription(queueName);

		System.out.println("receiving finished.");
		long end = System.currentTimeMillis();
		System.out.println("Time cost for receiving is " + (end - start)
				+ " ms.");
	}

	public void asyncSendWithResponse(String args[]) throws Exception {
		if (args.length < 3) {
			System.out.println("Parameters: topic msgLen num");
			return;
		}

		System.out.println("Version: 2012-11-16.");
		String queueName = args[0];

		int length = Integer.parseInt(args[1]);
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < length; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();

		final int numMessages = Integer.parseInt(args[2]);

		final AtomicInteger numPublished = new AtomicInteger(0);
		final CountDownLatch publishLatch = new CountDownLatch(1);
		// final Map<String, MessageSeqId> publishedMsgs = new HashMap<String,
		// MessageSeqId>();

		new AtomicInteger(0);
		new CountDownLatch(1);
		new HashMap<String, MessageSeqId>();

		long start = System.currentTimeMillis();

		System.out.println("Start to asynsPublish!");
		client.createQueue(queueName);
		client.publish(queueName, prefix + 0);

		// publishedMsgs.put(prefix+0, res.getPublishedMsgId());
		if (numMessages == numPublished.incrementAndGet()) {
			publishLatch.countDown();
		}

		for (int i = 1; i < numMessages; i++) {

			final String str = prefix + i;

			client.asyncPublishWithResponse(queueName, str,
					new Callback<PublishResponse>() {
						@Override
						public void operationFinished(Object ctx,
								PublishResponse response) {
							// map, same message content results wrong
							// publishedMsgs.put(str,
							// response.getPublishedMsgId());
							if (numMessages == numPublished.incrementAndGet()) {
								publishLatch.countDown();
							}
						}

						@Override
						public void operationFailed(Object ctx,
								final PubSubException exception) {
						}
					}, null);
		}
		long end = System.currentTimeMillis();

		// wait the work to finish
		assertTrue("Timed out waiting on callback for publish requests.",
				publishLatch.await(30, TimeUnit.SECONDS));

		System.out.println("AsyncPublished " + numMessages + " messages in "
				+ (end - start) + " ms.");
		msgBusClient.close();
	}

}