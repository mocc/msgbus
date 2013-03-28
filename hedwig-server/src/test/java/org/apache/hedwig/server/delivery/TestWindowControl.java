package org.apache.hedwig.server.delivery;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class TestWindowControl extends HedwigHubTestBase {

	// public MsgBusClient msgBusClient;
	// public MessageQueueClient mqClient;
	final String path = "D:/workspace/msgbus/hedwig-client/conf/hw_client.conf";
	// final String queueName = "mqclient_pub";//queue name

	protected boolean isSubscriptionChannelSharingEnabled;

	public TestWindowControl(boolean isSubscriptionChannelSharingEnabled) {

		super(1);
		this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
		System.out.println("enter ..........................constructor");
	}

	@BeforeClass
	public static void oneTimeSetUp() {
		// one-time initialization code
		System.out
				.println("@BeforeClass ------------------------- oneTimeSetUp");
		System.setProperty("build.test.dir", "F:\\test1");
		System.out.println("......................"
				+ System.getProperty("build.test.dir", "build"));// ly

	}

	@Override
	@Before
	public void setUp() throws Exception {
		// System.setProperty("build.test.dir", "F:\\test1");// ly
		super.setUp();
		System.out.println("quit.............................setup");
	}

	@Override
	protected ServerConfiguration getServerConfiguration(int port, int sslPort) {
		System.out.println("enter serverConfig........................");
		return new WindowControlServerConfiguration(port, sslPort);
	}

	protected class WindowControlServerConfiguration extends
			HubServerConfiguration {

		WindowControlServerConfiguration(int serverPort, int sslServerPort) {
			super(serverPort, sslServerPort);
		}
	}

	/* begin the publishing work for the message queue client */

	public void messageQueueClientPublish() throws Exception {
		final MsgBusClient msgBusClient = new MsgBusClient(path);
		final MessageQueueClient mqClient = msgBusClient
				.getMessageQueueClient();

		final String queueName = "mqClient_pub";

		int length = 1; // message length
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < length; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();

		final int numMessages = 100;// message number

		final AtomicInteger numPublished = new AtomicInteger(0);
		final CountDownLatch publishLatch = new CountDownLatch(1);
		// final Map<String, MessageSeqId> publishedMsgs = new HashMap<String,
		// MessageSeqId>();

		long start = System.currentTimeMillis();

		System.out.println("Start to asynsPublish!");

		mqClient.createQueue(queueName);

		// mqClient.publish(queueName, prefix + 1);

		// publishedMsgs.put(prefix+0, res.getPublishedMsgId());
		// if (numMessages == numPublished.incrementAndGet()) {
		// publishLatch.countDown();
		// }

		for (int i = 1; i <= numMessages; i++) {

			final String str = prefix + i;

			mqClient.asyncPublishWithResponse(queueName, str,
					new Callback<PublishResponse>() {
						@Override
						public void operationFinished(Object ctx,
								PublishResponse response) {
							// map, same message content results wrong

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

		// waiting for the publish work to finish

		System.out
				.println("before................................publish-assert");
		try {
			assertTrue("Timed out waiting on callback for publish requests.",
					publishLatch.await(3, TimeUnit.SECONDS));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("AsyncPublished " + numMessages + " messages in "
				+ (end - start) + " ms.");
		// return client;

	}

	// 一个msgClient，设置窗口5，循环发送5次消息，在每次delivery之前打印waiting和busy队列，
	// 当第五条消息发送完毕后，再打印waiting和busy队列，看msgClient是否由waiting队列移动到busy队列。
	// 然后consume一次，调用consumeMessage方法，然后打印waiting和busy队列看msgClient是否由busy队列移动到waiting队列。

	public void WindowControlReceive() throws Exception {

		final MsgBusClient msgBusclient = new MsgBusClient(path);
		final MessageQueueClient mqClient = msgBusclient
				.getMessageQueueClient();
		final String queueName = "mqClient_pub";
		final int numMessages = 20;

		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);

		// set the window size for this mqClient,here is 5
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(5).build();

		long start = System.currentTimeMillis();
		mqClient.createQueue(queueName);
		// client.getTime();
		// client.deleteQueue(queueName);

		mqClient.startDelivery(queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				System.out.println(msg.getMsgId().getLocalComponent()
						+ ":..........." + msg.getBody().toStringUtf8()
						+ "....." + Thread.currentThread().getName());
				// System.out.println(msg.getMsgId().getLocalComponent());

				if (numMessages == numReceived.incrementAndGet()) {
					receiveLatch.countDown();
				}
				// see whether mqClient is in waiting or busy queue

				callback.operationFinished(context, null);

				// try {
				// mqClient.consumeMessage(queueName, msg.getMsgId());
				// System.out.println(Thread.currentThread().getName()
				// + ":: consume.." + msg.getBody().toStringUtf8());
				// } catch (ClientNotSubscribedException e) {
				// e.printStackTrace();
				// }

			}
		}, options);

		// waiting for the receive work to finish
		System.out
				.println("before................................receive-assert");
		assertTrue("Timed out waiting on callback for messages.",
				receiveLatch.await(3, TimeUnit.SECONDS));
		mqClient.stopDelivery(queueName);
		mqClient.closeSubscription(queueName);
		long end = System.currentTimeMillis();

		System.out.println(Thread.currentThread().getName()
				+ "..........receiving finished.");
		System.out.println(Thread.currentThread().getName()
				+ "::Time cost for receiving is " + (end - start) + " ms.");

	}

}
