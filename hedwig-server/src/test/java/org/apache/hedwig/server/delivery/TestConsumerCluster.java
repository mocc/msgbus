package org.apache.hedwig.server.delivery;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.test.Tools;
import org.apache.hedwig.util.Callback;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

@RunWith(Parameterized.class)
public class TestConsumerCluster extends HedwigHubTestBase {

	private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 10;

	protected class ConsumerClusterServerConfiguration extends
			HubServerConfiguration {

		ConsumerClusterServerConfiguration(int serverPort, int sslServerPort) {
			super(serverPort, sslServerPort);
		}

		@Override
		public int getDefaultMessageWindowSize() {
			System.out
					.println("enter..............................getDefaultMW");
			return TestConsumerCluster.this.DEFAULT_MESSAGE_WINDOW_SIZE;
		}
	}

	@Parameters
	public static Collection<Object[]> configs() {
		return Arrays.asList(new Object[][] { { true } /* , { true } */});
	}

	protected boolean isSubscriptionChannelSharingEnabled;

	public TestConsumerCluster(boolean isSubscriptionChannelSharingEnabled) {

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
		return new ConsumerClusterServerConfiguration(port, sslPort);
	}

	public void testMessageQueueClient_pub() throws Exception {
		// String path = "F:/conf/hw_client.conf";
		String path = "D:/workspace/msgbus/hedwig-client/conf/hw_client.conf";
		final MsgBusClient msgBusClient = new MsgBusClient(path);
		final MessageQueueClient client = msgBusClient.getMessageQueueClient();

		final String queueName = "messageQueue-test";
		int length = 1;
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < length; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();

		final int numMessages = 1000;

		final AtomicInteger numPublished = new AtomicInteger(0);
		final CountDownLatch publishLatch = new CountDownLatch(1);
		// final Map<String, MessageSeqId> publishedMsgs = new HashMap<String,
		// MessageSeqId>();

		long start = System.currentTimeMillis();

		System.out.println("Start to asynsPublish!");
		client.createQueue(queueName);
		client.publish(queueName, prefix + 1);

		// publishedMsgs.put(prefix+0, res.getPublishedMsgId());
		if (numMessages == numPublished.incrementAndGet()) {
			publishLatch.countDown();
		}

		for (int i = 2; i <= numMessages; i++) {

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
				publishLatch.await(3, TimeUnit.SECONDS));

		System.out.println("AsyncPublished " + numMessages + " messages in "
				+ (end - start) + " ms.");
		// return client;
	}

	@Test
	public void test() throws Exception {

		class QueueConsumer1 implements Runnable {

			@Override
			public void run() {
				Tools t = new Tools();

				String[] myArgs = new String[3];
				myArgs[0] = "messageQueue-test";
				myArgs[1] = "500";
				myArgs[2] = "0";

				try {
					t.recv(myArgs);
					System.out.println(Thread.currentThread().getName()
							+ ":quit.............recv");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// System.out.println("receiving finished.");
			}

		}
		class QueueConsumer2 implements Runnable {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				Tools t = new Tools();

				String[] myArgs = new String[3];
				myArgs[0] = "messageQueue-test";
				myArgs[1] = "500";
				myArgs[2] = "1";

				try {
					t.recv(myArgs);
					System.out.println(Thread.currentThread().getName()
							+ ":quit.............recv");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// System.out.println("receiving finished.");
			}

		}

		testMessageQueueClient_pub();
		new Thread(new QueueConsumer1()).start();
		new Thread(new QueueConsumer2()).start();

		Thread.sleep(15000);
		System.out.println("quit...........main");

	}
}