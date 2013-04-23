/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.qos;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.server.HedwigHubTestBase1;
import org.apache.hedwig.server.common.ServerConfiguration;
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
public class TestResendOverTimeout extends HedwigHubTestBase1 {

	private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 1000;

	protected class TestServerConfiguration extends HubServerConfiguration {

		TestServerConfiguration(int serverPort, int sslServerPort) {
			super(serverPort, sslServerPort);
		}

		@Override
		public int getDefaultMessageWindowSize() {
			return TestResendOverTimeout.DEFAULT_MESSAGE_WINDOW_SIZE;
		}
	}

	protected class TestClientConfiguration extends HubClientConfiguration {

		int messageWindowSize;

		TestClientConfiguration() {
			this.messageWindowSize = 50;
		}

		TestClientConfiguration(int messageWindowSize) {
			this.messageWindowSize = messageWindowSize;
		}

		@Override
		public int getMaximumOutstandingMessages() {
			return messageWindowSize;
		}

		void setMessageWindowSize(int messageWindowSize) {
			this.messageWindowSize = messageWindowSize;
		}

		@Override
		public boolean isAutoSendConsumeMessageEnabled() {
			return false;
		}

		@Override
		public boolean isSubscriptionChannelSharingEnabled() {
			return isSubscriptionChannelSharingEnabled;
		}
	}

	@Parameters
	public static Collection<Object[]> configs() {
		return Arrays.asList(new Object[][] { { true } /* , { true } */});
	}

	protected boolean isSubscriptionChannelSharingEnabled;

	public TestResendOverTimeout(boolean isSubscriptionChannelSharingEnabled) {

		super(1);
		this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
		System.out.println("enter ..........................constructor");
	}

	@BeforeClass
	public static void oneTimeSetUp() {
		// one-time initialization code
		System.setProperty("build.test.dir", "F:\\logDir");
	}

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
	}

	@Override
	protected ServerConfiguration getServerConfiguration(int port, int sslPort) {
		return new TestServerConfiguration(port, sslPort);
	}

	public void testMessageQueueClient_pub() throws Exception {
		// String path = "F:/conf/hw_client.conf";
		final MsgBusClient msgBusClient = new MsgBusClient(
				new TestClientConfiguration());
		final MessageQueueClient client = msgBusClient.getMessageQueueClient();

		final String queueName = "messageQueue-test";
		int length = 1;
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < length; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();

		final int numMessages = 500;

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

		class QueueConsumer implements Runnable {

			@Override
			public void run() {
				QosUtils t = new QosUtils();

				String[] myArgs = new String[3];
				myArgs[0] = "messageQueue-test";
				myArgs[1] = "500";
				myArgs[2] = "0";

				try {
					t.recv1(myArgs);
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
		new Thread(new QueueConsumer()).start();
		System.out.println("loop...........main");

		Thread.sleep(25000);
		// while (true) {
		// }
		System.out.println("quit...........main");

	}
}
