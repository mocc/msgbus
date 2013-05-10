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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase2;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.hedwig.util.Callback;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

@RunWith(Parameterized.class)
public class TestHubUnavailable extends HedwigHubTestBase2 {

	private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 1000;

	protected class TestServerConfiguration extends HubServerConfiguration {

		TestServerConfiguration(int serverPort, int sslServerPort) {
			super(serverPort, sslServerPort);
		}

		@Override
		public int getDefaultMessageWindowSize() {
			return TestHubUnavailable.DEFAULT_MESSAGE_WINDOW_SIZE;
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

	public TestHubUnavailable(boolean isSubscriptionChannelSharingEnabled) {

		// super(2); //default numServers is 2 :set in super class
		this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
		System.out.println("enter ..........................constructor");
	}

	public List<PubSubServer> getServer() {
		// TODO Auto-generated method stub
		return super.getServer();

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

	public void Recv() throws Exception {
		System.out.println("enter.........................rec");
		final PubSubServer defaultServer = this.getServer().get(0);
		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);

		final String queueName = "messageQueue-test";
		final int numMessages = Integer.parseInt("500");
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(500).build();

		final MsgBusClient client = new MsgBusClient(this.getClass()
				.getResource("/hw_client.conf"));
		final MessageQueueClient mqClient = client.getMessageQueueClient();
		mqClient.createQueue(queueName);

		System.out.println("numMessages:::::" + numMessages);
		long start = System.currentTimeMillis();

		mqClient.startDelivery(queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				logger.info("client fetch local.."
						+ msg.getMsgId().getLocalComponent() + "::"
						+ msg.getBody().toStringUtf8());

				if (numMessages == numReceived.incrementAndGet()) {
					try {
						mqClient.stopDelivery(queueName);

					} catch (ClientNotSubscribedException e) {
					}
					receiveLatch.countDown();
				}
				try {
					System.out.println(Thread.currentThread().getName()
							+ ":: consume.."
							+ msg.getMsgId().getLocalComponent() + "::"
							+ msg.getBody().toStringUtf8());
					mqClient.consumeMessage(queueName, msg.getMsgId());

				} catch (ClientNotSubscribedException e) {
					e.printStackTrace();
				}

			}
		}, options);

		TimeUnit.MILLISECONDS.sleep(10);
		logger.info("will shut down default server.........");
		defaultServer.shutdown();
		mqClient.closeSubscription(queueName);
		logger.info("default server is shut down.............");
		TimeUnit.MILLISECONDS.sleep(10);
		// mqClient1.createQueue(queueName);
		// System.out.println("mq1 finish create.................");
		final MsgBusClient client1 = new MsgBusClient(this.getClass()
				.getResource("/hw_client1.conf"));
		final MessageQueueClient mqClient1 = client1.getMessageQueueClient();
		mqClient1.createQueue(queueName);
		System.out.println("mq1 finish create.................");
		mqClient1.startDelivery(queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				logger.info("client fetch local.."
						+ msg.getMsgId().getLocalComponent() + "::"
						+ msg.getBody().toStringUtf8());

				if (numMessages == numReceived.incrementAndGet()) {
					try {
						mqClient1.stopDelivery(queueName);

					} catch (ClientNotSubscribedException e) {
					}
					receiveLatch.countDown();
				}
				try {
					System.out.println(Thread.currentThread().getName()
							+ ":: consume.."
							+ msg.getMsgId().getLocalComponent() + "::"
							+ msg.getBody().toStringUtf8());
					mqClient1.consumeMessage(queueName, msg.getMsgId());

				} catch (ClientNotSubscribedException e) {
					e.printStackTrace();
				}

			}
		}, options);
		System.out.println("mq1 start deliver.................");
		assertTrue("Timed out waiting on callback for messages.",
				receiveLatch.await(15, TimeUnit.SECONDS));
		mqClient1.closeSubscription(queueName);
		long end = System.currentTimeMillis();
		System.out.println(Thread.currentThread().getName()
				+ "..........receiving finished.");
		System.out.println("numReceived:" + numReceived.get());
		System.out.println(Thread.currentThread().getName()
				+ "::Time cost for receiving is " + (end - start) + " ms.");
	}

	@Test
	public void test() throws Exception {

		testMessageQueueClient_pub();
		Recv();

		Thread.sleep(20000);
		System.out.println("quit...........main");
	}
}
