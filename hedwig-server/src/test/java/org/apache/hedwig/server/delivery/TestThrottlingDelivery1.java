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
package org.apache.hedwig.server.delivery;

import java.util.Arrays;
import java.util.Collection;
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
import org.apache.hedwig.server.HedwigHubTestBase1;
import org.apache.hedwig.server.common.ServerConfiguration;
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
import com.yonyou.msgbus.client.PubSubClient;
import com.yonyou.msgbus.example.queue.QueueConsumer;

@RunWith(Parameterized.class)
public class TestThrottlingDelivery1 extends HedwigHubTestBase1 {

	private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 10;

	protected class ThrottleDeliveryServerConfiguration extends
			HubServerConfiguration {

		ThrottleDeliveryServerConfiguration(int serverPort, int sslServerPort) {
			super(serverPort, sslServerPort);
		}

		@Override
		public int getDefaultMessageWindowSize() {
			return TestThrottlingDelivery1.this.DEFAULT_MESSAGE_WINDOW_SIZE;
		}
	}

	protected class ThrottleDeliveryClientConfiguration extends
			HubClientConfiguration {

		int messageWindowSize;

		ThrottleDeliveryClientConfiguration() {
			this(TestThrottlingDelivery1.this.DEFAULT_MESSAGE_WINDOW_SIZE);
		}

		ThrottleDeliveryClientConfiguration(int messageWindowSize) {
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
		return Arrays.asList(new Object[][] { { false } /* , { true } */});
	}

	protected boolean isSubscriptionChannelSharingEnabled;

	public TestThrottlingDelivery1(boolean isSubscriptionChannelSharingEnabled) {

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
		return new ThrottleDeliveryServerConfiguration(port, sslPort);
	}

	public void testPubSubClient() throws Exception {
		System.out.println("enter...................................test");
		String topic = "testServerSideThrottle";
		final String subid = "serverThrottleSub";

		int length = 1;
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < length; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();

		final int numMessages = 30;
		String path = "F:/conf/hw_client.conf";
		final MsgBusClient msgBusClient = new MsgBusClient(path);
		final PubSubClient client = msgBusClient.getPubSubClient();

		final AtomicInteger numPublished = new AtomicInteger(0);
		final CountDownLatch publishLatch = new CountDownLatch(1);
		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);

		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(40).build();

		System.out.println("Start to asynsPublish!");
		// first publish a message synchronously, to establish a connection.
		// ////////////////////////////////
		long start = System.currentTimeMillis();
		client.subscribe(topic, subid, options);
		client.publish(topic, prefix + 0);

		if (numMessages == numPublished.incrementAndGet()) {
			publishLatch.countDown();
		}

		// then pub other messages asynchronously
		for (int i = 1; i < numMessages; i++) {

			final String str = prefix + i;

			client.asyncPublishWithResponse(topic, str,
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
		System.out
				.println("before................................publish-assert");
		assertTrue("Timed out waiting on callback for publish requests.",
				publishLatch.await(3, TimeUnit.SECONDS));

		System.out.println("AsyncPublished " + numMessages + " messages in "
				+ (end - start) + " ms.");
		System.out.println("message num is.......................:"
				+ numMessages);

		long start1 = System.currentTimeMillis();
		client.startDelivery(topic, subid, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				System.out.println("message is................."
						+ msg.getBody().toStringUtf8());
				if (numMessages == numReceived.incrementAndGet()) {
					receiveLatch.countDown();
				}

				/*
				 * try { pubSubClient.consume(myTopic, mySubid, msg.getMsgId());
				 * } catch (ClientNotSubscribedException e) { // TODO
				 * Auto-generated catch block e.printStackTrace(); }
				 */
				// this is necessary!
				callback.operationFinished(context, null);
			}
		});
		System.out
				.println("before................................receive-assert");
		assertTrue("Timed out waiting on callback for messages.",
				receiveLatch.await(3, TimeUnit.SECONDS));
		client.stopDelivery(topic, subid);
		client.closeSubscription(topic, subid);
		long end1 = System.currentTimeMillis();
		System.out.println("Received " + numMessages + " messaged in "
				+ (end1 - start1) + " ms.");
	}

	public void testMessageQueueClient() throws Exception {
		String path = "F:/conf/hw_client.conf";
		final MsgBusClient msgBusClient = new MsgBusClient(path);
		final MessageQueueClient client = msgBusClient.getMessageQueueClient();

		final String queueName = "messageQueue-test";
		int length = 1;
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < length; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();

		final int numMessages = 30;

		final AtomicInteger numPublished = new AtomicInteger(0);
		final CountDownLatch publishLatch = new CountDownLatch(1);
		// final Map<String, MessageSeqId> publishedMsgs = new HashMap<String,
		// MessageSeqId>();

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
				publishLatch.await(3, TimeUnit.SECONDS));

		System.out.println("AsyncPublished " + numMessages + " messages in "
				+ (end - start) + " ms.");

		// ///////////////////////////////////
		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(10).build();
		long start1 = System.currentTimeMillis();
		client.startDelivery(queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				System.out.println(msg.getBody().toStringUtf8()
						+ "...messageId is..."
						+ msg.getMsgId().getLocalComponent());

				if (numMessages == numReceived.incrementAndGet()) {
					System.out.println("Last Seq: " + msg.getMsgId());
					receiveLatch.countDown();
				}

//				try {
//					client.consumeMessage(queueName, msg.getMsgId());
//				} catch (ClientNotSubscribedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
		}, options);
		assertTrue("Timed out waiting on callback for messages.",
				receiveLatch.await(3000, TimeUnit.SECONDS));
		client.stopDelivery(queueName);
		// client.closeSubscription(queueName);
		client.deleteQueue(queueName);
		System.out.println("receiving finished.");
		long end1 = System.currentTimeMillis();
		System.out.println("Time cost for receiving is " + (end1 - start1)
				+ " ms.");
		// ///////////////////////////////////
		msgBusClient.close();
	}

	public void testMessageQueueClient1() throws Exception {
		String path = "F:/conf/hw_client.conf";
		final MsgBusClient msgBusClient = new MsgBusClient(path);
		final MessageQueueClient client = msgBusClient.getMessageQueueClient();

		final String queueName = "messageQueue-test";
		int length = 1;
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < length; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();

		final int numMessages = 30;

		final AtomicInteger numPublished = new AtomicInteger(0);
		final CountDownLatch publishLatch = new CountDownLatch(1);
		// final Map<String, MessageSeqId> publishedMsgs = new HashMap<String,
		// MessageSeqId>();

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
				publishLatch.await(3, TimeUnit.SECONDS));

		System.out.println("AsyncPublished " + numMessages + " messages in "
				+ (end - start) + " ms.");

		// ///////////////////////////////////
		String[] myArgs = new String[3];
		myArgs[0] = "messageQueue-test";
		myArgs[1] = "1";
		myArgs[2] = "30";
		QueueConsumer c = null;
		c = new QueueConsumer();
		c.recv(myArgs);
		// new QueueConsumer().recv(myArgs, client);
		// ///////////////////////////////////
		msgBusClient.close();
	}

	public void testMessageQueueClient_pub() throws Exception {
		String path = "F:/conf/hw_client.conf";
		final MsgBusClient msgBusClient = new MsgBusClient(path);
		final MessageQueueClient client = msgBusClient.getMessageQueueClient();

		final String queueName = "messageQueue-test";
		int length = 1;
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < length; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();

		final int numMessages = 30;

		final AtomicInteger numPublished = new AtomicInteger(0);
		final CountDownLatch publishLatch = new CountDownLatch(1);
		// final Map<String, MessageSeqId> publishedMsgs = new HashMap<String,
		// MessageSeqId>();

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
				publishLatch.await(3, TimeUnit.SECONDS));

		System.out.println("AsyncPublished " + numMessages + " messages in "
				+ (end - start) + " ms.");
		// return client;
	}

	public void testMutiQueueConsumer() throws Exception {
		System.out
				.println("enter.........................testMutiQueueConsumer");

		MultiQueueConsumer m1 = new MultiQueueConsumer();
		MultiQueueConsumer m2 = new MultiQueueConsumer();
		m1.setName("consumer1");
		m2.setName("consumer2");
		System.out.println(m1.getName() + "///////////////////////////");
		m1.start();
		m2.start();
	}

	// //////////////////////////////////
	@Test
	public void test() throws Exception {
		testMessageQueueClient();
		// testPubSubClient();
		// testMessageQueueClient1();
		// ///////////////////////////////////////
		// testMessageQueueClient_pub();
		// testMutiQueueConsumer();
		// System.out.println("before sleep....................");
		// Thread.sleep(6000);
		// System.out.println("quit sleep,main.................");
		// ////////////////////////////////////////
		// TestJunit tj = new TestJunit();
		// tj.start();
		// Thread.sleep(5000);
		// assertTrue(" ",true);

	}
}

class MultiQueueConsumer extends Thread {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("enter.........................run");
		QueueConsumer c = null;

		c = new QueueConsumer();

		String[] myArgs = new String[3];
		myArgs[0] = "messageQueue-test";
		myArgs[1] = "1";
		myArgs[2] = "30";

		try {
			c.recv(myArgs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println("receiving finished.");
	}
}

// junit的assert出现在其他线程则junit窗口不会判断assert的结果。
class TestJunit extends Thread {

	@Override
	public void run() {
		// TODO Auto-generated method stub

		QueueConsumer qc = new QueueConsumer();
		try {
			qc.test();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// assertTrue需要静态引入
		// assertTrue("Timed out waiting on callback for messages.", true);

	}
}