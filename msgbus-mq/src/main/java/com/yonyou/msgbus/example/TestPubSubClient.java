package com.yonyou.msgbus.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MsgBusClient;
import com.yonyou.msgbus.client.PubSubClient;

public class TestPubSubClient {
	private PubSubClient client;

	public static Logger logger = LoggerFactory.getLogger(TestPubSubClient.class);

	public TestPubSubClient(String path) throws MalformedURLException, ConfigurationException {
		final MsgBusClient msgBusClient = new MsgBusClient(path);
		client = msgBusClient.getPubSubClient();
	}

	
	public static void main(String[] args) throws Exception {
		
		PropertyConfigurator.configure("F:/Java Projects/log4j.properties");
		String path = "F:/Java Projects3/hedwig/hw_client.conf";
		TestPubSubClient test= new TestPubSubClient(path);
		test.testAsyncPublish();
	}

	// SynchronousQueues to verify async calls
	private final SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();
	private final SynchronousQueue<Boolean> consumeQueue = new SynchronousQueue<Boolean>();	

	// Test implementation of Callback for async client actions.
	class TestCallback implements Callback<Void> {

		@Override
		public void operationFinished(Object ctx, Void resultOfOperation) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					if (logger.isDebugEnabled())
						logger.debug("Operation finished!");
					ConcurrencyUtils.put(queue, true);
				}
			}).start();
		}

		@Override
		public void operationFailed(Object ctx, final PubSubException exception) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					logger.error("Operation failed!", exception);
					ConcurrencyUtils.put(queue, false);
				}
			}).start();
		}
	}

	// Test implementation of subscriber's message handler.
	class TestMessageHandler implements MessageHandler {

		private final SynchronousQueue<Boolean> consumeQueue;

		public TestMessageHandler() {
			this.consumeQueue = TestPubSubClient.this.consumeQueue;
		}

		public TestMessageHandler(SynchronousQueue<Boolean> consumeQueue) {
			this.consumeQueue = consumeQueue;
		}

		public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
				Object context) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					if (logger.isDebugEnabled())
						logger.debug("Consume operation finished successfully!");
					ConcurrencyUtils.put(TestMessageHandler.this.consumeQueue, true);
				}
			}).start();
			callback.operationFinished(context, null);
		}
	}
	
	public void testSyncPublish() throws Exception {
		boolean publishSuccess = true;
		try {
			client.publish("mySyncTopic", "Hello Sync World!");
		} catch (Exception e) {
			publishSuccess = false;
		}
		assertTrue(publishSuccess);
	}

	public void testSyncPublishWithResponse() throws Exception {
		String topic = "testSyncPublishWithResponse";
		String subid = "mysubid";

		final String prefix = "SyncMessage-";
		final int numMessages = 30;

		final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);
		final Map<String, MessageSeqId> receivedMsgs = new HashMap<String, MessageSeqId>();
		
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(100).build();

		client.subscribe(topic, subid, options);
		client.startDelivery(topic, subid, new MessageHandler() {
			synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				String str = msg.getBody().toStringUtf8();
				receivedMsgs.put(str, msg.getMsgId());
				if (numMessages == numReceived.incrementAndGet()) {
					receiveLatch.countDown();
				}
				callback.operationFinished(context, null);
			}
		});

		for (int i = 0; i < numMessages; i++) {
			String msg = prefix + i;
			PublishResponse response = client.publish(topic, msg);
			assertNotNull(response);
			publishedMsgs.put(msg, response.getPublishedMsgId());
		}

		assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(30, TimeUnit.SECONDS));
		assertEquals("Should be expected " + numMessages + " messages.", numMessages, numReceived.get());
		assertEquals("Should be expected " + numMessages + " messages in map.", numMessages, receivedMsgs.size());

		for (int i = 0; i < numMessages; i++) {
			final String str = prefix + i;
			MessageSeqId pubId = publishedMsgs.get(str);
			MessageSeqId revId = receivedMsgs.get(str);
			assertTrue("Doesn't receive same message seq id for " + str, pubId.equals(revId));
		}
	}

	public void testAsyncPublish() throws Exception {
		client.asyncPublish("myAsyncTopic", "Hello Async World!", new TestCallback(), null);
		assertTrue(queue.take());
	}

	public void testAsyncPublishWithResponse() throws Exception {
		String topic = "testAsyncPublishWithResponse";
		String subid = "mysubid";

		final String prefix = "AsyncMessage-";
		final int numMessages = 30;

		final AtomicInteger numPublished = new AtomicInteger(0);
		final CountDownLatch publishLatch = new CountDownLatch(1);
		final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);
		final Map<String, MessageSeqId> receivedMsgs = new HashMap<String, MessageSeqId>();

		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(100).build();

		client.subscribe(topic, subid, options);
		client.startDelivery(topic, subid, new MessageHandler() {
			synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				String str = msg.getBody().toStringUtf8();
				receivedMsgs.put(str, msg.getMsgId());
				if (numMessages == numReceived.incrementAndGet()) {
					receiveLatch.countDown();
				}
				callback.operationFinished(context, null);
			}
		});

		for (int i = 0; i < numMessages; i++) {
			final String msg = prefix + i;
			client.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
				@Override
				public void operationFinished(Object ctx, PublishResponse response) {
					publishedMsgs.put(msg, response.getPublishedMsgId());
					if (numMessages == numPublished.incrementAndGet()) {
						publishLatch.countDown();
					}
				}

				@Override
				public void operationFailed(Object ctx, final PubSubException exception) {
					publishLatch.countDown();
				}
			}, null);
		}
		assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
		assertEquals("Should be expected " + numMessages + " publishes.", numMessages, numPublished.get());
		assertEquals("Should be expected " + numMessages + " publishe responses.", numMessages, publishedMsgs.size());

		assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(30, TimeUnit.SECONDS));
		assertEquals("Should be expected " + numMessages + " messages.", numMessages, numReceived.get());
		assertEquals("Should be expected " + numMessages + " messages in map.", numMessages, receivedMsgs.size());

		for (int i = 0; i < numMessages; i++) {
			final String str = prefix + i;
			MessageSeqId pubId = publishedMsgs.get(str);
			MessageSeqId revId = receivedMsgs.get(str);
			assertTrue("Doesn't receive same message seq id for " + str, pubId.equals(revId));
		}
	}

	public void testMultipleAsyncPublish() throws Exception {
		String topic1 = "myFirstTopic";
		String topic2 = "myNewTopic";

		client.asyncPublish(topic1, "Hello World!", new TestCallback(), null);
		assertTrue(queue.take());
		client.asyncPublish(topic2, "Hello on new topic!", new TestCallback(), null);
		assertTrue(queue.take());
		client.asyncPublish(topic1, "Hello Again on old topic!", new TestCallback(), null);
		assertTrue(queue.take());
	}

	public void testSyncSubscribe() throws Exception {
		boolean subscribeSuccess = true;
		try {
			SubscriptionOptions options = SubscriptionOptions.newBuilder()
					.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
					.setMessageWindowSize(100).build();			
			client.subscribe("mySyncSubscribeTopic", "1", options);
		} catch (Exception e) {
			subscribeSuccess = false;
		}
		assertTrue(subscribeSuccess);
	}

	public void testAsyncSubscribe() throws Exception {
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(100).build();		
		client.asyncSubscribe("myAsyncSubscribeTopic", "1", options, new TestCallback(), null);
		assertTrue(queue.take());
	}

	public void testStartDeliveryAfterCloseSub() throws Exception {
		String topic = "test";
		String subid = "mysubid";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(100).build();

		client.subscribe(topic, subid, options);

		// Start delivery for the subscriber
		client.startDelivery(topic, subid, new TestMessageHandler());

		// Now publish some messages for the topic to be consumed by the
		// subscriber.
		client.publish(topic, "Message #1");
		assertTrue(consumeQueue.take());

		// Close subscriber for the subscriber
		client.closeSubscription(topic, subid);

		// subscribe again
		client.subscribe(topic, subid, options);
		client.startDelivery(topic, subid, new TestMessageHandler());

		client.publish(topic, "Message #2");
		assertTrue(consumeQueue.take());
	}

	public void testSubscribeAndConsume() throws Exception {
		String topic = "myConsumeTopic";
		String subscriberId = "1";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(100).build();
		
		client.asyncSubscribe(topic, subscriberId, options, new TestCallback(), null);
		assertTrue(queue.take());

		// Start delivery for the subscriber
		client.startDelivery(topic, subscriberId, new TestMessageHandler());

		// Now publish some messages for the topic to be consumed by the
		// subscriber.
		client.asyncPublish(topic, "Message #1", new TestCallback(), null);
		assertTrue(queue.take());
		assertTrue(consumeQueue.take());
		client.asyncPublish(topic, "Message #2", new TestCallback(), null);
		assertTrue(queue.take());
		assertTrue(consumeQueue.take());
		client.asyncPublish(topic, "Message #3", new TestCallback(), null);
		assertTrue(queue.take());
		assertTrue(consumeQueue.take());
		client.asyncPublish(topic, "Message #4", new TestCallback(), null);
		assertTrue(queue.take());
		assertTrue(consumeQueue.take());
		client.asyncPublish(topic, "Message #5", new TestCallback(), null);
		assertTrue(queue.take());
		assertTrue(consumeQueue.take());
	}

	public void testAsyncSubscribeAndUnsubscribe() throws Exception {
		String topic = "myAsyncUnsubTopic";
		String subscriberId = "1";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(100).build();		
		client.asyncSubscribe(topic, subscriberId, options, new TestCallback(), null);
		assertTrue(queue.take());
		client.asyncUnsubscribe(topic, subscriberId, new TestCallback(), null);
		assertTrue(queue.take());
	}

	public void testSyncUnsubscribeWithoutSubscription() throws Exception {
		boolean unsubscribeSuccess = false;
		try {
			client.unsubscribe("mySyncUnsubTopic", "1");
		} catch (ClientNotSubscribedException e) {
			unsubscribeSuccess = true;
		} catch (Exception ex) {
			unsubscribeSuccess = false;
		}
		assertTrue(unsubscribeSuccess);
	}

	public void testAsyncSubscribeAndCloseSubscription() throws Exception {
		String topic = "myAsyncSubAndCloseSubTopic";
		String subscriberId = "1";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(100).build();		
		client.asyncSubscribe(topic, subscriberId, options, new TestCallback(), null);
		assertTrue(queue.take());
		client.closeSubscription(topic, subscriberId);
		assertTrue(true);
	}
}