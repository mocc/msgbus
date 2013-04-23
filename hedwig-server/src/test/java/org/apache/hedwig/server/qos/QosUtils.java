package org.apache.hedwig.server.qos;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class QosUtils {

	/**
	 * @param args
	 */
	static AtomicInteger numReceived = new AtomicInteger(0);
	static CountDownLatch receiveLatch = new CountDownLatch(1);
	protected static final Logger logger = LoggerFactory.getLogger(QosUtils.class);
	static Message message = null;
	static int count = 0;

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
		final MsgBusClient client = new MsgBusClient(this.getClass()
				.getResource("/hw_client.conf"));
		final MessageQueueClient mqClient = client.getMessageQueueClient();
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
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
					try {
						mqClient.stopDelivery(queueName);
					} catch (ClientNotSubscribedException e) {
					}
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
			// mqClient.closeSubscription(queueName);
			System.out.println(".........one client quit..............."
					+ Thread.currentThread().getName());
		}

		long end = System.currentTimeMillis();
		System.out.println(Thread.currentThread().getName()
				+ "..........receiving finished.");
		System.out.println("numReceived:" + numReceived.get());
		System.out.println(Thread.currentThread().getName()
				+ "::Time cost for receiving is " + (end - start) + " ms.");
	}

	public void recv1(String[] args) throws Exception {
		System.out.println("enter.........................rec");
		// java.security.Security.setProperty("networkaddress.cache.ttl", "0");
		final String queueName = args[0];
		final int numMessages = Integer.parseInt(args[1]);

		final MsgBusClient client = new MsgBusClient(this.getClass()
				.getResource("/hw_client.conf"));
		final MessageQueueClient mqClient = client.getMessageQueueClient();
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
		System.out.println("numMessages:::::" + numMessages);
		mqClient.createQueue(queueName);
		long start = System.currentTimeMillis();

		mqClient.startDelivery(queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				count++;
				logger.info("........................." + count);
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

				if (msg.getMsgId().getLocalComponent() != 10) {
					try {
						mqClient.consumeMessage(queueName, msg.getMsgId());
						logger.info("::client has call consume.."
								+ msg.getMsgId().getLocalComponent() + "::"
								+ msg.getBody().toStringUtf8());
					} catch (ClientNotSubscribedException e) {
						e.printStackTrace();
					}
				} else {
					logger.info(msg.getBody().toStringUtf8()
							+ ".........message not consumed");
					message = msg;

				}
				if (msg.getMsgId().getLocalComponent() == 50) {

					try {
						mqClient.consumeMessage(queueName, message.getMsgId());
						logger.info("::client has call consume message10.."
								+ message.getMsgId().getLocalComponent() + "::"
								+ message.getBody().toStringUtf8());
					} catch (ClientNotSubscribedException e) {
						e.printStackTrace();
					}
				}
			}
		}, options);

		assertTrue("Timed out waiting on callback for messages.",
				receiveLatch.await(20, TimeUnit.SECONDS));
		mqClient.stopDelivery(queueName);
		mqClient.closeSubscription(queueName);

		long end = System.currentTimeMillis();
		System.out.println(Thread.currentThread().getName()
				+ "..........receiving finished.");
		System.out.println("numReceived:" + numReceived.get());
		System.out.println(Thread.currentThread().getName()
				+ "::Time cost for receiving is " + (end - start) + " ms.");
	}

	public void recv2(String[] args) throws Exception {
		System.out.println("enter.........................rec");
		// java.security.Security.setProperty("networkaddress.cache.ttl", "0");
		final String queueName = args[0];
		final int numMessages = Integer.parseInt(args[1]);

		final MsgBusClient client = new MsgBusClient(this.getClass()
				.getResource("/hw_client.conf"));
		final MessageQueueClient mqClient = client.getMessageQueueClient();
		final SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
		System.out.println("numMessages:::::" + numMessages);
		mqClient.createQueue(queueName);
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
					mqClient.consumeMessage(queueName, msg.getMsgId());
					logger.info("::client has call consume.."
							+ msg.getMsgId().getLocalComponent() + "::"
							+ msg.getBody().toStringUtf8());
				} catch (ClientNotSubscribedException e) {
					e.printStackTrace();
				}

			}
		}, options);
		// ///////////////////////////////////////////////////////////////

		TimeUnit.MILLISECONDS.sleep(10);
		logger.info("simulate client disconnect................");
		client.close();
		// mqClient.closeSubscription(queueName);
		TimeUnit.MILLISECONDS.sleep(10);
		logger.info("simulate client try to reconnect................");
		final MsgBusClient client1 = new MsgBusClient(this.getClass()
				.getResource("/hw_client.conf"));
		final MessageQueueClient mqClient1 = client1.getMessageQueueClient();
		Thread t1 = new Thread() {

			@Override
			public void run() {
				try {
					// TODO Auto-generated method stub

					mqClient1.createQueue(queueName);
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
								mqClient1.consumeMessage(queueName,
										msg.getMsgId());
								logger.info("::client has call consume.."
										+ msg.getMsgId().getLocalComponent()
										+ "::" + msg.getBody().toStringUtf8());
							} catch (ClientNotSubscribedException e) {
								e.printStackTrace();
							}

						}
					}, options);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		};
		t1.start();
		// /////////////////////////////////////////////////////////////////
		assertTrue("Timed out waiting on callback for messages.",
				receiveLatch.await(10, TimeUnit.SECONDS));
		// mqClient.stopDelivery(queueName);
		mqClient1.closeSubscription(queueName);

		long end = System.currentTimeMillis();
		System.out.println(Thread.currentThread().getName()
				+ "..........receiving finished.");
		System.out.println("numReceived:" + numReceived.get());
		System.out.println(Thread.currentThread().getName()
				+ "::Time cost for receiving is " + (end - start) + " ms.");
	}

}
