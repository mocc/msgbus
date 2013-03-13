package com.yonyou.msgbus.client;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class MessageQueueClient {

	public static final String QUEUE_PREFIX = "q_";

	public static class QueueNotCreatedException extends RuntimeException {
		private static final long serialVersionUID = -1L;

		public QueueNotCreatedException() {
			super("The queue has not been created yet!");
		}
	}

	static class IllegalQueueStateException extends RuntimeException {
		private static final long serialVersionUID = -1L;

		public IllegalQueueStateException() {
			super("The queue's state is invalid.");
		}

		public IllegalQueueStateException(String reason) {
			super(reason);
		}
	}

	private static final ByteString SUBSCRIBER_ID = ByteString.copyFromUtf8("0");
	private static final ByteString CREATE_ID = ByteString.copyFromUtf8("1");

	static final Logger LOG = LoggerFactory.getLogger(MessageQueueClient.class);	

	private Publisher publisher;
	private Subscriber subscriber;
	private boolean isClosed;

	private HashMap<String, HedwigMessageQueue> queues = new HashMap<String, HedwigMessageQueue>();

	// Invisible to users
	MessageQueueClient(Publisher publisher, Subscriber subscriber) {
		this.publisher = publisher;
		this.subscriber = subscriber;
	}
	

	/**
	 * @throws InvalidSubscriberIdException
	 * @throws ServiceDownException
	 * @throws CouldNotConnectException
	 * 
	 */
	public synchronized boolean createQueue(String queueName, boolean autoAck, SubscriptionOptions options) {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;
		if (isClosed) {
			throw new IllegalStateException("Client has already been closed");
		}
		HedwigMessageQueue queue = null;
		if (queues.containsKey(myQueueName)) {
			System.out.println("Queue " + myQueueName + " already existed.");
			return false;
		} else {
			try {
				queue = new HedwigMessageQueue(myQueueName, autoAck, options);
			} catch (CouldNotConnectException e) {
				e.printStackTrace();
				return false;
			} catch (ServiceDownException e) {
				e.printStackTrace();
				return false;
			} catch (InvalidSubscriberIdException e) {
				e.printStackTrace();
				return false;
			}
			queues.put(myQueueName, queue);
			return true;
		}
	}

	// ok?
	public boolean deleteQueue(String queueName) {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;
		try {
			subscriber.unsubscribe(ByteString.copyFromUtf8(myQueueName), SUBSCRIBER_ID);
		} catch (CouldNotConnectException e) {
			e.printStackTrace();
			return false;
		} catch (ClientNotSubscribedException e) {
			e.printStackTrace();
			return false;
		} catch (ServiceDownException e) {
			e.printStackTrace();
			return false;
		} catch (InvalidSubscriberIdException e) {
			e.printStackTrace();
			return false;
		}
		return true; // success?

	}	

	public PublishResponse publish(String queueName, String msg) throws CouldNotConnectException, ServiceDownException {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;
		if (isClosed) {
			throw new IllegalStateException("Client has already been closed");
		}
		ByteString topic = ByteString.copyFromUtf8(myQueueName);
		Message message = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
		return publisher.publish(topic, message);
	}

	public void asyncPublish(String queueName, String msg, Callback<Void> callback, Object context) {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;
		if (isClosed) {
			throw new IllegalStateException("Client has already been closed");
		}
		ByteString topic = ByteString.copyFromUtf8(myQueueName);
		Message message = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
		publisher.asyncPublish(topic, message, callback, context);
	}

	public void asyncPublishWithResponse(String queueName, String msg, Callback<PublishResponse> callback,
			Object context) {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;
		if (isClosed) {
			throw new IllegalStateException("Client has already been closed");
		}
		ByteString topic = ByteString.copyFromUtf8(myQueueName);
		Message message = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
		publisher.asyncPublishWithResponse(topic, message, callback, context);
	}

	public void consumeMessage(String queueName, MessageSeqId msgSeqId) {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;
		if (isClosed) {
			throw new IllegalStateException("Client has already been closed");
		}
		if (!queues.containsKey(myQueueName)) {
			throw new QueueNotCreatedException();
		}
		try {
			subscriber.consume(ByteString.copyFromUtf8(myQueueName), SUBSCRIBER_ID, msgSeqId);
		} catch (ClientNotSubscribedException e) {
			// This is not gonna happen.
		}
	}

	private HedwigMessageQueue checkAndGet(String queueName) {

		if (isClosed) {
			throw new IllegalStateException("Client has already been closed");
		}
		if (!queues.containsKey(queueName)) {
			throw new QueueNotCreatedException();
		}
		return queues.get(queueName);
	}

	public Message receive(String queueName, long timeout) throws ClientNotSubscribedException,
			AlreadyStartDeliveryException {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;

		HedwigMessageQueue queue = checkAndGet(myQueueName);

		Message message = null;
		message = queue.receive(timeout, TimeUnit.MILLISECONDS);
		return message == null ? null : message;
	}

	/**
	 * (1) put subscribe and startDelivery together; (2) auto-ack is performed, it's ok?
	 */
	public boolean startDelivery(final String queueName, final MessageHandler handler)
			throws ClientNotSubscribedException {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;
		HedwigMessageQueue queue = checkAndGet(myQueueName);
		return queue.startDelivery(handler);
	}

	public void stopDelivery(final String queueName) throws ClientNotSubscribedException {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;
		HedwigMessageQueue queue = checkAndGet(myQueueName);

		queue.stopDelivery();
	}

	public void closeSubscription(String queueName) throws ServiceDownException {
		String myQueueName = MessageQueueClient.QUEUE_PREFIX + queueName;
		subscriber.closeSubscription(ByteString.copyFromUtf8(myQueueName), SUBSCRIBER_ID);
	}

	public synchronized void close() {
		if (isClosed) {
			return;
		}
		Set<String> queueNames = queues.keySet();
		HedwigMessageQueue queue;
		for (String name : queueNames) {
			queue = queues.get(name);
			queue.close();
		}
		isClosed = true;
	}

	class HedwigMessageQueue {

		private ByteString topic;
		private boolean autoAck;
		private boolean hasSubed;  //If start, stop and start delivery, don't send sub request
		private boolean delivering;		
		private boolean isClosed;
		private SubscriptionOptions options;
		private final SynchronousQueue<Message> consumeQueue = new SynchronousQueue<Message>();
		private final ExecutorService executor = Executors.newCachedThreadPool();

		class SyncMessageHandler implements MessageHandler {

			private final SynchronousQueue<Message> consumeQueue;

			public SyncMessageHandler() {
				this.consumeQueue = HedwigMessageQueue.this.consumeQueue;
			}

			public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
					Object context) {
				final Message message = Message.newBuilder(msg).build();
				// Using executor service may buy you some time and resources.
				executor.execute(new Runnable() {
					@Override
					public void run() {
						if (LOG.isDebugEnabled())
							LOG.debug("Consume operation finished successfully!");
						ConcurrencyUtils.put(consumeQueue, message);
					}
				});
				// The following operation is necessary
				callback.operationFinished(context, null);
			}
		}

		private HedwigMessageQueue(String queue, boolean autoAck, SubscriptionOptions options) throws CouldNotConnectException, ServiceDownException, InvalidSubscriberIdException {
			topic = ByteString.copyFromUtf8(queue);
			delivering = false;
			hasSubed = false;
			this.autoAck = autoAck;
			this.options=options;
			initialize();
		}

		private HedwigMessageQueue initialize() throws CouldNotConnectException, ServiceDownException,
				InvalidSubscriberIdException {
			try {				
				subscriber.subscribe(topic, CREATE_ID, options);				
			} catch (ClientAlreadySubscribedException e) {
				LOG.info("Client already subscribed.", e);
			}
			return this;
		}

		void stopDelivery() throws ClientNotSubscribedException {
			subscriber.stopDelivery(topic, SUBSCRIBER_ID);			
			delivering = false;
		}

		synchronized boolean startDelivery(final MessageHandler handler) throws ClientNotSubscribedException {
			if (isClosed) {
				throw new IllegalStateException("The queue has already been closed.");
			}
			if (!hasSubed) {
				try {
					subscriber.subscribe(topic, SUBSCRIBER_ID, options);
					hasSubed=true;
				} catch (ClientAlreadySubscribedException e) {
					// It's ok, so move on
					LOG.info("Client already subscribed.", e);
				} catch (Exception e) {
					return false;
				}
			}

			try {
				subscriber.startDelivery(topic, SUBSCRIBER_ID, new MessageHandler() {
					@Override
					public void deliver(ByteString topic, ByteString subscriberId, Message msg,
							Callback<Void> callback, Object context) {
						handler.deliver(topic, subscriberId, msg, callback, context);
						try {
							// The following operation is necessary
							callback.operationFinished(context, null);
							if (autoAck) {
								subscriber.consume(topic, subscriberId, msg.getMsgId());
							}
						} catch (ClientNotSubscribedException e) {
							// no-op now
						}
					}
				});
				delivering=true;
			} catch (AlreadyStartDeliveryException e) {
				// This is not gonna happen.
			}
			return true;
		}

		synchronized Message receive(long timeout, TimeUnit unit) throws ClientNotSubscribedException,
				AlreadyStartDeliveryException {
			if (isClosed) {
				throw new IllegalStateException("The queue has already been closed.");
			}
			if (!delivering) {
				if (!hasSubed) {
					try {						
						subscriber.subscribe(topic, SUBSCRIBER_ID, options);
						hasSubed = true;
					} catch (ClientAlreadySubscribedException e) {
						LOG.info("Client already subscribed.", e); // It's ok, don't throw.
					} catch (Exception e) {
						LOG.info("Client already subscribed.", e);
						return null;
					}
				}

				subscriber.startDelivery(topic, SUBSCRIBER_ID, new SyncMessageHandler());
				delivering = true;
			}

			Message msg = ConcurrencyUtils.poll(consumeQueue, timeout, unit);
			if (msg == null) {
				LOG.info("Return without message");
				return null;
			}

			if (autoAck) {
				subscriber.consume(topic, SUBSCRIBER_ID, msg.getMsgId());
			}
			return msg;
		}

		void close() {			
			isClosed = true;
		}

	}

}
