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

import static org.apache.hedwig.util.VarArgs.va;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ServerNotResponsibleForTopicException;
import org.apache.hedwig.filter.ServerMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.ProtocolVersion;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protoextensions.PubSubResponseUtils;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.UnexpectedError;
import org.apache.hedwig.server.delivery.FIFODeliveryManager.UnConsumedSeq;
import org.apache.hedwig.server.netty.ServerStats;
import org.apache.hedwig.server.persistence.Factory;
import org.apache.hedwig.server.persistence.MapMethods;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManagerWithRangeScan;
import org.apache.hedwig.server.persistence.ReadAheadCache;
import org.apache.hedwig.server.persistence.ScanCallback;
import org.apache.hedwig.server.persistence.ScanRequest;
import org.apache.hedwig.server.subscriptions.AbstractSubscriptionManager;
import org.apache.hedwig.util.Callback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class FIFODeliveryManager implements Runnable, DeliveryManager {

	protected static final Logger logger = LoggerFactory.getLogger(FIFODeliveryManager.class);

	private static Callback<Void> NOP_CALLBACK = new Callback<Void>() {
		@Override
		public void operationFinished(Object ctx, Void result) {
		}

		@Override
		public void operationFailed(Object ctx, PubSubException exception) {
		}
	};

	protected interface DeliveryManagerRequest {
		public void performRequest();
	}

	/**
	 * the main queue that the single-threaded delivery manager works off of
	 */
	BlockingQueue<DeliveryManagerRequest> requestQueue = new LinkedBlockingQueue<DeliveryManagerRequest>();

	/**
	 * The queue of all subscriptions that are facing a transient error either in scanning from the persistence manager,
	 * or in sending to the consumer
	 */
	Queue<ActiveSubscriberState> retryQueue = new PriorityBlockingQueue<ActiveSubscriberState>(32,
			new Comparator<ActiveSubscriberState>() {
				@Override
				public int compare(ActiveSubscriberState as1, ActiveSubscriberState as2) {
					long s = as1.lastScanErrorTime - as2.lastScanErrorTime;
					return s > 0 ? 1 : (s < 0 ? -1 : 0);
				}
			});

	/**
	 * Stores a mapping from topic to the delivery pointers on the topic. The delivery pointers are stored in a sorted
	 * map from seq-id to the set of subscribers at that seq-id
	 */
	Map<ByteString, SortedMap<Long, Set<ActiveSubscriberState>>> perTopicDeliveryPtrs;

	/**
	 * Mapping from delivery end point to the subscriber state that we are serving at that end point. This prevents us
	 * e.g., from serving two subscriptions to the same endpoint
	 */
	Map<TopicSubscriber, ActiveSubscriberState> subscriberStates;

	private PersistenceManager persistenceMgr;

	private ServerConfiguration cfg;

	// Boolean indicating if this thread should continue running. This is used
	// when we want to stop the thread during a PubSubServer shutdown.
	protected boolean keepRunning = true;
	private final Thread workerThread;

	/* msgbus team */
	ZooKeeper zk;

	/* msgbus team */

	// ?
	public FIFODeliveryManager(PersistenceManager persistenceMgr, ServerConfiguration cfg) {
		this.persistenceMgr = persistenceMgr;
		perTopicDeliveryPtrs = new HashMap<ByteString, SortedMap<Long, Set<ActiveSubscriberState>>>();
		subscriberStates = new HashMap<TopicSubscriber, ActiveSubscriberState>();
		workerThread = new Thread(this, "DeliveryManagerThread");
		this.cfg = cfg;
	}

	/* msgbus team */
	public FIFODeliveryManager(PersistenceManager persistenceMgr, ZooKeeper zk, ServerConfiguration cfg,
			ScheduledExecutorService scheduler) {
		this.persistenceMgr = persistenceMgr;
		perTopicDeliveryPtrs = new HashMap<ByteString, SortedMap<Long, Set<ActiveSubscriberState>>>();
		subscriberStates = new HashMap<TopicSubscriber, ActiveSubscriberState>();
		workerThread = new Thread(this, "DeliveryManagerThread");
		this.cfg = cfg;
		this.zk = zk;
	}

	/* msgbus team */

	@Override
	public void start() {
		workerThread.start();
	}

	/**
	 * ===================================================================== Our usual enqueue function, stop if error
	 * because of unbounded queue, should never happen
	 * 
	 */
	protected void enqueueWithoutFailure(DeliveryManagerRequest request) {
		if (!requestQueue.offer(request)) {
			throw new UnexpectedError("Could not enqueue object: " + request + " to delivery manager request queue.");
		}
	}

	/**
	 * ==================================================================== Public interface of the delivery manager
	 */

	/**
	 * Tells the delivery manager to start sending out messages for a particular subscription
	 * 
	 * @param topic
	 * @param subscriberId
	 * @param seqIdToStartFrom
	 *            Message sequence-id from where delivery should be started
	 * @param endPoint
	 *            The delivery end point to which send messages to
	 * @param filter
	 *            Only messages passing this filter should be sent to this subscriber
	 */
	@Override
	public void startServingSubscription(ByteString topic, ByteString subscriberId,
			SubscriptionPreferences preferences, MessageSeqId seqIdToStartFrom, DeliveryEndPoint endPoint,
			ServerMessageFilter filter) {

		ActiveSubscriberState subscriber = new ActiveSubscriberState(topic, subscriberId, preferences,
				seqIdToStartFrom.getLocalComponent() - 1, endPoint, filter);

		enqueueWithoutFailure(subscriber);
	}

	@Override
	public void stopServingSubscriber(ByteString topic, ByteString subscriberId, SubscriptionEvent event,
			Callback<Void> cb, Object ctx) {
		ActiveSubscriberState subState = subscriberStates.get(new TopicSubscriber(topic, subscriberId));

		if (subState != null) {
			stopServingSubscriber(subState, event, cb, ctx);
		} else {
			cb.operationFinished(ctx, null);
		}
	}

	/**
	 * Due to some error or disconnection or unsusbcribe, someone asks us to stop serving a particular endpoint
	 * 
	 * @param subscriber
	 *            Subscriber instance
	 * @param event
	 *            Subscription event indicates why to stop subscriber.
	 * @param cb
	 *            Callback after the subscriber is stopped.
	 * @param ctx
	 *            Callback context
	 */
	protected void stopServingSubscriber(ActiveSubscriberState subscriber, SubscriptionEvent event, Callback<Void> cb,
			Object ctx) {
		enqueueWithoutFailure(new StopServingSubscriber(subscriber, event, cb, ctx));
	}

	/**
	 * Instructs the delivery manager to backoff on the given subscriber and retry sending after some time
	 * 
	 * @param subscriber
	 */

	public void retryErroredSubscriberAfterDelay(ActiveSubscriberState subscriber) {

		subscriber.setLastScanErrorTime(MathUtils.now());

		if (!retryQueue.offer(subscriber)) {
			throw new UnexpectedError("Could not enqueue to delivery manager retry queue");
		}
	}

	public void clearRetryDelayForSubscriber(ActiveSubscriberState subscriber) {
		subscriber.clearLastScanErrorTime();
		if (!retryQueue.offer(subscriber)) {
			throw new UnexpectedError("Could not enqueue to delivery manager retry queue");
		}
		// no request in request queue now
		// issue a empty delivery request to not waiting for polling queue
		// msgbus team: so the retryQueue can be polled consequently
		if (requestQueue.isEmpty()) {
			enqueueWithoutFailure(new DeliveryManagerRequest() {
				@Override
				public void performRequest() {
					// do nothing
				}
			});
		}
	}

	@Override
	public void messageConsumed(ByteString topic, ByteString subscriberId, MessageSeqId consumedSeqId) {
		ActiveSubscriberState subState = subscriberStates.get(new TopicSubscriber(topic, subscriberId));
		if (null == subState) {
			return;
		}
		subState.messageConsumed(consumedSeqId.getLocalComponent());
	}

	/**
	 * Instructs the delivery manager to move the delivery pointer for a given subscriber
	 * 
	 * @param subscriber
	 * @param prevSeqId
	 * @param newSeqId
	 */
	public void moveDeliveryPtrForward(ActiveSubscriberState subscriber, long prevSeqId, long newSeqId) {
		enqueueWithoutFailure(new DeliveryPtrMove(subscriber, prevSeqId, newSeqId));
	}

	/*
	 * ========================================================================== == End of public interface, internal
	 * machinery begins.
	 */
	@Override
	public void run() {
		while (keepRunning) {
			DeliveryManagerRequest request = null;

			try {
				// We use a timeout of 1 second, so that we can wake up once in
				// a while to check if there is something in the retry queue.
				request = requestQueue.poll(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}

			// First retry any subscriptions that had failed and need a retry
			retryErroredSubscribers();

			if (request == null) {
				// logger.info("No request.");
				continue;
			}
			// logger.info(request.getClass().toString());
			request.performRequest();
		}
	}

	/**
	 * Stop method which will enqueue a ShutdownDeliveryManagerRequest.
	 */
	@Override
	public void stop() {
		enqueueWithoutFailure(new ShutdownDeliveryManagerRequest());
	}

	protected void retryErroredSubscribers() {
		long lastInterestingFailureTime = MathUtils.now() - cfg.getScanBackoffPeriodMs();
		ActiveSubscriberState subscriber;

		while ((subscriber = retryQueue.peek()) != null) {
			if (subscriber.getLastScanErrorTime() > lastInterestingFailureTime) {
				// Not enough time has elapsed yet, will retry later
				// Since the queue is fifo, no need to check later items
				return;
			}

			// retry now
			subscriber.deliverNextMessage();
			retryQueue.poll();
		}
	}

	protected void removeDeliveryPtr(ActiveSubscriberState subscriber, Long seqId, boolean isAbsenceOk,
			boolean pruneTopic) {

		assert seqId != null;

		// remove this subscriber from the delivery pointers data structure
		ByteString topic = subscriber.getTopic();
		SortedMap<Long, Set<ActiveSubscriberState>> deliveryPtrs = perTopicDeliveryPtrs.get(topic);

		if (deliveryPtrs == null && !isAbsenceOk) {
			throw new UnexpectedError("No delivery pointers found while disconnecting " + "channel for topic:" + topic);
		}

		if (null == deliveryPtrs) {
			return;
		}

		if (!MapMethods.removeFromMultiMap(deliveryPtrs, seqId, subscriber) && !isAbsenceOk) {

			throw new UnexpectedError("Could not find subscriber:" + subscriber + " at the expected delivery pointer");
		}

		if (pruneTopic && deliveryPtrs.isEmpty()) {
			perTopicDeliveryPtrs.remove(topic);
		}

	}

	protected long getMinimumSeqId(ByteString topic) {
		SortedMap<Long, Set<ActiveSubscriberState>> deliveryPtrs = perTopicDeliveryPtrs.get(topic);

		if (deliveryPtrs == null || deliveryPtrs.isEmpty()) {
			return Long.MAX_VALUE - 1;
		}
		return deliveryPtrs.firstKey();
	}

	protected void addDeliveryPtr(ActiveSubscriberState subscriber, Long seqId) {

		// If this topic doesn't exist in the per-topic delivery pointers table,
		// create an entry for it
		SortedMap<Long, Set<ActiveSubscriberState>> deliveryPtrs = MapMethods.getAfterInsertingIfAbsent(
				perTopicDeliveryPtrs, subscriber.getTopic(), TreeMapLongToSetSubscriberFactory.instance);

		MapMethods.addToMultiMap(deliveryPtrs, seqId, subscriber, HashMapSubscriberFactory.instance);
	}

	public class ActiveSubscriberState implements ScanCallback, DeliveryCallback, DeliveryManagerRequest {

		static final int UNLIMITED = 0;
		/* msgbus team */
		ConsumerCluster consumerCluster;
		AtomicBoolean delivering = new AtomicBoolean(false);
		/* msgbus team */

		ByteString topic;
		ByteString subscriberId;
		long lastLocalSeqIdDelivered;
		boolean connected = true;
		DeliveryEndPoint deliveryEndPoint;
		long lastScanErrorTime = -1;
		long localSeqIdDeliveringNow;
		long lastSeqIdCommunicatedExternally;
		long lastSeqIdConsumedUtil;
		boolean isThrottled = false;
		final int messageWindowSize;
		ServerMessageFilter filter;

		final static int SEQ_ID_SLACK = 10;

		public ActiveSubscriberState(ByteString topic, ByteString subscriberId, SubscriptionPreferences preferences,
				long lastLocalSeqIdDelivered, DeliveryEndPoint deliveryEndPoint, ServerMessageFilter filter) {
			this.topic = topic;
			this.subscriberId = subscriberId;
			this.lastLocalSeqIdDelivered = lastLocalSeqIdDelivered;
			this.lastSeqIdConsumedUtil = lastLocalSeqIdDelivered;
			this.deliveryEndPoint = deliveryEndPoint;
			this.filter = filter;
			if (preferences.hasMessageWindowSize()) {
				messageWindowSize = preferences.getMessageWindowSize();
			} else {
				if (FIFODeliveryManager.this.cfg.getDefaultMessageWindowSize() > 0) {
					messageWindowSize = FIFODeliveryManager.this.cfg.getDefaultMessageWindowSize();
				} else {
					messageWindowSize = UNLIMITED;
				}
			}
		}

		public void setNotConnected(SubscriptionEvent event) {
			// have closed it.
			if (!isConnected()) {
				return;
			}
			this.connected = false;
			if (null != event
					&& (SubscriptionEvent.TOPIC_MOVED == event || SubscriptionEvent.SUBSCRIPTION_FORCED_CLOSED == event)) {
				// we should not close the channel now after enabling
				// multiplexing
				PubSubResponse response = PubSubResponseUtils.getResponseForSubscriptionEvent(topic, subscriberId,
						event);
				deliveryEndPoint.send(response, new DeliveryCallback() {
					@Override
					public void sendingFinished() {
						// do nothing now
					}

					@Override
					public void transientErrorOnSend() {
						// do nothing now
					}

					@Override
					public void permanentErrorOnSend(Object ctx) {
						// if channel is broken, close the channel
						deliveryEndPoint.close();
					}
				});
			}
			// uninitialize filter
			this.filter.uninitialize();
		}

		public ByteString getTopic() {
			return topic;
		}

		public long getLastLocalSeqIdDelivered() {
			return lastLocalSeqIdDelivered;
		}

		public long getLastScanErrorTime() {
			return lastScanErrorTime;
		}

		public void setLastScanErrorTime(long lastScanErrorTime) {
			this.lastScanErrorTime = lastScanErrorTime;
		}

		/**
		 * Clear the last scan error time so it could be retry immediately.
		 */
		protected void clearLastScanErrorTime() {
			this.lastScanErrorTime = -1;
		}

		protected boolean isConnected() {
			return connected;
		}

		protected void messageConsumed(long newSeqIdConsumed) {
			if (newSeqIdConsumed <= lastSeqIdConsumedUtil) {
				return;
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Subscriber ({}) moved consumed ptr from {} to {}.",
						va(this, lastSeqIdConsumedUtil, newSeqIdConsumed));
			}
			lastSeqIdConsumedUtil = newSeqIdConsumed;
			// after updated seq id check whether it still exceed msg limitation
			if (msgLimitExceeded()) {
				return;
			}
			if (isThrottled) {
				isThrottled = false;
				logger.info(
						"Try to wake up subscriber ({}) to deliver messages again : last delivered {}, last consumed {}.",
						va(this, lastLocalSeqIdDelivered, lastSeqIdConsumedUtil));

				enqueueWithoutFailure(new DeliveryManagerRequest() {
					@Override
					public void performRequest() {
						// enqueue
						clearRetryDelayForSubscriber(ActiveSubscriberState.this);
					}
				});
			}
		}

		protected boolean msgLimitExceeded() {
			if (messageWindowSize == UNLIMITED) {
				return false;
			}
			if (lastLocalSeqIdDelivered - lastSeqIdConsumedUtil >= messageWindowSize) {
				return true;
			}
			return false;
		}

		public void deliverNextMessage() {

			if (!isConnected()) {
				return;
			}			

			consumerCluster.checkTimeOut();
			
			if (!delivering.get()) {
				logger.info("Stop delivering next message. count= " + (++consumerCluster.stopCount));
				return;
			}

			localSeqIdDeliveringNow = persistenceMgr.getSeqIdAfterSkipping(topic, lastLocalSeqIdDelivered, 1);

			ScanRequest scanRequest = new ScanRequest(topic, localSeqIdDeliveringNow,
			/* callback= */this, /* ctx= */null);
			persistenceMgr.scanSingleMessage(scanRequest);
		}

		/**
		 * =============================================================== {@link ScanCallback} methods
		 */

		@Override
		public void messageScanned(Object ctx, final Message message) {
			if (!connected) {
				return;
			}

			if (!filter.testMessage(message)) {
				sendingFinished();
				return;
			}

			// Pick one member and send message to it
			// Not lock. If the consumer is just in the air, it's ok to put message in retryMessageQueue
			consumerCluster.qcDelLock.lock();
			QueueConsumer qc = consumerCluster.waitingConsumers.peek();

			// Suppose that happens rarely, so don't check delivering in advance
			if (qc == null) {
				// client is leave, or wrong
				//logger.info("Can't find waiting queueConsumer. Put the message in retryMessageQueue.");
				consumerCluster.retryMessageQueue.add(new UnConsumedSeq(message.getMsgId().getLocalComponent(), System
						.currentTimeMillis()));
				// note: release the lock if return here
				consumerCluster.qcDelLock.unlock();
				return;
			}

			PubSubResponse response = PubSubResponse.newBuilder().setProtocolVersion(ProtocolVersion.VERSION_ONE)
					.setStatusCode(StatusCode.SUCCESS).setTxnId(0).setMessage(message).build();
			// test
			/*
			 * if (ctx == null) { logger.info("Message to send: " + message.getMsgId().getLocalComponent()); } else {
			 * logger.info("Message to resend: " + message.getMsgId().getLocalComponent()); }
			 */

			DeliveryEndPoint queueEndPoint = ((ctx == null) ? qc.endPoint : qc.endPoint2);
			
			// First add then send, so removing in other thread can't insert between them
			qc.unConsumedMsgSeqs.add(new UnConsumedSeq(message.getMsgId().getLocalComponent(), System
					.currentTimeMillis()));
			queueEndPoint.send(response, qc);
			
			consumerCluster.qcDelLock.unlock();

			ConcurrentLinkedQueue<QueueConsumer> from;
			ConcurrentLinkedQueue<QueueConsumer> to;

			// qc.isWaiting is also locked
			consumerCluster.qcMoveLock.lock();
			// qc must be in waitingConsumers?
			from = qc.isWaiting.get() ? consumerCluster.waitingConsumers : consumerCluster.busyConsumers;
			if (qc.unConsumedMsgSeqs.size() >= qc.messageWindowSize) {
				//logger.info("Move consumer from waitingConsumers to busyConsumers.");
				qc.isWaiting.set(false);
				to = consumerCluster.busyConsumers;
			} else {
				to = consumerCluster.waitingConsumers;
			}

			from.remove(qc);
			to.add(qc);
			consumerCluster.qcMoveLock.unlock();
			if (consumerCluster.waitingConsumers.size() == 0) {
				delivering.set(false);
			}

		}

		@Override
		public void scanFailed(Object ctx, Exception exception) {
			if (!connected) {
				return;
			}
			// wait for some time and then retry
			retryErroredSubscriberAfterDelay(this);
		}

		@Override
		public void scanFinished(Object ctx, ReasonForFinish reason) {
			// no-op
		}

		/**
		 * =============================================================== {@link DeliveryCallback} methods
		 */
		@Override
		public void sendingFinished() {
			if (!connected) {
				return;
			}
			lastLocalSeqIdDelivered = localSeqIdDeliveringNow;

			if (lastLocalSeqIdDelivered > lastSeqIdCommunicatedExternally + SEQ_ID_SLACK) {
				// Note: The order of the next 2 statements is important. We should submit a request to change our
				// delivery pointer only *after* we have actually changed it. Otherwise, there is a race condition
				// with removal of this channel, w.r.t, maintaining the deliveryPtrs tree map.
				long prevId = lastSeqIdCommunicatedExternally;
				lastSeqIdCommunicatedExternally = lastLocalSeqIdDelivered;
				moveDeliveryPtrForward(this, prevId, lastLocalSeqIdDelivered);
			}
			// increment deliveried message
			ServerStats.getInstance().incrementMessagesDelivered();
			deliverNextMessage();
		}

		public long getLastSeqIdCommunicatedExternally() {
			return lastSeqIdCommunicatedExternally;
		}

		@Override
		public void permanentErrorOnSend(Object ctx) {
			// the underlying channel is broken, the channel will be closed in UmbrellaHandler when exception happened.
			// so we don't need to close the channel again
			stopServingSubscriber(this, null, NOP_CALLBACK, ctx);
		}

		@Override
		public void transientErrorOnSend() {
			retryErroredSubscriberAfterDelay(this);
		}

		/**
		 * =============================================================== {@link DeliveryManagerRequest} methods
		 */
		@Override
		public void performRequest() {
			/* msgbus team added */
			TopicSubscriber ts = new TopicSubscriber(topic, subscriberId);
			ActiveSubscriberState ass;
			// The first client, perform subscription
			// "ass.connected==false" is for "disconnected then connected" situation, where ass already exists but
			// no longer works, so we construct a new one
			if ((ass = subscriberStates.get(ts)) == null || ass.connected == false) {
				consumerCluster = new ConsumerCluster(this, lastLocalSeqIdDelivered);
				QueueConsumer qc = new QueueConsumer((ChannelEndPoint) this.deliveryEndPoint, consumerCluster, this,
						this.messageWindowSize);
				consumerCluster.addNewConsumer(qc);
				subscriberStates.put(ts, this);

				lastSeqIdCommunicatedExternally = lastLocalSeqIdDelivered;
				addDeliveryPtr(this, lastLocalSeqIdDelivered);				

				// This "compareAndSet" operation is necessary
				if (this.delivering.compareAndSet(false, true)) {
					this.deliverNextMessage();
				}
			} else {
				// Consumer Cluster is already working
				consumerCluster = ass.consumerCluster;
				QueueConsumer qc = new QueueConsumer((ChannelEndPoint) this.deliveryEndPoint, consumerCluster, ass,
						this.messageWindowSize);
				consumerCluster.addNewConsumer(qc);			

				// This "compareAndSet" operation is necessary
				if (ass.delivering.compareAndSet(false, true)) {
					ass.deliverNextMessage();
				}
			}
			/* msgbus team added */
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("Topic: ");
			sb.append(topic.toStringUtf8());
			sb.append("DeliveryPtr: ");
			sb.append(lastLocalSeqIdDelivered);
			return sb.toString();
		}
	}

	protected class StopServingSubscriber implements DeliveryManagerRequest {
		ActiveSubscriberState subscriber;
		SubscriptionEvent event;
		final Callback<Void> cb;
		final Object ctx;

		public StopServingSubscriber(ActiveSubscriberState subscriber, SubscriptionEvent event,
				Callback<Void> callback, Object ctx) {
			this.subscriber = subscriber;
			this.event = event;
			this.cb = callback;
			this.ctx = ctx;
		}

		@Override
		public void performRequest() {
			// This will automatically stop delivery, and disconnect the channel
			subscriber.setNotConnected(event);

			// if the subscriber has moved on, a move request for its delivery
			// pointer must be pending in the request queue. Note that the
			// subscriber first changes its delivery pointer and then submits a
			// request to move so this works.
			removeDeliveryPtr(subscriber, subscriber.getLastSeqIdCommunicatedExternally(), //
					// isAbsenceOk=
					true,
					// pruneTopic=
					true);
			cb.operationFinished(ctx, null);
		}
	}

	protected class DeliveryPtrMove implements DeliveryManagerRequest {

		ActiveSubscriberState subscriber;
		Long oldSeqId;
		Long newSeqId;

		public DeliveryPtrMove(ActiveSubscriberState subscriber, Long oldSeqId, Long newSeqId) {
			this.subscriber = subscriber;
			this.oldSeqId = oldSeqId;
			this.newSeqId = newSeqId;
		}

		@Override
		public void performRequest() {
			ByteString topic = subscriber.getTopic();
			long prevMinSeqId = getMinimumSeqId(topic);

			if (subscriber.isConnected()) {
				removeDeliveryPtr(subscriber, oldSeqId, //
						// isAbsenceOk=
						false,
						// pruneTopic=
						false);

				addDeliveryPtr(subscriber, newSeqId);
			} else {
				removeDeliveryPtr(subscriber, oldSeqId, //
						// isAbsenceOk=
						true,
						// pruneTopic=
						true);
			}

			long nowMinSeqId = getMinimumSeqId(topic);

			if (nowMinSeqId > prevMinSeqId) {
				persistenceMgr.deliveredUntil(topic, nowMinSeqId);
			}
		}
	}

	protected class ShutdownDeliveryManagerRequest implements DeliveryManagerRequest {
		// This is a simple type of Request we will enqueue when the
		// PubSubServer is shut down and we want to stop the DeliveryManager
		// thread.
		@Override
		public void performRequest() {
			keepRunning = false;
		}
	}

	/**
	 * ====================================================================
	 * 
	 * Dumb factories for our map methods
	 */
	protected static class TreeMapLongToSetSubscriberFactory implements
			Factory<SortedMap<Long, Set<ActiveSubscriberState>>> {
		static TreeMapLongToSetSubscriberFactory instance = new TreeMapLongToSetSubscriberFactory();

		@Override
		public SortedMap<Long, Set<ActiveSubscriberState>> newInstance() {
			return new TreeMap<Long, Set<ActiveSubscriberState>>();
		}
	}

	protected static class HashMapSubscriberFactory implements Factory<Set<ActiveSubscriberState>> {
		static HashMapSubscriberFactory instance = new HashMapSubscriberFactory();

		@Override
		public Set<ActiveSubscriberState> newInstance() {
			return new HashSet<ActiveSubscriberState>();
		}
	}

	/* msgbus team added */
	public class UnConsumedSeq {
		public UnConsumedSeq(long seq, long time) {
			this.seq = seq;
			this.timeStamp = time;
		}

		long seq;
		long timeStamp;
	}

	/**
	 * Use to send message to one client
	 */
	public class QueueConsumer implements DeliveryCallback {
		Channel channel;
		ChannelEndPoint endPoint; // for send
		ChannelEndPoint2 endPoint2; // for resend
		ConsumerCluster cc;
		ActiveSubscriberState ass;
		int messageWindowSize;
		AtomicBoolean isWaiting = new AtomicBoolean(true);
		long lastExpiredTime;

		// This member must be synchronized, or else it must be accessed in a serial order
		// I prefer to the latter way
		ConcurrentSkipListSet<UnConsumedSeq> unConsumedMsgSeqs = new ConcurrentSkipListSet<UnConsumedSeq>(
				new Comparator<UnConsumedSeq>() {

					@Override
					public int compare(UnConsumedSeq o1, UnConsumedSeq o2) {
						if (o1.seq > o2.seq)
							return 1;
						else if (o1.seq < o2.seq)
							return -1;
						else
							return 0;
					}
				});

		public QueueConsumer(ChannelEndPoint deliveryEndPoint, ConsumerCluster cc, ActiveSubscriberState ass,
				int messageWindowSize) {
			this.endPoint = deliveryEndPoint;
			this.channel = this.endPoint.getChannel();
			this.endPoint2 = new ChannelEndPoint2(this.channel);
			this.cc = cc;
			this.ass = ass;
			this.messageWindowSize = messageWindowSize;
			this.isWaiting.set(true);
			this.lastExpiredTime = 0;
		}

		@Override
		public void sendingFinished() {
			if (!ass.connected) {
				return;
			}

			ass.lastLocalSeqIdDelivered = ass.localSeqIdDeliveringNow;
			if (ass.lastLocalSeqIdDelivered > ass.lastSeqIdCommunicatedExternally + ActiveSubscriberState.SEQ_ID_SLACK) {
				long prevId = ass.lastSeqIdCommunicatedExternally;
				ass.lastSeqIdCommunicatedExternally = ass.lastLocalSeqIdDelivered;
				moveDeliveryPtrForward(ass, prevId, ass.lastLocalSeqIdDelivered);
			}
			// increment deliveried message
			ServerStats.getInstance().incrementMessagesDelivered();
			// call deliverNextMessage() after a message is consumed?
			ass.deliverNextMessage();
		}

		@Override
		public void transientErrorOnSend() {
			// no one call this
		}

		@Override
		public void permanentErrorOnSend(Object ctx) {
			// The unConsumed messages will be moved to retryMessageQueue when the request is processed, and
			// deliverNextMessage should be called after that moment
			enqueueWithoutFailure(new RemoveChannelForQueueRequest(ass.topic, ass.subscriberId, endPoint.channel));
		}

		public void resendingFinished() {
			// Don't no any thing
		}
	}

	protected static class SeqBlock {
		public long start;
		public long end;

		public SeqBlock(long l1, long l2) {
			this.start = l1;
			this.end = l2;
		}
	}

	/**
	 * Client cluster
	 */
	protected class ConsumerCluster {
		Lock qcMoveLock = new ReentrantLock(true);
		Lock qcDelLock = new ReentrantLock(true);
		public int stopCount = 0; // For test
		private long lastConsumedSeq;
		long lastConsumedTime;
		private ActiveSubscriberState ass;

		ConcurrentLinkedQueue<QueueConsumer> waitingConsumers = new ConcurrentLinkedQueue<QueueConsumer>();
		ConcurrentLinkedQueue<QueueConsumer> busyConsumers = new ConcurrentLinkedQueue<QueueConsumer>();
		ConcurrentHashMap<Channel, QueueConsumer> allConsumers = new ConcurrentHashMap<Channel, FIFODeliveryManager.QueueConsumer>(
				16);

		final long timeOut = 5000;
		AtomicLong lastCheckTime = new AtomicLong(System.currentTimeMillis());

		ConcurrentSkipListSet<UnConsumedSeq> retryMessageQueue = new ConcurrentSkipListSet<UnConsumedSeq>(
				new Comparator<UnConsumedSeq>() {

					@Override
					public int compare(UnConsumedSeq o1, UnConsumedSeq o2) {
						if (o1.seq > o2.seq)
							return 1;
						else if (o1.seq < o2.seq)
							return -1;
						else
							return 0;
					}
				});

		/**
		 * compare() return 0 means blocks are overlapped
		 * 
		 */
		TreeSet<SeqBlock> consumedSeqs = new TreeSet<SeqBlock>(new Comparator<SeqBlock>() {

			@Override
			public int compare(SeqBlock o1, SeqBlock o2) {
				if (o1.start > o2.end)
					return 1;
				else if (o1.end < o2.start)
					return -1;
				else
					return 0;
			}
		});

		public ConsumerCluster(ActiveSubscriberState ass, long lastConsumedSeq) {
			this.ass = ass;
			this.lastConsumedSeq = lastConsumedSeq;
			this.lastConsumedTime = System.currentTimeMillis();
		}

		public void checkTimeOut() {
			long now = System.currentTimeMillis();
			if (now - timeOut > lastCheckTime.get()) {
				// logger.debug("resendExpiredMessages.");
				checkExpiredMessages(now);
				lastCheckTime.set(now);
			}
		}

		public void addConsumeSeqIdToCluster(ByteString topic, ByteString subscriberId, MessageSeqId consumeSeqId,
				AbstractSubscriptionManager sm, Callback<Void> callback, Object ctx) {
			// test
			// logger.info("Consumed seq: " + consumeSeqId.getLocalComponent() + ". lastConsumedSeq: " +
			// lastConsumedSeq);

			// First update consumeSeqs of this
			long longConsumeSeqId = consumeSeqId.getLocalComponent();

			if (addToListAndMaybeConsume(longConsumeSeqId)) {
				sm.setConsumeSeqIdForSubscriber(topic, subscriberId, MessageSeqId.newBuilder(consumeSeqId)
						.setLocalComponent(lastConsumedSeq).build(), callback, null);
			}

			// Then update unConsumedMsgSeqs of qc
			Channel channel = (Channel) ctx;
			QueueConsumer qc = allConsumers.get(channel);

			if (qc == null) {
				// disconnected, the message will be resent, but and it's second consume will be omitted
				return;
			}

			// Is likely in qc.unConsumedMsgSeqs, could also be in retryMessageQueue, don't need sync
			// If both operation failed because the message being moving from retryMessageQueue to unConsumedMsgSeqs,
			// it will be send later
			UnConsumedSeq toRemove = new UnConsumedSeq(longConsumeSeqId, 0);
			if (!qc.unConsumedMsgSeqs.remove(toRemove)) {				
				this.retryMessageQueue.remove(toRemove);
			}			
			

			// Then remove cache of this message, only if it is a message queue.			
			// This is for making full of cache, but not necessary, so do not implement it
			/*if(subscriberId.equals(SubscriptionStateUtils.QUEUE_SUBID_BS)) {
				((ReadAheadCache) persistenceMgr).removeMessageFromCache(topic, longConsumeSeqId);
			}*/
			
			// finally check the states
			// To avoid using lock as far as possible
			if (qc.isWaiting.get() == false) {
				ass.consumerCluster.qcMoveLock.lock();
				if (qc.isWaiting.get() == false && qc.unConsumedMsgSeqs.size() < qc.messageWindowSize) {
					// logger.info("Move consumer from busyConsumers to waitingConsumers.");
					ass.consumerCluster.busyConsumers.remove(qc);
					ass.consumerCluster.waitingConsumers.add(qc);

					qc.isWaiting.set(true);
					logger.info("Try to wake up subscriber to deliver messages again.");
					ass.delivering.compareAndSet(false, true);
					enqueueWithoutFailure(new DeliveryManagerRequest() {
						@Override
						public void performRequest() {
							// enqueue
							clearRetryDelayForSubscriber(ass);
						}
					});
				}
				ass.consumerCluster.qcMoveLock.unlock();
			}

			if (!retryMessageQueue.isEmpty()) {
				enqueueWithoutFailure(new ResendRequest(this, false));
			}
		}

		public void addNewConsumer(QueueConsumer qc) {
			qcMoveLock.lock();
			waitingConsumers.add(qc);
			qcMoveLock.unlock();
			allConsumers.put(qc.channel, qc);

		}

		/**
		 * Add consumeSeq and maybe run ConsumeOp. Return true if consume should be performed.
		 * <p>
		 * ConsumeSeqs is organized by TreeSet.
		 */
		private boolean addToListAndMaybeConsume(long l) {
			lastConsumedTime = System.currentTimeMillis();

			if (l <= lastConsumedSeq) {
				logger.info("Expired ConsumeSeq: " + l);
				return false;
			}
			if (l == lastConsumedSeq + 1) {
				lastConsumedSeq++;
				if (consumedSeqs.isEmpty())
					return true;
				SeqBlock sb = consumedSeqs.first();
				// The first element of the list can also be consumed
				if (sb != null && sb.start == lastConsumedSeq + 1) {
					lastConsumedSeq = sb.end;
					consumedSeqs.remove(sb);
				}
				return true;
			}

			// Add to tree
			SeqBlock tmp = new SeqBlock(l, l);
			SeqBlock lower = consumedSeqs.lower(tmp);
			SeqBlock higher = consumedSeqs.higher(tmp);

			if (lower != null && lower.end + 1 == tmp.start) {
				tmp.start = lower.start;
				consumedSeqs.remove(lower);
			}

			if (higher != null && tmp.end + 1 == higher.start) {
				tmp.end = higher.end;
				consumedSeqs.remove(higher);
			}

			if (false == consumedSeqs.add(tmp)) {
				logger.info("Duplicated consumeSeq: " + l);
				return false;
			}
			return false;
		}

		private void checkExpiredMessages(long currentTime) {
			QueueConsumer qc = null;
			UnConsumedSeq unConsumedSeq = null;
			for (Channel channel : this.allConsumers.keySet()) {
				qc = allConsumers.get(channel);
				while (!qc.unConsumedMsgSeqs.isEmpty()) {
					unConsumedSeq = qc.unConsumedMsgSeqs.first();
					// logger.debug("Put expired message in retryMessageQueue: " + unConsumedSeq.seq);
					if (currentTime - unConsumedSeq.timeStamp < timeOut)
						break;
					enqueueWithoutFailure(new ResendRequest(this, true));
					return;
				}
			}
		}
	}

	// Request of adding consumeSeq.
	protected class AddConsumeSeqIdRequest implements DeliveryManagerRequest {
		ByteString topic;
		ByteString subscriberId;
		MessageSeqId consumeSeqId;
		AbstractSubscriptionManager sm;
		Callback<Void> callback;
		Object ctx;

		public AddConsumeSeqIdRequest(ByteString topic, ByteString subscriberId, MessageSeqId consumeSeqId,
				AbstractSubscriptionManager sm, Callback<Void> callback, Object ctx) {
			this.topic = topic;
			this.subscriberId = subscriberId;
			this.consumeSeqId = consumeSeqId;
			this.sm = sm;
			this.callback = callback;
			this.ctx = ctx;
		}

		@Override
		public void performRequest() {
			ConsumerCluster cc = subscriberStates.get(new TopicSubscriber(topic, subscriberId)).consumerCluster;
			cc.addConsumeSeqIdToCluster(topic, subscriberId, consumeSeqId, sm, callback, ctx);
		}
	}

	@Override
	public void addConsumeSeqForQueue(ByteString topic, ByteString subscriberId, MessageSeqId consumeSeqId,
			AbstractSubscriptionManager sm, Callback<Void> callback, Object ctx) {
		enqueueWithoutFailure(new AddConsumeSeqIdRequest(topic, subscriberId, consumeSeqId, sm, callback, ctx));
	}

	protected class RemoveChannelForQueueRequest implements DeliveryManagerRequest {
		ByteString topic;
		ByteString subid;
		Channel channel;

		public RemoveChannelForQueueRequest(ByteString topic, ByteString subid, Channel channel) {
			this.topic = topic;
			this.subid = subid;
			this.channel = channel;
		}

		@Override
		// Delete queueConsumer using the channel
		public void performRequest() {
			TopicSubscriber ts = new TopicSubscriber(topic, subid);
			ActiveSubscriberState ass = subscriberStates.get(ts);
			if (ass == null) {
				logger.info("Can't find the ActiveSubscriberState object.");
				return;
			}
			ConsumerCluster cc = ass.consumerCluster;
			QueueConsumer qc = cc.allConsumers.remove(channel);

			// Already removed
			if (qc == null) {
				return;
			}

			ass.consumerCluster.qcDelLock.lock();
			ass.consumerCluster.qcMoveLock.lock();
			ConcurrentLinkedQueue<QueueConsumer> consumers = qc.isWaiting.get() ? cc.waitingConsumers
					: cc.busyConsumers;
			consumers.remove(qc);
			ass.consumerCluster.qcMoveLock.unlock();

			// Can't modified unConsumedMsgSeqs when addAll() if implemented
			cc.retryMessageQueue.addAll(qc.unConsumedMsgSeqs);
			ass.consumerCluster.qcDelLock.unlock();

			if (cc.waitingConsumers.size() > 0) {
				enqueueWithoutFailure(new ResendRequest(cc, false));
			}

			if (cc.allConsumers.isEmpty()) {
				logger.info("Subscriber {} will be removed.", va(ts));
				// test
				logger.info("Last consumed seq: " + cc.lastConsumedSeq);
				cc.ass.delivering.set(false);
				stopServingSubscriber(cc.ass, SubscriptionEvent.SUBSCRIPTION_FORCED_CLOSED, NOP_CALLBACK,
						cc.ass.deliveryEndPoint);
			}
		}
	}

	protected class ResendRequest implements DeliveryManagerRequest {
		private ConsumerCluster cc;
		private Boolean isTimeOut;

		public ResendRequest(ConsumerCluster consumerCluster, Boolean isTimeOut) {
			this.cc = consumerCluster;
			this.isTimeOut = isTimeOut;
		}

		@Override
		public void performRequest() {
			UnConsumedSeq unConsumedSeq = null;
			if (isTimeOut) {				
				long currentTime = System.currentTimeMillis();
				QueueConsumer qc = null;
				for (Channel channel : cc.allConsumers.keySet()) {
					qc = cc.allConsumers.get(channel);
					while (!qc.unConsumedMsgSeqs.isEmpty()) {
						unConsumedSeq = qc.unConsumedMsgSeqs.first();
						if (unConsumedSeq.timeStamp + cc.timeOut > currentTime)
							break;
						logger.info("Move expired message to retryMessageQueue: " + unConsumedSeq.seq);

						qc.unConsumedMsgSeqs.remove(unConsumedSeq);
						cc.retryMessageQueue.add(unConsumedSeq);
					}
				}
			}

			while (!cc.retryMessageQueue.isEmpty()) {
				unConsumedSeq = cc.retryMessageQueue.first();
				cc.retryMessageQueue.remove(unConsumedSeq);
				ScanRequest scanRequest = new ScanRequest(cc.ass.topic, unConsumedSeq.seq, cc.ass, unConsumedSeq.seq);
				// logger.info("Resend: " + unConsumedSeq.seq);
				persistenceMgr.scanSingleMessage(scanRequest);
			}

		}
	}

	@Override
	// In queue condition, when channel is disconnected, some operations should
	// be done, so add this method
	public void channelDisconnected(ByteString topic, ByteString subid, Channel channel) {
		enqueueWithoutFailure(new RemoveChannelForQueueRequest(topic, subid, channel));
	}

	public boolean zkDeleteRecursive(String path) {
		List<String> children = null;

		try {
			children = zk.getChildren(path, false);
		} catch (KeeperException.NoNodeException e) {
			return true;
		} catch (KeeperException e) {
			return false;
		} catch (InterruptedException e) {
			return false;
		}

		for (String subPath : children) {
			if (!zkDeleteRecursive(path + "/" + subPath)) {
				return false;
			}
		}

		try {
			zk.delete(path, -1); // -1 matches any varsion
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public boolean deleteTopicPersistenceInfoRecursive(ByteString topic) {
		long currentSeqId = 0;
		try {
			currentSeqId = persistenceMgr.getCurrentSeqIdForTopic(topic).getLocalComponent();
		} catch (ServerNotResponsibleForTopicException e) {
			// Must do something
			e.printStackTrace();
		}
		long minSeqId = getMinimumSeqId(topic); // not fine
		if (currentSeqId - minSeqId > 0) {
			logger.info("Request to delete topic/queue when it has unconcumed message.");
		}
		String path = cfg.getZkTopicPath(new StringBuilder(), topic).toString();
		return zkDeleteRecursive(path);
	}

	/* msgbus team added */
}
