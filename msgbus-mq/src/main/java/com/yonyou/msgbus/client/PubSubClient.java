package com.yonyou.msgbus.client;

import java.util.List;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;

import com.google.protobuf.ByteString;

public class PubSubClient {

	public static final String TOPIC_PREFIX = "t_";

	private Publisher publisher;
	private Subscriber subscriber;
	private boolean isClosed;

	// Invisible to users
	PubSubClient(Publisher publisher, Subscriber subscriber) {
		this.publisher = publisher;
		this.subscriber = subscriber;
	}

	public PublishResponse publish(String topic, String msg) throws CouldNotConnectException, ServiceDownException {
		if (isClosed) {
			throw new IllegalStateException("Client has already been closed");
		}
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		Message myMessage = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
		return publisher.publish(myTopic, myMessage);
	}

	public void asyncPublish(String topic, String msg, Callback<Void> callback, Object context) {
		if (isClosed) {
			throw new IllegalStateException("Client has already been closed");
		}
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		Message myMsg = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
		publisher.asyncPublish(myTopic, myMsg, callback, context);
	}

	public void asyncPublishWithResponse(String topic, String msg, Callback<PublishResponse> callback, Object context) {
		if (isClosed) {
			throw new IllegalStateException("Client has already been closed");
		}
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		Message myMsg = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
		publisher.asyncPublishWithResponse(myTopic, myMsg, callback, context);
	}

	protected void subscribe(String topic, String subscriberId, CreateOrAttach mode) throws CouldNotConnectException,
			ClientAlreadySubscribedException, ServiceDownException, InvalidSubscriberIdException {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.subscribe(myTopic, mySubscriberId, mode);
	}

	// merge with above function
	public void subscribe(String topic, String subscriberId, SubscriptionOptions options)
			throws CouldNotConnectException, ClientAlreadySubscribedException, ServiceDownException,
			InvalidSubscriberIdException {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.subscribe(myTopic, mySubscriberId, options);
	}

	protected void asyncSubscribe(String topic, String subscriberId, CreateOrAttach mode, Callback<Void> callback,
			Object context) {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		SubscriptionOptions options = SubscriptionOptions.newBuilder().setCreateOrAttach(mode).build();
		subscriber.asyncSubscribe(myTopic, mySubscriberId, options, callback, context);
	}

	// merge with above function
	public void asyncSubscribe(String topic, String subscriberId, SubscriptionOptions options, Callback<Void> callback,
			Object context) {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.asyncSubscribe(myTopic, mySubscriberId, options, callback, context);
	}

	// this will call perform closeSubscription in advance
	public void unsubscribe(String topic, String subscriberId) throws CouldNotConnectException,
			ClientNotSubscribedException, ServiceDownException, InvalidSubscriberIdException {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.unsubscribe(myTopic, mySubscriberId);
	}

	public void asyncUnsubscribe(final String topic, final String subscriberId, final Callback<Void> callback,
			final Object context) {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.asyncUnsubscribe(myTopic, mySubscriberId, callback, context);
	}

	public void consume(String topic, String subscriberId, MessageSeqId messageSeqId)
			throws ClientNotSubscribedException {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.consume(myTopic, mySubscriberId, messageSeqId);
	}

	public boolean hasSubscription(String topic, String subscriberId) throws CouldNotConnectException,
			ServiceDownException {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		return subscriber.hasSubscription(myTopic, mySubscriberId);
	}

	// should't be visible to users until it works
	protected List<ByteString> getSubscriptionList(String subscriberId) throws CouldNotConnectException,
			ServiceDownException {
		// Same as the previous hasSubscription method, this data should reside
		// on the server end, not the client side.
		return null;
	}

	public void startDelivery(final String topic, final String subscriberId, MessageHandler messageHandler)
			throws ClientNotSubscribedException, AlreadyStartDeliveryException {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.startDelivery(myTopic, mySubscriberId, messageHandler);
	}

	public void startDeliveryWithFilter(final String topic, final String subscriberId, MessageHandler messageHandler,
			ClientMessageFilter messageFilter) throws ClientNotSubscribedException, AlreadyStartDeliveryException {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.startDeliveryWithFilter(myTopic, mySubscriberId, messageHandler, messageFilter);
	}

	public void stopDelivery(final String topic, final String subscriberId) throws ClientNotSubscribedException {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.stopDelivery(myTopic, mySubscriberId);
	}

	public void closeSubscription(String topic, String subscriberId) throws ServiceDownException {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.closeSubscription(myTopic, mySubscriberId);
	}

	public void asyncCloseSubscription(final String topic, final String subscriberId, final Callback<Void> callback,
			final Object context) {
		ByteString myTopic = ByteString.copyFromUtf8(PubSubClient.TOPIC_PREFIX + topic);
		ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
		subscriber.asyncCloseSubscription(myTopic, mySubscriberId, callback, context);
	}

	// subscriber.addSubscriptionListener() and subscriber.removeSubscriptionListener() is supported inside if needed
	// addSubscriptionListener()
	// removeSubscriptionListener

	void close() {
		isClosed = true;
	}

}
