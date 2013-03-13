package com.yonyou.msgbus.client;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.netty.HedwigSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.util.Callback;

import com.google.protobuf.ByteString;

public class MsgBusClient {
	public HedwigClient client;
	public Publisher publisher;
	public Subscriber subscriber;
	
	private PubSubClient pubSubClient=null;
	private MessageQueueClient mqClient=null;
	private static final ByteString STATS_ID = ByteString.copyFromUtf8("2");
	
	public MsgBusClient() {
		ClientConfiguration conf = new ClientConfiguration();
		this.client = new HedwigClient(conf);
		this.publisher = client.getPublisher();
		this.subscriber = client.getSubscriber();
	}

	public MsgBusClient(String localPath) throws MalformedURLException, ConfigurationException {			
		this( new URL("file://localhost/" + localPath));
	}
	
	public MsgBusClient(URL url) throws MalformedURLException, ConfigurationException {		
		ClientConfiguration cfg = new ClientConfiguration();
		cfg.loadConf(url);		
		client = new HedwigClient(cfg);
		this.publisher = client.getPublisher();
		this.subscriber = client.getSubscriber();
	}
	
	public PubSubClient getPubSubClient() {
		if(pubSubClient==null)
			return new PubSubClient(publisher, subscriber);
		else
			return pubSubClient;
	}
	
	public MessageQueueClient getMessageQueueClient() {
		if(mqClient==null)
			return new MessageQueueClient(publisher, subscriber);
		else			
			return mqClient;
	}
	
	public void getAvailableHubs(Callback<ResponseBody> callback) {
		((HedwigSubscriber)subscriber).getAvailableHubs(ByteString.copyFromUtf8("q_test"), STATS_ID, callback, null);
	}
	
	public void close() {
		if(pubSubClient!=null)
			pubSubClient.close();
		if(mqClient!=null)
			mqClient.close();		
        client.close();
    }
}
