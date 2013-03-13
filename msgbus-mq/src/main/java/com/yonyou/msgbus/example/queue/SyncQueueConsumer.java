package com.yonyou.msgbus.example.queue;

import java.net.MalformedURLException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class SyncQueueConsumer {

	/**
	 * @param args
	 */	
	private MessageQueueClient client;
	public static Logger logger = LoggerFactory.getLogger(SyncQueueConsumer.class);
	
	public SyncQueueConsumer(String path) throws MalformedURLException, ConfigurationException {
		final MsgBusClient msgBusClient = new MsgBusClient(path);
		client=msgBusClient.getMessageQueueClient();		
	}
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
    		System.out.println("Parameters: queueName subid(whatever) num");
			return;
		}
		java.security.Security.setProperty("networkaddress.cache.ttl", "0");
		SyncQueueConsumer c=new SyncQueueConsumer("F:/Java Projects3/hedwig/hw_client.conf");
		c.recv(args);		
	}

	public void recv(String[] args) throws ClientNotSubscribedException, AlreadyStartDeliveryException, ServiceDownException, InterruptedException  {
		long start= System.currentTimeMillis();
		String queueName = args[0];
		int num = Integer.parseInt(args[2]);		
		Message msg;
		
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(1).build();		
		if(!client.createQueue(queueName, false, options)) {
			return;
		}
		int i=0;
		while(i<num){
			msg=client.receive(queueName, 1000);
			if(null==msg){				
				System.out.println("Received null.");
				break;
				
			}else{
				Thread.sleep(1000);	//to test timeout
				client.consumeMessage(queueName, msg.getMsgId());
				System.out.println(msg.getBody().toStringUtf8());
				System.out.println(msg.getMsgId().getLocalComponent());
				i++;				
			}
		}
		client.stopDelivery(queueName);	
		
		// give enough time to send consume request		
		Thread.sleep(3000);
		
		client.closeSubscription(queueName);
		long end= System.currentTimeMillis();
		System.out.println("Received " +i +" messages in "+ (end-start) +"ms.");		
	}
}
