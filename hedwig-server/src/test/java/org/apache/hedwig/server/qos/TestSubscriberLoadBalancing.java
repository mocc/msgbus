package org.apache.hedwig.server.qos;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase1;
import org.apache.hedwig.util.Callback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class TestSubscriberLoadBalancing extends HedwigHubTestBase1 {
    MsgBusClient msgBusClient, msgBusClient1, msgBusClient2, msgBusClient3;
    MessageQueueClient msgClient, msgClient1, msgClient2, msgClient3;
    URL path = this.getClass().getResource("/hw_client.conf");

    @Override
    @Before
    public void setUp() throws Exception {
        System.setProperty("build.test.dir", "F:\\test1");
        super.setUp();
        msgBusClient = new MsgBusClient(path);
        msgBusClient1 = new MsgBusClient(path);
        msgBusClient2 = new MsgBusClient(path);
        msgBusClient3 = new MsgBusClient(path);

        msgClient = msgBusClient.getMessageQueueClient();
        msgClient1 = msgBusClient1.getMessageQueueClient();
        msgClient2 = msgBusClient2.getMessageQueueClient();
        msgClient3 = msgBusClient3.getMessageQueueClient();
        System.out.println("setup finished..................................");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testLoadBalancing() throws Exception {
        System.out.println("testLoadBalancing beginning...........................");
        final String queueName = "testLoadBalancing";
        String prefix = "message";
        int numMessage = 1000;
        // begin publishing messages
        msgClient.createQueue(queueName);
        System.out.println("publishing work beginning!");
        for (int i = 1; i <= numMessage; i++) {
            // msgClient.asyncPublish(queueName, prefix + i, null, null);
            msgClient.publish(queueName, prefix + i);
        }
        System.out.println("publishing work is finished!" + msgClient.queryMessageCount(queueName));

        // multimsgClient subscribe to the same topic
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(100).build();

        final Map<String, MessageSeqId> receivedMsgs1 = new HashMap<String, MessageSeqId>();
        final Map<String, MessageSeqId> receivedMsgs2 = new HashMap<String, MessageSeqId>();
        final Map<String, MessageSeqId> receivedMsgs3 = new HashMap<String, MessageSeqId>();

        System.out.println("msgClient1 begins to delivery!");
        msgClient1.createQueue(queueName);
        msgClient1.startDelivery(queueName, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {

                String str = msg.getBody().toStringUtf8();
                receivedMsgs1.put(str, msg.getMsgId());
                System.out.println("msgClient1 receivedMsgs:" + str + ", " + msg.getMsgId());
                try {
                    System.out.println("msgClient1 sleeping..............................");
                    TimeUnit.MICROSECONDS.sleep(20);
                    System.out.println("msgClient1 begin consuming MessageID:  " + msg.getMsgId());
                    msgClient1.consumeMessage(queueName, msg.getMsgId());

                } catch (Exception e) {

                    e.printStackTrace();
                }
                // System.out.println("sleeping..............................");
                // try {
                // // TimeUnit.MILLISECONDS.sleep(1);
                // TimeUnit.MICROSECONDS.sleep(10);
                // } catch (InterruptedException e) {
                // // TODO Auto-generated catch block
                // e.printStackTrace();
                // }
            }

        }, options);

        System.out.println("msgClient2 begins to delivery!");
        msgClient2.createQueue(queueName);
        msgClient2.startDelivery(queueName, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {

                String str = msg.getBody().toStringUtf8();
                receivedMsgs2.put(str, msg.getMsgId());
                System.out.println("msgClient2 receivedMsgs:" + str + ", " + msg.getMsgId());
                try {
                    System.out.println("msgClient2 sleeping..............................");
                    // TimeUnit.MICROSECONDS.sleep(20);
                    msgClient2.consumeMessage(queueName, msg.getMsgId());
                    System.out.println("msgClient2 begin consuming MessageID:  " + msg.getMsgId());
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }, options);

        System.out.println("msgClient3 begins to delivery!");
        msgClient3.createQueue(queueName);
        msgClient3.startDelivery(queueName, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {

                String str = msg.getBody().toStringUtf8();
                receivedMsgs3.put(str, msg.getMsgId());
                System.out.println("msgClient3 receivedMsgs:" + str + ", " + msg.getMsgId());
                try {
                    System.out.println("msgClient3 sleeping..............................");
                    TimeUnit.MICROSECONDS.sleep(200);
                    msgClient3.consumeMessage(queueName, msg.getMsgId());
                    System.out.println("msgClient3 begin consuming MessageID:  " + msg.getMsgId());
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }, options);

        TimeUnit.MILLISECONDS.sleep(500);
        msgClient1.stopDelivery(queueName);
        msgClient2.stopDelivery(queueName);
        msgClient3.stopDelivery(queueName);

        msgClient1.closeSubscription(queueName);
        msgClient2.closeSubscription(queueName);
        msgClient3.closeSubscription(queueName);

        // System.out.println("message count after stopdelivery:  " +
        // msgClient.queryMessageCount(queueName));

        System.out.println("....................................................");
        System.out.println("number of messages msgClient1 received: " + receivedMsgs1.size());
        System.out.println("number of messages msgClient2 received: " + receivedMsgs2.size());
        System.out.println("number of messages msgClient3 received: " + receivedMsgs3.size());

    }

}
