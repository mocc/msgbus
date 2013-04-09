package org.apache.hedwig.server.delivery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.filter.PipelineFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.FIFODeliveryManager.ConsumerCluster;
import org.apache.hedwig.server.delivery.FIFODeliveryManager.QueueConsumer;
import org.apache.hedwig.server.persistence.PersistRequest;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.StubPersistenceManager;
import org.apache.hedwig.server.subscriptions.AllToAllTopologyFilter;
import org.apache.hedwig.util.Callback;
import org.jboss.netty.channel.Channel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class TestWindowControlWithoutSetup {

    static Logger logger = LoggerFactory.getLogger(TestWindowControlWithoutSetup.class);

    static class TestCallback implements Callback<MessageSeqId> {
        AtomicBoolean success = new AtomicBoolean(false);
        final CountDownLatch latch;
        MessageSeqId msgid = null;

        TestCallback(CountDownLatch l) {
            this.latch = l;
        }

        public void operationFailed(Object ctx, PubSubException exception) {
            logger.error("Persist operation failed", exception);
            latch.countDown();
        }

        public void operationFinished(Object ctx, MessageSeqId resultOfOperation) {
            msgid = resultOfOperation;
            success.set(true);
            latch.countDown();
        }

        MessageSeqId getId() {
            assertTrue("Persist operation failed", success.get());
            return msgid;
        }
    }

    /**
     * Delivery endpoint which puts all responses on a queue
     */
    static class ExecutorDeliveryEndPointWithQueue implements DeliveryEndPoint {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger numResponses = new AtomicInteger(0);
        ConcurrentLinkedQueue<PubSubResponse> queue = new ConcurrentLinkedQueue<PubSubResponse>();

        public void send(final PubSubResponse response, final DeliveryCallback callback) {
            logger.info("Received response {}", response);
            queue.add(response);
            numResponses.incrementAndGet();
            executor.submit(new Runnable() {
                public void run() {
                    callback.sendingFinished();
                }
            });
        }

        public void close() {
            executor.shutdown();
        }

        PubSubResponse getNextResponse() {
            return queue.poll();
        }

        int getNumResponses() {
            return numResponses.get();
        }
    }

    static class ExecutorDeliveryEndPoint implements DeliveryEndPoint {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger numDelivered = new AtomicInteger();
        final DeliveryManager dm;

        ExecutorDeliveryEndPoint(DeliveryManager dm) {
            this.dm = dm;
        }

        public void send(final PubSubResponse response, final DeliveryCallback callback) {
            executor.submit(new Runnable() {
                public void run() {
                    if (response.hasMessage()) {
                        MessageSeqId msgid = response.getMessage().getMsgId();
                        if ((msgid.getLocalComponent() % 2) == 1) {
                            dm.messageConsumed(response.getTopic(), response.getSubscriberId(), response.getMessage()
                                    .getMsgId());
                        } else {
                            executor.schedule(new Runnable() {
                                public void run() {
                                    dm.messageConsumed(response.getTopic(), response.getSubscriberId(), response
                                            .getMessage().getMsgId());
                                }
                            }, 1, TimeUnit.SECONDS);
                        }
                    }
                    numDelivered.incrementAndGet();
                    callback.sendingFinished();
                }
            });
        }

        public void close() {
            executor.shutdown();
        }

        int getNumDelivered() {
            return numDelivered.get();
        }
    }

    /**
     * Test throttle race issue cause by messageConsumed and
     * doDeliverNextMessage {@link https
     * ://issues.apache.org/jira/browse/BOOKKEEPER-503}
     */
    @Test
    public void testWindowControlWithPersistence() throws Exception {
        final int numMessages = 20;
        final int windowSize = 10;
        ServerConfiguration conf = new ServerConfiguration() {
            @Override
            public int getDefaultMessageWindowSize() {
                return windowSize;
            }
        };
        ByteString topic = ByteString.copyFromUtf8("windowControlTopic");
        ByteString subscriber = ByteString.copyFromUtf8("windowControlSubscriber");

        PersistenceManager pm = new StubPersistenceManager();
        FIFODeliveryManager fdm = new FIFODeliveryManager(pm, conf);
        ExecutorDeliveryEndPoint dep = new ExecutorDeliveryEndPoint(fdm);
        SubscriptionPreferences prefs = SubscriptionPreferences.newBuilder().build();

        PipelineFilter filter = new PipelineFilter();
        filter.addLast(new AllToAllTopologyFilter());
        filter.initialize(conf.getConf());
        filter.setSubscriptionPreferences(topic, subscriber, prefs);

        CountDownLatch l = new CountDownLatch(numMessages);

        TestCallback firstCallback = null;

        // publish messages by persistence manager
        for (int i = 0; i < numMessages; i++) {
            Message m = Message.newBuilder().setBody(ByteString.copyFromUtf8(String.valueOf(i))).build();
            TestCallback cb = new TestCallback(l);
            if (firstCallback == null) {
                firstCallback = cb;
            }
            pm.persistMessage(new PersistRequest(topic, m, cb, null));
        }
        assertTrue("Persistence never finished", l.await(10, TimeUnit.SECONDS));
        // deliver the messages
        fdm.start();

        ConsumerCluster cc = fdm.subscriberStates.get(new TopicSubscriber(topic, subscriber)).consumerCluster;
        ConcurrentLinkedQueue<QueueConsumer> qc = cc.waitingConsumers;
        Channel channel = (Channel) new Object();
        QueueConsumer qc1 = cc.allConsumers.get(channel);
        assertrue("not in the waiting queue", qc1.isWaiting.get());

        fdm.startServingSubscription(topic, subscriber, prefs, firstCallback.getId(), dep, filter,
                new Callback<Void>() {
                    @Override
                    public void operationFinished(Object ctx, Void result) {
                    }

                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        // would not happened
                    }
                }, null);

        int count = 30; // wait for 30 seconds maximum
        while (dep.getNumDelivered() < numMessages) {
            Thread.sleep(1000);
            if (count-- == 0) {
                break;
            }
        }
        assertEquals("Should have delivered " + numMessages, numMessages, dep.getNumDelivered());
    }

    private void assertrue(String string, boolean b) {
        // TODO Auto-generated method stub

    }

}
