package com.mao.mqdemo.jms;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

/**
 * @author Mingpeidev
 * @date 2020/10/15 9:29
 * @description 分布式事务消息
 */
@Component
public class TransactionProducer {

    private String producerGroup = "trans_producer_group";

    //事务监听器
    private TransactionListener transactionListener = new TransactionListenerImpl();

    private TransactionMQProducer producer;

    private ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("client-transaction-msg-check-thread");
            return thread;
        }
    });

    public TransactionProducer() {
        producer = new TransactionMQProducer(producerGroup);

        producer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        producer.setTransactionListener(transactionListener);
        producer.setExecutorService(executorService);

        start();
    }

    public TransactionMQProducer getProducer() {
        return this.producer;
    }

    /**
     * 对象在使用之前必须要调用一次，只能初始化一次
     */
    public void start() {
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     */
    public void shutdown() {
        this.producer.shutdown();
    }

    class TransactionListenerImpl implements TransactionListener {

        //执行本地事务
        @Override
        public LocalTransactionState executeLocalTransaction(Message message, Object o) {
            System.out.println("====executeLocalTransaction=======");

            String body = new String(message.getBody());
            String key = message.getKeys();
            String transactionId = message.getTransactionId();

            System.out.println("transactionId=" + transactionId + ", key=" + key + ", body=" + body);

            // 执行本地事务begin TODO

            // 执行本地事务end TODO

            int status = Integer.parseInt(o.toString());

            switch (status) {
                case 1:
                    //二次确认消息，然后消费者可以消费
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    //回滚消息，broker端会删除半消息
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                case 3:
                    //broker端会进行回查消息，再或者什么都不响应返回null，会定时回查
                    return LocalTransactionState.UNKNOW;
                default:
                    return null;
            }
        }

        //当LocalTransactionState返回UNKNOW或者超过一定时间(默认1min)，回查消息，要么commit要么rollback，reconsumeTimes不生效
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
            System.out.println("====checkLocalTransaction=======");

            String body = new String(messageExt.getBody());
            String key = messageExt.getKeys();
            String transactionId = messageExt.getTransactionId();

            System.out.println("transactionId=" + transactionId + ", key=" + key + ", body=" + body);

            //要么commit 要么rollback

            //可以根据key去检查本地事务消息是否完成

            //也可返回ROLLBACK_MESSAGE
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}
