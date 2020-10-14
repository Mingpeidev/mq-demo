package com.mao.mqdemo.jms;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Mingpeidev
 * @date 2020/10/14 9:50
 * @description mq顺序消息消费者。监听 pay_order_topic_orderly
 */
@Component
public class PayOrderlyConsumer {
    private DefaultMQPushConsumer consumer;

    private String consumerGroup = "pay_orderly_consumer_group";

    public PayOrderlyConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);

        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //默认是集群方式，可以更改为广播，但是广播方式不支持重试
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.subscribe(JmsConfig.ORDERLY_TOPIC, "*");

        //顺序消费 加锁 同一时间同一个queue只会有一个consumer消费 防止并发 实现顺序消费
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {

                MessageExt msg = list.get(0);

                try {
                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody()));

                    //业务逻辑操作

                    return ConsumeOrderlyStatus.SUCCESS;
                } catch (Exception exception) {
                    exception.printStackTrace();

                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }
        });

        consumer.start();
        System.out.println("consumer pay_order_topic_orderly start ...");
    }

}
