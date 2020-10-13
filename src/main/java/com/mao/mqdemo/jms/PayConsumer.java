package com.mao.mqdemo.jms;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Mingpeidev
 * @date 2020/10/13 9:29
 * @description
 */
@Component
public class PayConsumer {

    private String consumerGroup = "pay_consumer_group";

    private DefaultMQPushConsumer consumer;

    public PayConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        //监听哪些Tag标签
        consumer.subscribe(JmsConfig.TOPIC, "*");

        //消费逻辑
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

                try {
                    Message msg = list.get(0);
                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody()));

                    String topic = msg.getTopic();
                    String body = new String(msg.getBody(), "UTF-8");
                    String tags = msg.getTags();
                    String keys = msg.getKeys();
                    System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception exception) {
                    exception.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });

        consumer.start();
        System.out.println("consumer start ... ");
    }

}
