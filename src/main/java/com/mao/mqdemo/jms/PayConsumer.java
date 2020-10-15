package com.mao.mqdemo.jms;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Mingpeidev
 * @date 2020/10/13 9:29
 * @description mq消费者, 当消息体为reSend时可以测试重试机制。监听 mq_demo_pay_test_topic
 */
@Component
public class PayConsumer {

    private String consumerGroup = "pay_consumer_group";

    private DefaultMQPushConsumer consumer;

    public PayConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        //默认是集群方式，可以更改为广播，但是广播方式不支持重试
        consumer.setMessageModel(MessageModel.CLUSTERING);
        //1.监听所有Tag标签
        //consumer.subscribe(JmsConfig.TOPIC, "*");

        //2.多标签订阅
        //consumer.subscribe(JmsConfig.TOPIC, "tag_a||tag_b||tag_c");

        //3.根据SQL92语法进行过滤消息
        consumer.subscribe(JmsConfig.TOPIC, MessageSelector.bySql("amount>5"));

        //消费逻辑
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

                MessageExt msg = list.get(0);

                int times = msg.getReconsumeTimes();
                System.out.println("重试次数=" + times);

                try {
                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody()));

                    String topic = msg.getTopic();
                    String body = new String(msg.getBody(), "UTF-8");
                    String tags = msg.getTags();
                    String keys = msg.getKeys();
                    System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);

                    //异常模拟
                    if (body.equalsIgnoreCase("hello mq reSend")) {
                        throw new Exception();
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception exception) {
                    System.out.println("消费异常" + exception);

                    if (times >= 2) {
                        System.out.println("重试次数大于2，记录数据库，发短信通知开发人员或者运营人员");
                        //告诉broker，消息成功
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }

                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });

        consumer.start();
        System.out.println("consumer mq_demo_pay_test_topic start ...");
    }

}
