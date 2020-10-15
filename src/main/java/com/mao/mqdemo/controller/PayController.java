package com.mao.mqdemo.controller;

import com.mao.mqdemo.jms.JmsConfig;
import com.mao.mqdemo.jms.PayProducer;
import com.mao.mqdemo.jms.TransactionProducer;
import com.mao.mqdemo.pojo.ProductOrder;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author Mingpeidev
 * @date 2020/10/12 17:17
 * @description
 */
@RestController
@RequestMapping("/mqSend")
public class PayController {
    @Resource
    private PayProducer payProducer;

    @Resource
    private TransactionProducer transactionProducer;

    /**
     * 同步发送mq
     * <p>
     * 有发送结果反馈，数据可靠。用于重要信息发送，如邮件、报名短信等。
     *
     * @param msg
     * @return
     * @throws InterruptedException
     * @throws RemotingException
     * @throws MQClientException
     * @throws MQBrokerException
     */
    @RequestMapping("sync")
    public SendResult mqSendSync(String msg) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC, "tag_a", "test111", ("hello mq " + msg).getBytes());

        //延迟消息
        //1表示配置里面的第一个级别，2表示第二个级别
        //"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
        message.setDelayTimeLevel(3);

        //根据sql语法进行过滤消息 设置amount>5时消费
        message.putUserProperty("amount", "1");

        SendResult sendResult = payProducer.getProducer().send(message);

        System.out.println("发送成功" + sendResult.toString());

        return sendResult;
    }

    /**
     * 异步发送mq：不会重试，发送总次数等于1
     * <p>
     * 有发送结果反馈，数据可靠。对RT时间敏感，可以支持更高的并发，回调成功后触发对应业务，用于如注册成功后通知积分系统发放优惠券。
     *
     * @param msg
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    @RequestMapping("async")
    public void mqSendAsync(String msg) throws RemotingException, MQClientException, InterruptedException {
        Message message = new Message(JmsConfig.TOPIC, "tag_a", "test111", ("hello mq " + msg).getBytes());

        payProducer.getProducer().send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.printf("发送结果=%s, msg=%s ", sendResult.getSendStatus(), sendResult.toString());
            }

            @Override
            public void onException(Throwable throwable) {
                //补偿机制，根据业务情况进行使用，看是否进行重试
            }
        });
    }

    /**
     * One-way方式发送
     * <p>
     * 无需等待响应，无发送结果反馈，数据可能丢失。主要用于日志收集，适用于某些耗时非常短，但对可靠性要求并不高的场景，也就是LogServer
     * 只负责发送消息，不等待服务器回应且无回调函数触发，即只发送请求不等待应答。
     *
     * @param msg
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    @RequestMapping("oneWay")
    public void mqSendOneWay(String msg) throws RemotingException, MQClientException, InterruptedException {
        Message message = new Message(JmsConfig.TOPIC, "tag_a", "test111", ("hello mq " + msg).getBytes());

        payProducer.getProducer().sendOneway(message);
    }

    /**
     * 将消息投递到指定queue arg是queue下标
     *
     * @param msg
     * @throws InterruptedException
     * @throws RemotingException
     * @throws MQClientException
     * @throws MQBrokerException
     */
    @RequestMapping("toQueue")
    public void mqSendToQueue(String msg) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC, "tag_a", "test111", ("hello mq " + msg).getBytes());

        //同步发送指定queue 0
        /*SendResult sendResult = payProducer.getProducer().send(message, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                int queueNum = Integer.valueOf(o.toString());
                return list.get(queueNum);
            }
        }, 0);

        System.out.printf("发送结果=%s, msg=%s ", sendResult.getSendStatus(), sendResult.toString());*/

        //异步发送指定queue 1
        payProducer.getProducer().send(message, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                int queueNum = Integer.valueOf(o.toString());
                return list.get(queueNum);
            }
        }, 1, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.printf("发送结果=%s, msg=%s ", sendResult.getSendStatus(), sendResult.toString());
            }

            @Override
            public void onException(Throwable throwable) {
                throwable.printStackTrace();
            }
        });
    }

    /**
     * 顺序消息实现
     *
     * @throws InterruptedException
     * @throws RemotingException
     * @throws MQClientException
     * @throws MQBrokerException
     */
    @RequestMapping("orderMsg")
    public void orderMsg() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        List<ProductOrder> productOrderList = ProductOrder.getOrderList();

        for (int i = 0; i < productOrderList.size(); i++) {
            ProductOrder productOrder = productOrderList.get(i);

            //发送消息
            Message msg = new Message(JmsConfig.ORDERLY_TOPIC, "",
                    productOrder.getOrderId() + "", productOrder.toString().getBytes());

            //同一个订单号发送到同一个queue
            SendResult sendResult = payProducer.getProducer().send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    long id = (Long) o;
                    int index = (int) (id % list.size());

                    return list.get(index);
                }
            }, productOrder.getOrderId());

            System.out.printf("发送结果=%s, sendResult=%s ,order_id=%s, type=%s\n", sendResult.getSendStatus(),
                    sendResult.toString(), productOrder.getOrderId(), productOrder.getType());
        }
    }

    /**
     * 分布式事务消息发送实例
     *
     * @param tag        信息主体
     * @param otherParam 传入executeLocalTransaction的Object，程序进行判断1提交，2回滚，3走checkLocalTransaction
     * @return
     * @throws MQClientException
     */
    @RequestMapping("trans")
    public SendResult trans(String tag, String otherParam) throws MQClientException {
        Message message = new Message(JmsConfig.TOPIC, tag, tag + "_key", tag.getBytes());

        SendResult sendResult = transactionProducer.getProducer().sendMessageInTransaction(message, otherParam);

        System.out.printf("发送结果=%s, sendResult=%s \n", sendResult.getSendStatus(), sendResult.toString());

        return sendResult;
    }
}
