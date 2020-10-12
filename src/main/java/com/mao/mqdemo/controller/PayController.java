package com.mao.mqdemo.controller;

import com.mao.mqdemo.jms.PayProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author Mingpeidev
 * @date 2020/10/12 17:17
 * @description
 */
@RestController
public class PayController {
    @Resource
    private PayProducer payProducer;

    private static final String topic = "mq_demo_pay_test_topic";

    @RequestMapping("mqSend")
    public SendResult test() {
        Message message = new Message(topic, "tag_a", "test111", "hello mq".getBytes());

        SendResult sendResult = null;
        try {
            sendResult = payProducer.getProducer().send(message);
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return sendResult;
    }
}
