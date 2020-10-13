package com.mao.mqdemo.controller;

import com.mao.mqdemo.jms.JmsConfig;
import com.mao.mqdemo.jms.PayProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendCallback;
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

    /**
     * 同步发送mq
     *
     * @param msg
     * @return
     */
    @RequestMapping("mqSendSync")
    public SendResult mqSendSync(String msg) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC, "tag_a", "test111", ("hello mq " + msg).getBytes());

        SendResult sendResult = payProducer.getProducer().send(message);

        return sendResult;
    }

    /**
     * 异步发送mq：不会重试，发送总次数等于1
     *
     * @param msg
     * @return
     */
    @RequestMapping("mqSendAsync")
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
}
