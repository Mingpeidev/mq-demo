rocketMQ

url:
http://localhost:8080/mqSend/sync?msg=reSend             同步发送重试(SYNC)--延时10s
http://localhost:8080/mqSend/async?msg=reSend            异步发送重试(ASYNC)
http://localhost:8080/mqSend/oneWay?msg=reSend           One-way发送重试

http://localhost:8080/mqSend/sync?msg=test               同步发送不重试--延时10s
http://localhost:8080/mqSend/async?msg=test              异步发送不重试
http://localhost:8080/mqSend/oneWay?msg=test             One-way发送不重试

http://localhost:8080/mqSend/toQueue?msg=test            投递消息到指定queue

http://localhost:8080/mqSend/orderMsg                    顺序消息投递