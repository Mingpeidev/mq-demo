rocketMQ

url:
http://localhost:8080/mqSendSync?msg=reSend             同步发送重试(SYNC)--延时10s
http://localhost:8080/mqSendAsync?msg=reSend            异步发送重试(ASYNC)
http://localhost:8080/mqSendOneWay?msg=reSend           One-way发送重试

http://localhost:8080/mqSendSync?msg=test               同步发送不重试--延时10s
http://localhost:8080/mqSendAsync?msg=test              异步发送不重试
http://localhost:8080/mqSendOneWay?msg=test             One-way发送不重试

http://localhost:8080/mqSendToQueue?msg=test            投递消息到指定queue