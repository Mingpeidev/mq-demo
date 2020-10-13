rocketMQ

url:
http://localhost:8080/mqSendSync?msg=reSend             同步发送重试(SYNC)
http://localhost:8080/mqSendAsync?msg=reSend            异步发送重试(ASYNC)
http://localhost:8080/mqSendOneWay?msg=reSend           One-way发送重试

http://localhost:8080/mqSendSync?msg=test               同步发送不重试
http://localhost:8080/mqSendAsync?msg=test              异步发送不重试
http://localhost:8080/mqSendOneWay?msg=test             One-way发送不重试