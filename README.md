# Based-on-ZooKeeper-Hdfs-Netty-High-Availability-About-Keep-Alive-And-Data-Syn
基于Netty、ZooKeeper、Hdfs的高可用性的数据同步和保活


入口在这里：

src/main/java/Zookeeper/ZooKeeper_Master/StartRun.java
     
客户端的测试代码在这里：

src/main/java/Zookeeper/ZooKeeper_Nio/Client.java

提供五种动作：
0.创建节点

1.删除节点

2.查询节点

3.修改节点

4.同步数据

11,12,13创建节点的监听器（节点监听器，节点的孩子监听器、tree监听器）


"Packet#####"+getSendname()+"#####"+getCode()+"#####"+getData()+"#####"+getSessionId()

getSendname() ---》发送者的名字

getCode() ---》 事件代码（0,1,2,3,4,11,12,13）

getData() ---》 数据，没有事自动填充为null

getSessionId() ---》会话id，第一次建立时服务器分配一个会话id。


在client运行后在终端输入

Packet#####127.0.0.1:12345   //先获取会话id，此时服务器会给client返回一个会话id，如1

Packet#####127.0.0.1:1235#####1#####hello#####

Packet#####127.0.0.1:1235#####3#####hell，world.#####1
。。。。

     
