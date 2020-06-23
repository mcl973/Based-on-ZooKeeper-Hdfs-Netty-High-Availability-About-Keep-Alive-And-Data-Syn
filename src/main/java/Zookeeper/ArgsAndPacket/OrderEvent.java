  package Zookeeper.ArgsAndPacket;

//import java.nio.channels.SocketChannel;

import Zookeeper.ZooKeeper_Master.DistributIdGenerate;
import io.netty.channel.socket.SocketChannel;

public class OrderEvent {
    private SocketChannel socketChannel;
    private int order;
    private String data;
    private DistributIdGenerate distributIdGenerate;
    public OrderEvent(SocketChannel socketChannel, int order, DistributIdGenerate distributIdGenerate){
        this.socketChannel = socketChannel;
        this.order = order;
        this.distributIdGenerate = distributIdGenerate;
    }
    public OrderEvent(SocketChannel socketChannel, int order, String data, DistributIdGenerate distributIdGenerate){
        this.socketChannel = socketChannel;
        this.order = order;
        this.data = data;
        this.distributIdGenerate = distributIdGenerate;
    }
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public DistributIdGenerate getDistributIdGenerate() {
        return distributIdGenerate;
    }

    public void setDistributIdGenerate(DistributIdGenerate distributIdGenerate) {
        this.distributIdGenerate = distributIdGenerate;
    }
}
