package Zookeeper.ArgsAndPacket;



import Zookeeper.ZooKeeper_Master.DistributIdGenerate;
import io.netty.channel.socket.SocketChannel;
import Zookeeper.ZooKeeper_Master.ContainerAndHandler;

//import java.nio.channels.SocketChannel;

public class HandlePacket implements Runnable{
    private  String packet;
    private SocketChannel socketChannel;
    private DistributIdGenerate distributIdGenerate;
    public HandlePacket(String packet, SocketChannel socketChannel, DistributIdGenerate distributIdGenerate){
        this.packet = packet;
        this.socketChannel = socketChannel;
        this.distributIdGenerate = distributIdGenerate;
    }

    @Override
    public void run() {
        String[] split = packet.split("#####");
        System.out.println("当前的发送者是："+split[1]);
//        if(split.length == 4)
//            ContainerAndHandler.eventQueue.offer(new OrderEvent(socketChannel,Integer.parseInt(split[2]),distributIdGenerate));
//        else if(split.length == 5)
        ContainerAndHandler.eventQueue.offer(new OrderEvent(socketChannel,Integer.parseInt(split[2]),split[3],distributIdGenerate));
    }
}
