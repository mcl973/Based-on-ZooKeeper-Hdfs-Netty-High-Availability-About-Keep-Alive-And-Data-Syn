package Zookeeper.ZooKeeper_Nio.NettyServer2;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import Zookeeper.ArgsAndPacket.ArgsInfo;

public class NettyServer2 implements Runnable{
    private EventLoopGroup boss,worker;
    private int i;
    public zookeeperhandler2 zh2 = new zookeeperhandler2();
    private void init(){
        boss = new NioEventLoopGroup();
        worker = new NioEventLoopGroup();
    }
    public void config() throws InterruptedException {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boss,worker).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {

                ch.pipeline()
                        .addLast(new StringDecoder())  //添加加码器
                        .addLast(new StringEncoder())  //添加解码器
                        .addLast(new zookeeperhandler2());
            }
        });
        ChannelFuture future = serverBootstrap.bind(ArgsInfo.hostname, ArgsInfo.port).sync();
        future.channel().closeFuture().sync();
    }

    public void congig2() throws InterruptedException {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boss,worker).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {

                ch.pipeline()
                        .addLast(new StringDecoder())  //添加加码器
                        .addLast(new StringEncoder())  //添加解码器
                        .addLast(zh2);
            }
        });
        ChannelFuture future = serverBootstrap.bind(ArgsInfo.hostname, ArgsInfo.port2).sync();
        future.channel().closeFuture().sync();
    }

    public NettyServer2(){

    }

    @Override
    public void run() {
        init();
        try {
                congig2();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
