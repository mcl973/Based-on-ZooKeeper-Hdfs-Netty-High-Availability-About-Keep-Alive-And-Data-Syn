package Zookeeper.ZooKeeper_Master;

import Zookeeper.KeepAlivePacket.HandlerKeepAlive;
import Zookeeper.KeepAlivePacket.HelloPacketHandler;
import io.netty.channel.socket.SocketChannel;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.data.Stat;
import Zookeeper.ArgsAndPacket.ArgsInfo;
import Zookeeper.ArgsAndPacket.OrderEvent;
import Zookeeper.ArgsAndPacket.SynDataBetweenNettyServersPacket;
import Zookeeper.ZooKeeper_Nio.ExcutorService;
import Zookeeper.ZooKeeper_Nio.NettyServer2.NettyServer2;
import Zookeeper.zookeeper_hdfs.HandlerHdfs;
import Zookeeper.zookeeper_hdfs.ZooKeeperHdfs;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

//import java.nio.channels.SocketChannel;

public class ContainerAndHandler {
    public static BlockingQueue<OrderEvent> eventQueue = new LinkedBlockingQueue<>();
    private ThreadPoolExecutor poolExecutor;
    public static ZooKeeperHdfs zooKeeperHdfs;
    private static NettyServer2 nettyServer2 = null;
    public  static BlockingQueue<SynDataBetweenNettyServersPacket> synqueue = new LinkedBlockingQueue<>();
    public static long MaxSeq = -1L;

    public ContainerAndHandler() throws IOException {
//        得到线程
        poolExecutor = ExcutorService.getDefaultThreadPoolExcutor();
//        开启NettyServer2的线程
        nettyServer2 = new NettyServer2();
//        开启时将这个删掉，这个在后期会删掉，原因是client会保留sessionid，所以不会再创建sessionid，也就不会重写这个文件了。
        poolExecutor.submit(nettyServer2);
//        创建2个节点，这2个节点是用来通知
        poolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    DistributIdGenerate distributIdGenerate = new DistributIdGenerate();
//                    这个节点就是检测那些借点down了，以便通知各个client
                    distributIdGenerate.CreateNodeAndGetPersistentNodeName(ArgsInfo.ClientIsDown);
//                    这个节点时通知各个节点数据已经备份完毕可以进行下一步了
                    distributIdGenerate.CreateNodeAndGetPersistentNodeName(ArgsInfo.PhoneEachClient);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

//        连接hdfs
        zooKeeperHdfs = new ZooKeeperHdfs();
        zooKeeperHdfs.deletePath(ArgsInfo.NodeAliveList);

        /**
         * 1.总的处理线程 handler，处理各种事件
         * 2.SynInitNettyServer，NettyServer之间的服务器的热备的初始化
         * 3.SynDataBetweenNettyServers，实现NettyServer之间数据同步的线程
         * 4.处理NettyServer切换后的client重新连接的线程
         * 5.处理hello包的线程
         */
        poolExecutor.submit(new Handler());
        poolExecutor.submit(new SynInitNettyServer());
        poolExecutor.submit(new SynDataBetweenNettyServers());
//        为备份后的节点做准备
        poolExecutor.submit(new HandlerKeepAlive());
        poolExecutor.submit(new HelloPacketHandler());
//        poolExecutor.submit(new NioServer());
    }

    class Handler implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    //这里是单线程处理所以不会有线程安全的问题
                    OrderEvent take = eventQueue.take();
                    int order = take.getOrder();
                    SocketChannel socketChannel = take.getSocketChannel();
                    DistributIdGenerate distributIdGenerate = take.getDistributIdGenerate();
                    distributIdGenerate.setSocketChannel(socketChannel);
                    switch (order){
                        case ArgsInfo.CREATE :
                            System.out.println("远程的地址是："+socketChannel.remoteAddress().getHostString()+
                                    "状态码是："+order+"开始创建节点");
//                            创建节点
                            if (take.getData().equals("None"))
                                distributIdGenerate.CreateNodeAndGetNodeName(ArgsInfo.path);
                            else
                                distributIdGenerate.CreateNodeAndGetNodeName(take.getData());
                            long l = Long.parseLong(distributIdGenerate.getSeq());
//                            保存最大的seq
                            if (MaxSeq<l)
                                MaxSeq = l;
                            /**
                             * 每次创建对象都需要将这个distributIdGenerate和节点名称写入到hdfs中
                             */
                            HelloPacketHandler.helloQueue.offer(distributIdGenerate);
//                            保存distributIdGenerate对象数据
                            poolExecutor.submit(new HandlerHdfs(zooKeeperHdfs, ArgsInfo.SAVEOBJECT,
                                    ArgsInfo.HdfsSavePath+distributIdGenerate.getNodename(),distributIdGenerate));
                            String sessionId = distributIdGenerate.getSessionId();
                            poolExecutor.submit(new HandlerHdfs(zooKeeperHdfs,
                                    ArgsInfo.SAVESESSIONID,
                                    ArgsInfo.HdfsSavePath + sessionId, distributIdGenerate.getNodename()));
                            /**
                             *  需要所来控制数据的一致性不会被破坏
                             *  这里需要放在队列中，这样才可以保证数据的最终的稳定性
                             */
                            SynDataBetweenNettyServersPacket synDataBetweenNettyServersPacket = new SynDataBetweenNettyServersPacket(zooKeeperHdfs, distributIdGenerate);
                            synqueue.offer(synDataBetweenNettyServersPacket);
                            break;
                        case ArgsInfo.DELETE:
                            System.out.println("远程的地址是："+socketChannel.remoteAddress().getHostString()+
                                    "状态码是："+order+"开始删除节点");
                            if (take.getData().equals("None"))
                                distributIdGenerate.DeleteNodeByName(distributIdGenerate.getNodename());
                            else
                                distributIdGenerate.DeleteNodeByName(take.getData());

//                            还需要将其他的删掉如hdfs的数据节点
                            String nodename = distributIdGenerate.getNodename();
                            zooKeeperHdfs.deletePath(ArgsInfo.HdfsSavePath+nodename);
//                            清空相应的数据
                            distributIdGenerate.setNodename(null);
                            distributIdGenerate.setSeq(null);
                            break;
                        case ArgsInfo.MODIFY:
                            System.out.println("远程的地址是："+socketChannel.remoteAddress().getHostString()+
                                    "状态码是："+order+"开始修改节点");
                            Stat stat = null;
                            if (!take.getData().contains("$$$$$"))
                                stat = distributIdGenerate.ModifyNodeDataByName(distributIdGenerate.getNodename(), take.getData());
                            else {
                                String[] split = take.getData().split("$$$$$");
                                stat = distributIdGenerate.ModifyNodeDataByName(split[0], split[1]);
                            }
                            System.out.println(stat.toString());
                            break;
                        case ArgsInfo.SELECT:
                            if (take.getData().equals("None"))
                                distributIdGenerate.SelectNodeDataByName(distributIdGenerate.getNodename());
                            else distributIdGenerate.SelectNodeDataByName(take.getData());
                            break;
                        case ArgsInfo.WATCHNODE:
                            System.out.println("远程的地址是："+socketChannel.remoteAddress().getHostString()+
                                    "状态码是："+order+"对节点设置监听器，监听器的类型为 "+order);
                            if (take.getData().equals("None"))
                                distributIdGenerate.setNodeWatch(distributIdGenerate.getNodename());
                            else
                                distributIdGenerate.setNodeWatch(take.getData());
                            break;
                        case ArgsInfo.WATCHCHILDREN:
                            System.out.println("远程的地址是："+socketChannel.remoteAddress().getHostString()+
                                    "状态码是："+order+"对节点设置监听器，监听器的类型为 "+order);
                            if (take.getData().equals("None"))
                                distributIdGenerate.setChildrenWatch(distributIdGenerate.getNodename());
                            else distributIdGenerate.setChildrenWatch(take.getData());
                            break;
                        case ArgsInfo.WATCHTREE:
                            System.out.println("远程的地址是："+socketChannel.remoteAddress().getHostString()+
                                    "状态码是："+order+"对节点设置监听器，监听器的类型为 "+order);
                            if (take.getData().equals("None"))
                                distributIdGenerate.setTreeNodeWatch(distributIdGenerate.getNodename());
                            else distributIdGenerate.setTreeNodeWatch(take.getData());
                            break;
                        case ArgsInfo.SYNDATA:
//                            此代码为测试代码
                            if (distributIdGenerate.getSeq().equals(take.getData()))
                                HelloPacketHandler.helloQueue.offer(distributIdGenerate);
                            else{
                                distributIdGenerate.setSeq(take.getData());
                                HelloPacketHandler.helloQueue.offer(distributIdGenerate);
                            }
                            break;
                        case ArgsInfo.SAVESYNDATA:
//                            保存数据，先判断文件是否存在
                            boolean b = zooKeeperHdfs.insertStringData(ArgsInfo.HdfsSavePath + Long.parseLong(distributIdGenerate.getSeq()), take.getData(),true);
                            /**
                             *  要在这里触发一个监听器就是，最后的数据已经保存在了hdfs中
                             *
                             *  每一个client都可以接受这个数据包了，并将其持久化到本地的磁盘中去。
                             */


                            if(b)
                                socketChannel.writeAndFlush("数据已收到,已经存在了hdfs中");
                            else
                                socketChannel.writeAndFlush("失败，文件已存在或是异常抛出导致读写失败");
                            break;
                        default:break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

    /**
     *  同步不同的NettySerer之间的数据
     */
    class SynInitNettyServer implements Runnable{

        @Override
        public void run() {
            /**
             * 需要做的是
             * 1.建立一个永久节点
             * 2.注册事件通知
             * 3.拉取数据
             */
            while(true) {
                if (nettyServer2 != null) {
//                1.先获的存放对象的地方

                    boolean iswatched = false;
//                2.判断节点存不存在，如果不存在就创建
                    try {
                        DistributIdGenerate distributIdGenerate = new DistributIdGenerate();
                        boolean nodeExists = distributIdGenerate.isNodeExists(ArgsInfo.NettyServer_BackUp_Path);
                        boolean nodeExists1 = distributIdGenerate.isNodeExists(ArgsInfo.NettyServer_Syn_Ok);
                        if (!nodeExists) {
//                        创建节点
                            distributIdGenerate.CreateNodeAndGetPersistentNodeName(ArgsInfo.NettyServer_BackUp_Path);
//                        设置节点的监听器
                            distributIdGenerate.setNettyNodeWatch(ArgsInfo.NettyServer_BackUp_Path);
                        }else if(!nodeExists1){
                            distributIdGenerate.CreateNodeAndGetPersistentNodeName(ArgsInfo.NettyServer_Syn_Ok);
                            distributIdGenerate.setSynOkNodeWatch(ArgsInfo.NettyServer_Syn_Ok);
                        } else {
                            if (!iswatched) {
                                distributIdGenerate.setNettyNodeWatch(ArgsInfo.NettyServer_BackUp_Path);
                                distributIdGenerate.setSynOkNodeWatch(ArgsInfo.NettyServer_Syn_Ok);
                                iswatched = true;
                            }
//                3.拉取数据是在具体的监听器里
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                }
            }
        }
    }

    public static NettyServer2 getNettyServer2() {
        return nettyServer2;
    }

}
