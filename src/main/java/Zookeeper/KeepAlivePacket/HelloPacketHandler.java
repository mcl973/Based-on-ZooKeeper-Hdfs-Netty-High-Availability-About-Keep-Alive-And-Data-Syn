package Zookeeper.KeepAlivePacket;

import Zookeeper.ArgsAndPacket.ArgsInfo;
import Zookeeper.ArgsAndPacket.Pakcet;
import Zookeeper.ZooKeeper_Master.ContainerAndHandler;
import Zookeeper.ZooKeeper_Master.DistributIdGenerate;
import Zookeeper.zookeeper_hdfs.ZooKeeperHdfs;
import io.netty.channel.socket.SocketChannel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class HelloPacketHandler implements Runnable{
    public static BlockingQueue<DistributIdGenerate> helloQueue = new LinkedBlockingQueue<>();
    ZooKeeperHdfs zooKeeperHdfs = ContainerAndHandler.zooKeeperHdfs;
    private static ConcurrentHashMap<SocketChannel,String> map = new ConcurrentHashMap<>();

    @Override
    public void run() {
        while (true){
            try {
                DistributIdGenerate take = helloQueue.take();
                long sessionId = Long.parseLong(take.getSessionId());
//                开始比对，如果seq比当前的seq小那么就开始数据同步。
                /**
                 * 这里其实有一个比较难以处理的地方是：
                 *  正在处理这个hello包的时候有可能hello包中的
                 *  seq已经不是最新的了，但是在发送这个hello包的节点处其实seq已经更新了，但是由于
                 *  hello数据包已经发送了，所以ns端无法判断当前的这个seq是不是最新的
                 *  但是这个只是特殊的情况，所以这里不管即
                 *  只要当前的seq比我的seq小那么我就向你发送数据
                 *  这里还有一个需要确认的是如果第二次发现同一个节点发送了和上次一样的且不是最新的seq，
                 *  那么就认为这个节点是一个恶意节点。
                 */
//                先获取，防止数据突然的变化
                long maxSeq = ContainerAndHandler.MaxSeq;

                if (sessionId<maxSeq){
//                  开始同步的部分
                    if (map.containsKey(take.getSocketChannel())) {
                        if (map.get(take.getSocketChannel()).equals(take.getSessionId())) {
                            take.getSocketChannel().writeAndFlush("你出现了重复的seq，需要将你down掉");
//                            这里还需要通知其他的client，告知出现了恶意节点。
                            /**
                             *  使用事件触发来实现
                             */

                        }
                        else
                            map.put(take.getSocketChannel(), take.getSessionId());
                    }else
                        map.put(take.getSocketChannel(), take.getSessionId());
                    /**
                     * 从[sessionId+1,maxseq]这个区间的数据都要被同步到这个节点中
                     */
                    long startid = sessionId+1;
                    synDataToClient(take,startid,maxSeq);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public void synDataToClient(DistributIdGenerate distributIdGenerate,long startid,long maxSeq){
        SocketChannel socketChannel = distributIdGenerate.getSocketChannel();
        for(;startid<=maxSeq;startid++) {
//      具体的数据应该是nodename+data
            String stringdata = zooKeeperHdfs.getStringdata(ArgsInfo.HdfsSavePath + startid);
//      获取了数据那么就开始同步了
            SocketChannel socketChannel1 = distributIdGenerate.getSocketChannel();
            Pakcet pakcet = new Pakcet(socketChannel1.localAddress().getHostName(),
                    ArgsInfo.SYNDATA,stringdata,distributIdGenerate.getSessionId());
            socketChannel.writeAndFlush(pakcet.toString()+"\n");
        }

    }
}
