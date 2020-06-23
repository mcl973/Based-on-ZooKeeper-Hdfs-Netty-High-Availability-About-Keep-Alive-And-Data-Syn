package Zookeeper.ZooKeeper_Master;

import Zookeeper.ArgsAndPacket.ArgsInfo;
import Zookeeper.ArgsAndPacket.SynDataBetweenNettyServersPacket;
import Zookeeper.zookeeper_hdfs.ZooKeeperHdfs;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;


/**
 *           |||||||||||||||||||||||||||||
 *  插入--->| 数据n,......数据2,数据1   |--->拿去
 *          ||||||||||||||||||||||||||||
 *
 *          拿到的数据-->处理，触发监听器-->处理监听器的中的
 *
 *
 */
public class SynDataBetweenNettyServers implements Runnable{
    public static BlockingQueue<Integer> waitAndUpQueue = new ArrayBlockingQueue<>(2);
    public static BlockingQueue<Integer> synwaitqueue = new SynchronousQueue<>();
    @Override
    public void run() {
        while (true){
            try {
                SynDataBetweenNettyServersPacket take = ContainerAndHandler.synqueue.take();
                waitAndUpQueue.clear();
                DistributIdGenerate distributIdGenerate = take.getDistributIdGenerate();
                ZooKeeperHdfs zooKeeperHdfs = take.getZooKeeperHdfs();
                zooKeeperHdfs.insertStringData(ArgsInfo.NewObjectGeneratePath,distributIdGenerate.getNodename());
                distributIdGenerate.ModifyNodeDataByName(ArgsInfo.NettyServer_BackUp_Path,"Object Add");
                waitAndUpQueue.take();
                System.out.println("###############");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
