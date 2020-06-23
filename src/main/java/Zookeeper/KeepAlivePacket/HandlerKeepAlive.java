package Zookeeper.KeepAlivePacket;

import Zookeeper.ArgsAndPacket.ArgsInfo;
import Zookeeper.ZooKeeper_Master.ContainerAndHandler;
import Zookeeper.ZooKeeper_Nio.ExcutorService;
import Zookeeper.zookeeper_hdfs.ZooKeeperHdfs;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class HandlerKeepAlive implements Runnable{
    public static CopyOnWriteArraySet<String> NodeIsAlive = new CopyOnWriteArraySet<>();
    public static BlockingQueue<String> NodeAlive = new LinkedBlockingQueue<>();
    private ThreadPoolExecutor poolExecutor = ExcutorService.getDefaultThreadPoolExcutor();
    public HandlerKeepAlive(){
    }
    @Override
    public void run() {
        /**
         * 能够进入到这里的只能是主的NS挂掉了，所以需要进行会话的恢复
         * 如果在一个保活期限内没有收集完所有的数据，那么就表示没有收集到的节点需要被删除。
         * 1、开启计时器
         * 2、统计节点数
         * 3、开启一个计时器，用于处理没有下向备份的NS发送连接的client
         */
        ZooKeeperHdfs zooKeeperHdfs = ContainerAndHandler.zooKeeperHdfs;
        while (true) {
            String sid = null;
            try {
                sid = NodeAlive.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (HandlerKeepAlive.NodeIsAlive.size() == 0) {
                String stringdata = zooKeeperHdfs.getStringdata(ArgsInfo.NodeAliveList);
                System.out.println("000000000000000");
                System.out.println(stringdata);
                System.out.println("11111111111111111");
                String[] split1 = stringdata.split("\n");
                for (String s : split1) {
                    NodeIsAlive.add(s);
                }
//                           删除本节点
                NodeIsAlive.remove(sid);
//                           开始计时器
                poolExecutor.submit(new OneKeepTimeHandler());
            } else {
//                           删除本节点
                NodeIsAlive.remove(sid);
            }
        }
    }
}
