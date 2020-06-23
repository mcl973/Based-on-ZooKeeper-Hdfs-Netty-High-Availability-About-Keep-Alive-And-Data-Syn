package Zookeeper.KeepAlivePacket;

import Zookeeper.ArgsAndPacket.ArgsInfo;
import Zookeeper.ZooKeeper_Nio.NettyServer2.zookeeperhandler2;

import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArraySet;

public class OneKeepTimeHandler implements Runnable{

    @Override
    public void run() {
        int keeptime = ArgsInfo.keeptime;
        while (keeptime>0){
            try {
                Thread.sleep(1000);
                keeptime--;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("当前的NodeIsAlive剩余的数量为"+HandlerKeepAlive.NodeIsAlive.size());
//        超时处理
        if(HandlerKeepAlive.NodeIsAlive.size()>0){
//                处理节点的删除业务
            Iterator<String> iterator = HandlerKeepAlive.NodeIsAlive.iterator();
//            开始挨个的的删除节点
            while (iterator.hasNext()){
                String sid = iterator.next();
//                通过sid拿去对应的数据
                zookeeperhandler2.deletelinkandall(null,sid);
            }

        }
    }
}
