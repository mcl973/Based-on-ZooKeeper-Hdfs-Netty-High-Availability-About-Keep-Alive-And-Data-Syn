package Zookeeper.ZooKeeper_Master;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.data.Stat;
import Zookeeper.ArgsAndPacket.ArgsInfo;
import Zookeeper.zookeeper_hdfs.ZooKeeperHdfs;

import java.util.concurrent.ConcurrentHashMap;

/**     存放会话id和DistributIdGenerate的容器
 *     private ConcurrentHashMap<String,DistributIdGenerate> sessionToDIG;
 *     这个字段说明自己是不是该处理事件，如果这个事件是自己出发的，那么不用处理
 *     是自己为true，不是自己为false
 *     private boolean isme;
 *     连接zookeeper的客户端
 *     private CuratorFramework client;
 */
public class MyNodeCacheListener implements NodeCacheListener {
    private ConcurrentHashMap<String, DistributIdGenerate> sessionToDIG;
    private boolean isme;
    private CuratorFramework client;
    public MyNodeCacheListener(CuratorFramework client, ConcurrentHashMap<String, DistributIdGenerate> sessionToDIG, boolean isme){
        this.client = client;
        this.sessionToDIG = sessionToDIG;
        this.isme = isme;
    }

    @Override
    public void nodeChanged() throws Exception {
        System.out.println("节点发生了变化");
        Stat stat = client.checkExists().forPath(ArgsInfo.NettyServer_BackUp_Path);
        InterProcessMutex lock = new InterProcessMutex(client, ArgsInfo.NettyServer_BackUp_Path);
        if(stat == null){
            System.out.println("远程节点被删除了");
        }else{
            System.out.println("////////////////////");
            ZooKeeperHdfs zooKeeperHdfs = ContainerAndHandler.zooKeeperHdfs;
//            首先获取hdfs上的sessionid，看看本地有没有具体的条目

            if (!isme) {
                try {
//                分布式锁
//                lock.acquire();
                    /**
                     *  由于分布式锁的存在所以在这里读写是安全的
                     *  素以写完后可以将其删掉
                     */
//              1.读取元数据，即获得是哪一个对象被创建了
                    String stringdata = zooKeeperHdfs.getStringdata(ArgsInfo.NewObjectGeneratePath);
                    if(stringdata != null) {
//              2.读取对象
                        DistributIdGenerate distributIdGenerate = (DistributIdGenerate) zooKeeperHdfs.getObject(ArgsInfo.HdfsSavePath + stringdata);
//                        删除数据有没有数据的节点来做
                        if(!sessionToDIG.containsKey(distributIdGenerate.getSessionId())) {
//              3.将其放入容器中,这里不能添加
//                            sessionToDIG.put(distributIdGenerate.getSessionId(), distributIdGenerate);
//              4.删除数据,卧槽数据居然被我请了
                            zooKeeperHdfs.clearPathData(ArgsInfo.NewObjectGeneratePath);
//              5.还需要同步的是一个在线列表，这个在线列表放置在hdfs中。
                            
//                解锁SynDataBetweenNettyServers的run方法使其可以继续运行下去
                            distributIdGenerate.ModifyNodeDataByName(ArgsInfo.NettyServer_Syn_Ok, "数据拉取完毕");
//              6.处理遗留问题，如监听器的注册
                            boolean watchSet = distributIdGenerate.isWatchSet();
                            if (watchSet){
                                distributIdGenerate.setNodeWatch(distributIdGenerate.getNodename());
                            }
//                lock.release();
                        }
                        System.out.println(stringdata);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
}
