package Zookeeper.ZooKeeper_Nio.NettyServerHa;

import Zookeeper.ArgsAndPacket.ArgsInfo;
import Zookeeper.ZooKeeper_Master.DistributIdGenerate;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.util.function.Function;

/**
 *  选举主节点，当主节点挂掉后或是删除zookeeper节点即放弃成为主节点后争抢成为主节点。
 */
public class LeaderSelector {
    private DistributIdGenerate distributIdGenerate = new DistributIdGenerate();
    private String path;
    private PathChildrenCache pcc = null;
    public LeaderSelector(CuratorFramework client, String path) throws IOException {
        this.distributIdGenerate.setClient(client);
        this.distributIdGenerate.setNodename(System.currentTimeMillis()+"");
        this.path = path;
    }

    /**
     *  具体的执行的函数，传入一个function和一个object，function就是具体的业务逻辑，object是function的参数
     * @param function
     * @param myFlag
     */
    public void Actions(Function<MyFlag,Void> function,MyFlag myFlag){
        try {
            /**
             * 所有的操作在这里执行
             */
            function.apply(myFlag);
            System.out.println("我退出了");
//            重置状态
            myFlag.isalive = false;
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            distributIdGenerate.DeleteNodeByName(ArgsInfo.leaderpathnode);
        }
    }

    public void AutoSelect(Function<MyFlag,Void> function,MyFlag myFlag) throws Exception {
        final InterProcessMutex lock = distributIdGenerate.getDistributeLock(ArgsInfo.leaderpath);
//        if (pcc == null) {
//            pcc = new PathChildrenCache(distributIdGenerate.getClient(), ArgsInfo.leaderpath, true);
//            pcc.getListenable().addListener(new PathChildrenCacheListener() {
//                @Override
//                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
//                    byte[] bytes = client.getData().forPath(ArgsInfo.leaderpathnode);
//                    String data = new String(bytes);
//                    String s = new String(event.getData().getData());
//                    if (!data.equals(s)) {
//                        if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
//                            /**
//                             * 开始处理获取所得业务
//                             * 节点被删除了，表示主节点放弃了master的位置。
//                             * 所以我开始创建数据
//                             */
//                            System.out.println("/////////////////////");
//                            /**
//                             * 防止重复执行带来的开销，其实都是一样的，反正都是只有本地才能够执行
//                             * 但是还是会出现问题
//                             * 如上下都判断当前的路径为空，那么都会去创建，这是就会出现抢的时间，为了更高的效率还是设置一个锁机制
//                             * 虽然不知道zookeeper对于同一个节点的多个同时的创建节点操作是如何做的
//                             * 但是大致可以判断出来还是要按照事务id来执行的，所以还是需要进行抢夺锁的，所以尽量
//                             * 将锁的抢夺放置到本地。
//                             */
//                            lock.acquire();
//                            if (!distributIdGenerate.isNodeExists(ArgsInfo.leaderpathnode)) {
//                                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ArgsInfo.leaderpathnode);
//                                distributIdGenerate.ModifyNodeDataByName(ArgsInfo.leaderpathnode,distributIdGenerate.getNodename());
//                                Actions(function, myFlag);
//                            }
//                            lock.release();
//                        }
//                    }
//                }
//            });
//            pcc.start();
//        }
        while (true) {
            /**
             * 这里是一开始的创建入口和主节点down掉后的入口
             * 如果同一个节点，并且是这个节点执行了删除操作，那么上面是不会执行的，
             * 那么下面与可能会继续会抢到锁，所以不一定主节点放弃了锁就一定在在这次中抢不到锁。当然这种情况会很少发生。
             */
            System.out.println("开始抢锁");
//            没有抢到锁会卡在这里，一直等待。
            lock.acquire();
            System.out.println("抢到锁了");
            Actions(function,myFlag);
//            if (!distributIdGenerate.isNodeExists(ArgsInfo.leaderpathnode)) {
//                distributIdGenerate.getClient().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ArgsInfo.leaderpathnode);
//                distributIdGenerate.ModifyNodeDataByName(ArgsInfo.leaderpathnode,distributIdGenerate.getNodename());
//
//            }
            lock.release();
        }
    }
}
