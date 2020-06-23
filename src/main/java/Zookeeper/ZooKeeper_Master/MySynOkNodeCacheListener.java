package Zookeeper.ZooKeeper_Master;

import org.apache.curator.framework.recipes.cache.NodeCacheListener;


public class MySynOkNodeCacheListener implements NodeCacheListener {
    private boolean isme ;

    public MySynOkNodeCacheListener(boolean isme){
        this.isme = isme;
    }
    @Override
    public void nodeChanged() throws Exception {
		// 先清除之前的数据，防止waitAndUpQueue达到上限而阻塞
		SynDataBetweenNettyServers.waitAndUpQueue.clear();
		//放置数据，以通知其他的线程。
        SynDataBetweenNettyServers.waitAndUpQueue.offer(1);
    }
}
