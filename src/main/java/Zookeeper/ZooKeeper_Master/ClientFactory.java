package Zookeeper.ZooKeeper_Master;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ClientFactory {
    static ExponentialBackoffRetry retry = new ExponentialBackoffRetry(100,3);
    public static CuratorFramework Curator_zookeeper_By_NewClient(String connectStringlist){
        /**
         * 设置超时时间和重试的次数
         */
//        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(100,3);
        return CuratorFrameworkFactory.newClient(connectStringlist,retry);
    }
    public static CuratorFramework Curator_zookeeper_By_Builder(String connectStringlist, RetryPolicy retryPolicy,
                                                                int connecttimeoutms,int sessiontimeout){
        return CuratorFrameworkFactory.builder().connectString(connectStringlist).retryPolicy(retryPolicy)
                .connectionTimeoutMs(connecttimeoutms).sessionTimeoutMs(sessiontimeout).retryPolicy(retry).build();
    }
}
