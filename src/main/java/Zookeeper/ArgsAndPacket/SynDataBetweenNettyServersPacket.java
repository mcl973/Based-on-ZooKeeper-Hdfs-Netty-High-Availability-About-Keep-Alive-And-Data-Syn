package Zookeeper.ArgsAndPacket;

import Zookeeper.ZooKeeper_Master.DistributIdGenerate;
import Zookeeper.zookeeper_hdfs.ZooKeeperHdfs;

public class SynDataBetweenNettyServersPacket {
    private ZooKeeperHdfs zooKeeperHdfs;
    private DistributIdGenerate distributIdGenerate;
    public SynDataBetweenNettyServersPacket(ZooKeeperHdfs zooKeeperHdfs, DistributIdGenerate distributIdGenerate){
        this.zooKeeperHdfs = zooKeeperHdfs;
        this.distributIdGenerate = distributIdGenerate;
    }

    public ZooKeeperHdfs getZooKeeperHdfs() {
        return zooKeeperHdfs;
    }


    public DistributIdGenerate getDistributIdGenerate() {
        return distributIdGenerate;
    }
}
