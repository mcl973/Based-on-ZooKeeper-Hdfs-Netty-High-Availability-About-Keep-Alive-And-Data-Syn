package Zookeeper.zookeeper_hdfs;

import Zookeeper.ArgsAndPacket.ArgsInfo;

import java.util.concurrent.Callable;

public class HandlerHdfs implements Callable {
    private ZooKeeperHdfs zooKeeperHdfs;
    private int code = 0;
    private String path;
    private Object object;
    private String data;

    public HandlerHdfs(ZooKeeperHdfs zooKeeperHdfs, int code, String path){
        this.zooKeeperHdfs = zooKeeperHdfs;
        this.code = code;
        this.path = path;
    }
    public HandlerHdfs(ZooKeeperHdfs zooKeeperHdfs, int code, String path, Object object){
        this.zooKeeperHdfs = zooKeeperHdfs;
        this.code = code;
        this.path = path;
        this.object = object;
    }
    public HandlerHdfs(ZooKeeperHdfs zooKeeperHdfs, int code, String path, String data){
        this.zooKeeperHdfs = zooKeeperHdfs;
        this.code = code;
        this.path = path;
        this.data = data;
    }

    @Override
    public Object call() throws Exception {
        Object object = null;
        switch (code){
            case ArgsInfo.GETOBJECT:
                object = zooKeeperHdfs.getObject(path);
                break;
            case ArgsInfo.SAVEOBJECT:
                zooKeeperHdfs.insertData(path,this.object);
                break;
            case ArgsInfo.SAVESESSIONID:
                zooKeeperHdfs.insertStringData(path,data);
            case ArgsInfo.GETSESSIONID:
                zooKeeperHdfs.getStringdata(path);
            default:break;
        }
        return object;
    }
}
