package Zookeeper.ZooKeeper_Master;

import Zookeeper.ArgsAndPacket.ArgsInfo;
import Zookeeper.ZooKeeper_Nio.NettyServer2.zookeeperhandler2;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.CharsetUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;

//import java.nio.channels.SocketChannel;

/**
 * 分布式id生成器
 * 此种方法的优点
 * 1.简单
 * 2.易验证,关于验证其实很好理解，可以使用节点的已加密的信息作为节点的数据，其他的节点获取后可以进行验证
 * 缺点是会造成zookeeper的负担较重。
 */
public class DistributIdGenerate implements Serializable {
//    为了序列化而设置的字段，不然会出现以下情况
    /**
     *java.io.InvalidClassException: com.*.*;   local class incompatible: stream classdesc serialVersionUID = 5590259895198052390, local class serialVersionUID = 7673969121092229700
     */
    private static final  long serialVersionUID = 1L;

    private transient CuratorFramework client;
    private String seq = "";
    private String nodename = "";
    private transient SocketChannel socketChannel;
    private String SessionId = "";
    private boolean isWatchSet = false;

    /**
     * 创建节点，输入节点名称
     * 创建的是一个永久的可序列化的节点
     * @param pathname
     * @return
     */
    public String CreateNodeAndGetNodeName(String pathname) {
        String nodename = null;
        if (client == null){
            reset();
        }
        try {
                nodename = client.create().creatingParentsIfNeeded().
                        withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(pathname);
                this.nodename = nodename;
                GetSeqFromNodeName(ArgsInfo.path, nodename);
                sendDataInNetty(getString("ndoename :" + nodename + "seq:" + seq));

        }catch (Exception e){
            e.printStackTrace();
        }
        return nodename;
    }

    public void CreateNodeAndGetPersistentNodeName(String pathname){
        if (client == null){
            reset();
        }
        try {
            nodename = client.create().creatingParentsIfNeeded().
                withMode(CreateMode.PERSISTENT).forPath(pathname);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 发送数据
     * @param socketChannel socketchannel
     * @param data  发送的数据
     */
    public void sendData(SocketChannel socketChannel,String data){
        ByteBuffer byteBuffer = ByteBuffer.allocate(ArgsInfo.ByteBufferSize);
        char[] chars = data.toCharArray();
        for (char aChar : chars) {
            byteBuffer.putChar(aChar);
        }
        byteBuffer.flip();
        try {
            socketChannel.write(byteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void sendDataInNetty(SocketChannel socketChannel,String data){
        socketChannel.writeAndFlush(data);
    }
    public void sendDataInNetty(String data){
        socketChannel.writeAndFlush(data);
    }

    public void sendData(String data){
        ByteBuffer byteBuffer = ByteBuffer.allocate(ArgsInfo.ByteBufferSize);
        char[] chars = data.toCharArray();
        for (char aChar : chars) {
            byteBuffer.putChar(aChar);
        }
        byteBuffer.flip();
        try {
            socketChannel.write(byteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void DeleteNodeByName(String nodename){
        if (client == null){
            reset();
        }
        try {
            client.delete().deletingChildrenIfNeeded().forPath(nodename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Stat ModifyNodeDataByName(String nodename,String data){
        Stat stat = null;
        if (client == null){
            reset();
        }
        try {
            stat = client.setData().forPath(nodename, data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stat;
    }

    /**
     *  查询数据
     * @param nodename  节点的名字
     */
    public void SelectNodeDataByName(String nodename)  {
        if (client == null){
            reset();
        }
        byte[] bytes = null;
        try {
            bytes = client.getData().forPath(nodename);
            sendDataInNetty(new String(bytes));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置监听器
     * @param nodename 节点名字
     */
    public void setNodeWatch(final String nodename){
        if (client == null){
            reset();
        }
        NodeCache nc = new NodeCache(client,nodename,false);
        setWatchSet(true);
        nc.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("节点发生了变化");
                Stat stat = client.checkExists().forPath(nodename);
                if(stat == null){
                    System.out.println("节点down掉了");
                    sendDataInNetty(getString(nodename+"  node is down"));
                }else{
                    System.out.println("节点的其他类型的改变");
                    byte[] bytes = client.getData().forPath(nodename);
                    sendDataInNetty(getString(nodename+" node changed,and data are "+getString(bytes)));
                }
            }
        });
        try {
            nc.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *  注册NettyServer的节点的监听器
     */
    public void setNettyNodeWatch(String nodename){
        if (client == null){
            reset();
        }
        NodeCache nc = new NodeCache(client,nodename,false);

            ConcurrentHashMap<String, DistributIdGenerate> sessionToDIG = zookeeperhandler2.getSessionToDIG();
            boolean b = sessionToDIG.containsKey(getSessionId());
            nc.getListenable().addListener(new MyNodeCacheListener(client, sessionToDIG, b));

        try {
            nc.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setSynOkNodeWatch(String nodename){
        if (client == null){
            reset();
        }
        NodeCache nc = new NodeCache(client,nodename,false);

            ConcurrentHashMap<String, DistributIdGenerate> sessionToDIG = zookeeperhandler2.getSessionToDIG();
            boolean b = sessionToDIG.containsKey(getSessionId());
            nc.getListenable().addListener(new MySynOkNodeCacheListener(b));

        try {
            nc.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置子节点的监听器
     * @param nodename
     */
    public void setChildrenWatch(final String nodename){
        if (client == null){
            reset();
        }

        PathChildrenCache pc = new PathChildrenCache(client,nodename,true);
        pc.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                List<String> strings;
                String string = "";
                switch(event.getType()){
                    case CHILD_REMOVED:
                        string = nodename + ",child removed,and left children are ";
                        break;
                    case CHILD_ADDED:
                        string = nodename+",child add";
                        break;
                    case CHILD_UPDATED:
                        string = nodename+",child add";
                        break;
                    default:
                        string = nodename+"other things happened";
                        break;
                }
                strings = client.getChildren().forPath(nodename);
                string = getString(strings.iterator(), string);
                sendDataInNetty(string);
            }
        });
    }

    public void setTreeNodeWatch(final String nodename){
        if (client == null){
            reset();
        }
        TreeCache tc = new TreeCache(client,nodename);
        tc.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                List<String> strings;
                String string = "";
                switch(event.getType()){
                    case NODE_REMOVED:
                        string = nodename + ",child removed,and left children are ";
                        break;
                    case NODE_ADDED:
                        string = nodename+",child add";
                        break;
                    case NODE_UPDATED:
                        string = nodename+",child add";
                        break;
                    default:
                        string = nodename+"other things happened";
                        break;
                }
                strings = client.getChildren().forPath(nodename);
                string = getString(strings.iterator(), string);
                sendDataInNetty(string);
            }
        });
    }

    /**
     * 从节点的名字中获取序号
     * @param pathname
     * @param nodename
     * @return
     */
    public void GetSeqFromNodeName(String pathname,String nodename){
        this.seq = nodename.split(pathname)[1];
    }

    public void reset(){
        client = ClientFactory.Curator_zookeeper_By_NewClient(ArgsInfo.connectString);
        client.start();
    }

    /**
     *  判断给定节点存不存在
     *  存在返回true，否则返回false；
     * @param pathname 节点名称
     * @return
     */
    public boolean isNodeExists(String pathname){
        boolean isexist = false;
        if (client == null)
            reset();
        try {
            Stat stat = client.checkExists().forPath(pathname);
            if (stat != null)
                isexist = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isexist;
    }

    /**
     *  获取当前节点的状态
     */
    public Stat getStat(String pathname){
        Stat stat = null;
        if (client == null)
            reset();
        try {
            stat = client.checkExists().forPath(ArgsInfo.NettyServer_BackUp_Path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stat;
    }

    /**
     * 获取分布式锁
     * @param path  需要抢占的节点
     * @return
     */
    public InterProcessMutex getDistributeLock(String path){
        if (client == null)
            reset();
        return new InterProcessMutex(client, path);
    }


    /**
     *
     * @throws IOException
     */
    public DistributIdGenerate() throws IOException {
//        GenerateId();
        reset();
    }

//    public void GenerateId() throws IOException {
//        String pathname = ArgsInfo.path;
//        String nodename = CreateNodeAndGetNodeName(pathname);
////        SaveCurrentName(nodename);
//        this.seq = GetSeqFromNodeName(pathname, nodename);
//    }

//    /**
//     *  保存数据
//     * @param name
//     * @throws IOException
//     */
//    public void SaveCurrentName(String name) throws IOException {
//        File file = new File(Arges.path_ser);
//        if (!file.exists())
//            file.createNewFile();
//        FileWriter fw = new FileWriter(file);
//        fw.write(name);
//        fw.flush();
//        fw.close();
//    }

    public String getSeq() {
        return seq;
    }

    public void setSeq(String seq) {
        this.seq = seq;
    }

    public String getNodename() {
        return nodename;
    }

    public void setNodename(String nodename) {
        this.nodename = nodename;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public String getString(String string){
        return new String(string.getBytes(),CharsetUtil.UTF_8);
    }
    public String getString(byte[] bytes){
        return new String(bytes,CharsetUtil.UTF_8);
    }
    public String getString(Iterator<String> iterator,String startdata){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(startdata);
        while (iterator.hasNext()){
            stringBuilder.append(iterator.next()).append("\n");
        }
        return stringBuilder.toString();
    }

    public String getSessionId() {
        return SessionId;
    }

    public void setSessionId(String sessionId) {
        SessionId = sessionId;
    }

    public boolean isWatchSet() {
        return isWatchSet;
    }

    public void setWatchSet(boolean watchSet) {
        isWatchSet = watchSet;
    }
}
