package Zookeeper.ZooKeeper_Nio.NettyServer2;

import Zookeeper.KeepAlivePacket.HandlerKeepAlive;
import Zookeeper.KeepAlivePacket.OneKeepTimeHandler;
import Zookeeper.ZooKeeper_Master.DistributIdGenerate;
import Zookeeper.ZooKeeper_Nio.NettyServerHa.AliveUntilDieFunction;
import Zookeeper.ZooKeeper_Nio.NettyServerHa.LeaderSelector;
import Zookeeper.ZooKeeper_Nio.NettyServerHa.MyFlag;
import Zookeeper.zookeeper_hdfs.HandlerHdfs;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import Zookeeper.ArgsAndPacket.ArgsInfo;
import Zookeeper.ArgsAndPacket.HandlePacket;
import Zookeeper.ArgsAndPacket.Pakcet;
import Zookeeper.ZooKeeper_Master.ContainerAndHandler;
import Zookeeper.ZooKeeper_Nio.ExcutorService;
import Zookeeper.zookeeper_hdfs.ZooKeeperHdfs;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import javax.xml.soap.Node;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class zookeeperhandler2 extends SimpleChannelInboundHandler<String> {
    private static ThreadPoolExecutor poolExecutor = ExcutorService.getDefaultThreadPoolExcutor();
    private static ConcurrentHashMap<SocketChannel, DistributIdGenerate> channelToDIG = new ConcurrentHashMap<>();
    public  static ConcurrentHashMap<String, DistributIdGenerate> sessionToDIG = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String,String> sessionid_Correct = new ConcurrentHashMap<>();
    private static AtomicLong sessionid = new AtomicLong();
//    是否为主节点
    private static MyFlag myFlag = null;
//    private static AtomicInteger hosts = new AtomicInteger();
    static {
    /**
     * 开启master的抢夺，一旦抢上master，那么就不会放开，除非节点挂掉
     */
    try {
            myFlag = new MyFlag();
            DistributIdGenerate temp = new DistributIdGenerate();
            //获取msater抢夺器
            LeaderSelector ls = new LeaderSelector(temp.getClient(), ArgsInfo.leaderpath);
            poolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        //开始抢锁并随时监听节点的状态如果分布式锁被释放那么就抢夺锁
                        ls.AutoSelect(new AliveUntilDieFunction(), myFlag);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch(Exception e){
            e.printStackTrace();
        }
    }
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        if (!myFlag.isalive){
            Channel channel = ctx.channel();
            channel.writeAndFlush("100");
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        poolExecutor.submit(new DeleteLinkAndAboutAllOfIt((SocketChannel)ctx.channel()));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        poolExecutor.submit(new SimpleHandler(msg,(SocketChannel) ctx.channel()));
    }

    class SimpleHandler implements Runnable{
        private String msg;
        private SocketChannel socketChannel;
        public SimpleHandler(String msg,SocketChannel socketChannel){
            this.msg = msg;
            this.socketChannel = socketChannel;
        }

        @Override
        public void run() {
            /**
             *  先判断有没有会话id，如果没有那就生成一个新的。
             *  如果有那么就对比其验证码，这个验证码是每一个client的私钥对会话id加密后的数据
             *  由于默认集群是一个安全的操作，所以client不会得到其他的节点的验证码
             *  验证码被保存在hdfs中，只有nettyServer有访问的权限，且只在这个时候访问，且是在本地没有验证码的情况下访问。
             */
            /*
                如果当前的装他是false的那么表示没有获取锁，那么就等待
                如果收到了数据，那么就表示当前的服务器不可用。
                知道这个节点成为master。
             */
            while (!myFlag.isalive){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            String[] split = msg.split("#####");
            String sid = split[split.length-2];
            String correct_seq = split[split.length-1];
            DistributIdGenerate distributIdGenerate = null;
            ZooKeeperHdfs zooKeeperHdfs = ContainerAndHandler.zooKeeperHdfs;

//        1.有无sessionid
            if (sid.equals("None")){
                /**
                 * 如果sessionid为空，那么生成一个会话id，并将当前的distributIdGenerate注入到channelToDIG中
                 */
                try {
                    distributIdGenerate = new DistributIdGenerate();
                    long andIncrement = sessionid.getAndIncrement();
                    sessionToDIG.put(andIncrement+"",distributIdGenerate);
                    distributIdGenerate.setSessionId(andIncrement+"");
                    distributIdGenerate.setSocketChannel(socketChannel);
                    channelToDIG.put(socketChannel,distributIdGenerate);
                    Pakcet p = new Pakcet(socketChannel.localAddress().getHostName(),-1,andIncrement+"");
                    socketChannel.writeAndFlush(p.toString());
                    InterProcessMutex distributeLock = distributIdGenerate.getDistributeLock(ArgsInfo.NettyServer_BackUp_Path);
                    distributeLock.acquire();
//                    这里存放的是session id
                    zooKeeperHdfs.appendStringData(ArgsInfo.NodeAliveList,andIncrement+"");
                    distributeLock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else{
//            2.如果有的话那么就对比其验证码
                String local_correct_seq = null;
                if (sessionid_Correct.containsKey(sid))
                    local_correct_seq = sessionid_Correct.get(sid);
                else{
                    /**
                     * 如果本地不存在correct_seq那么就去hdfs中查找
                     * 如果没有那么在进行添加
                     */
                    local_correct_seq = zooKeeperHdfs.getStringdata(ArgsInfo.correct_seq_path + sid);
                    if (local_correct_seq == null) {
                        local_correct_seq = correct_seq;
                        sessionid_Correct.put(sid, correct_seq);
                        zooKeeperHdfs.insertStringData(ArgsInfo.correct_seq_path + sid, correct_seq);
                    }else
                        sessionid_Correct.put(sid, correct_seq);
                }
//            3.判断过来的验证码存不存在，如果存在是不是和本地的验证码是不是一样的。
                if (local_correct_seq.trim().equals(correct_seq)){
//            4.因为是一样的，所以先看看这个socket在不在channelToDIG中
                    if (channelToDIG.containsKey(socketChannel)){
                        distributIdGenerate = channelToDIG.get(socketChannel);
                    }else if(sessionToDIG.containsKey(sid)){
//                        通过sessionid拿取对应的数据
                        distributIdGenerate = sessionToDIG.get(sid);
                        distributIdGenerate.setSessionId(sid);
                        distributIdGenerate.setSocketChannel(socketChannel);
                        channelToDIG.put(socketChannel,distributIdGenerate);
                    }else{
                        /**
                         * 本地没有distributIdGenerate，那么就去hdfs中找
                         * 1.先通过sessionid找到具体的nodename
                         * 2.再通过nodename和存储路径拼接就可以得到distributIdGenerate对象的存储路径
                         */
                        String nodename = zooKeeperHdfs.getStringdata(ArgsInfo.HdfsSavePath + sid);
                        distributIdGenerate = (DistributIdGenerate)zooKeeperHdfs.getObject(ArgsInfo.HdfsSavePath + nodename.trim());
                        channelToDIG.put(socketChannel,distributIdGenerate);
                        sessionToDIG.put(sid,distributIdGenerate);
                        HandlerKeepAlive.NodeAlive.offer(sid);
                    }
//            5.如果一样那么就执行具体的事务流程，如将其封装成HandlePacket
                    poolExecutor.submit(new HandlePacket(msg, socketChannel, distributIdGenerate));
                }else {
                    socketChannel.writeAndFlush("correct_seq not equals");
                }
            }
        }
    }

    class DeleteLinkAndAboutAllOfIt implements Runnable{
        public SocketChannel socketChannel;
        public DeleteLinkAndAboutAllOfIt(SocketChannel socketChannel){
            this.socketChannel =socketChannel;
        }

        @Override
        public void run() {
//            这里主要依靠socketchannel来删除
            deletelinkandall(socketChannel,null);
        }
    }

    public static void deletelinkandall(SocketChannel socketChannel,String sessionid){
        /**
         * 在这里监控的是client的连接断开，删除一切与这个session有关的东西。
         * 1、首先提取sessionid
         * 2、清除channelToDIG,sessionToDIG,channelToDIG中对应的数据
         * 3、清除hdfs中的数据
         *         1.通过path+session找到hdfs中存放这个sessionid对应的节点
         *         2.通过path+节点名称删除hdfs节点
         *         3.删除hdfs会话节点
         *         4.删除zookeeper节点
         */
        DistributIdGenerate distributIdGenerate = null;
        if(socketChannel != null) {
            channelToDIG.remove(socketChannel);
//            提取DistributIdGenerate
            distributIdGenerate = channelToDIG.get(socketChannel);
        }else if(sessionid!=null){
            if(sessionToDIG.containsKey(sessionid))
                distributIdGenerate = sessionToDIG.get(sessionid);
            else
                return;
        }
//            此时还有一个需要删除的就是存活节点列表的中具体的数据，这放置到其他的地方去处理
//            使用分布式锁来保证数据的一致性
        InterProcessMutex distributeLock = distributIdGenerate.getDistributeLock(ArgsInfo.NettyServer_BackUp_Path);
//        获取hdfs
        ZooKeeperHdfs zooKeeperHdfs = ContainerAndHandler.zooKeeperHdfs;
        try {
            distributeLock.acquire();
//                开始删除
            String stringdata = zooKeeperHdfs.getStringdata(ArgsInfo.NodeAliveList);
            //将hostname数据删掉
            String replace = stringdata.replace(stringdata + "\n", "");
//                将数据在写会hdfs
            zooKeeperHdfs.insertStringData(ArgsInfo.NodeAliveList,replace);
            distributeLock.release();
        } catch (Exception e) {
            e.printStackTrace();
        }
//            提取sessionid
        String sessionId = distributIdGenerate.getSessionId();
//            删除在本地容器中的关于这个sessionid的数据
        sessionToDIG.remove(sessionId);
        sessionid_Correct.remove(sessionId);
//        查找nodename
        String stringdata = zooKeeperHdfs.getStringdata(ArgsInfo.HdfsSavePath + sessionId);
//        开始删除
        zooKeeperHdfs.deletePath(ArgsInfo.HdfsSavePath+stringdata);
        zooKeeperHdfs.deletePath(ArgsInfo.HdfsSavePath + sessionId);
//        删除zookeeper的对应的节点
        distributIdGenerate.DeleteNodeByName(stringdata);
        /**
         * 这里该需要通知其他的client，某个clientdown掉了。
         */
        distributIdGenerate.ModifyNodeDataByName(ArgsInfo.ClientIsDown,socketChannel.remoteAddress().getHostName()+"挂掉了了");

    }
    public static ConcurrentHashMap<String, DistributIdGenerate> getSessionToDIG() {
        return sessionToDIG;
    }

    public static void setSessionToDIG(ConcurrentHashMap<String, DistributIdGenerate> sessionToDIG) {
        zookeeperhandler2.sessionToDIG = sessionToDIG;
    }
}
