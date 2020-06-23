package Zookeeper.ZooKeeper_Nio.NettyServerHa;


import java.util.function.Function;

public class AliveUntilDieFunction implements Function<MyFlag,Void> {
    @Override
    public Void apply(MyFlag myFlag) {
        /*
            除非手动杀掉或是故障挂掉否则不是是发给资源。
         */
        while (true){
            myFlag.isalive = true;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
