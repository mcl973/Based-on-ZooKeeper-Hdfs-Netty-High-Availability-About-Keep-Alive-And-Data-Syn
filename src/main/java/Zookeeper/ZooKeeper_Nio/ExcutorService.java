package Zookeeper.ZooKeeper_Nio;

import Zookeeper.ArgsAndPacket.ArgsInfo;

import java.util.concurrent.*;

/**
 * 线程池
 */
public class ExcutorService {
    public static ThreadPoolExecutor getDefaultThreadPoolExcutor(){
        return new ThreadPoolExecutor(ArgsInfo.Default_Thread_Number,
                                      ArgsInfo.Default_Thread_Number,
                                      10,
                                      TimeUnit.SECONDS,
                                      new ArrayBlockingQueue<Runnable>(ArgsInfo.Default_Thread_Number));
    }
    public static ThreadPoolExecutor getMySelfThreadPoolExecutor(int coreThreadSize, int maxYhreadSize, int KeepTime, TimeUnit unit,
                                                                 BlockingQueue<Runnable> waitingQueue,
                                                                 ThreadFactory threadFactory,
                                                                 RejectedExecutionHandler rejectedExecutionHandler){
        return new ThreadPoolExecutor(coreThreadSize,maxYhreadSize,KeepTime,unit,waitingQueue,threadFactory,rejectedExecutionHandler);
    }
}
