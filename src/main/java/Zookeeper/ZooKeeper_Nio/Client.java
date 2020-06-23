package Zookeeper.ZooKeeper_Nio;

import Zookeeper.ArgsAndPacket.Pakcet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;

/**
 * 测试使用的类，使用的是bio
 */
public class Client {
    public static void main(String[] args) throws IOException {

        Socket socket = new Socket("localhost",8888);
        OutputStream outputStream = socket.getOutputStream();
        new receive(socket.getInputStream()).start();
        Pakcet localhost = new Pakcet("localhost", 0);
        outputStream.write(localhost.toString().getBytes());
        outputStream.flush();
        Scanner scanner = new Scanner(System.in);
        String s = scanner.nextLine();
        while (!s.equals("eof")){
            String[] split = s.split("#####");
            Pakcet pakcet = null;
            if (split.length==2)
                pakcet = new Pakcet(split[0],Integer.parseInt(split[1].trim()));
            else if(split.length == 3)
                pakcet = new Pakcet(split[0],Integer.parseInt(split[1].trim()),split[2]);
            outputStream.write(pakcet.toString().getBytes());
            outputStream.flush();
            s = scanner.nextLine();
        }
    }
    static class receive extends Thread{
        public InputStream inputStream;
        public receive(InputStream inputStream){
            this.inputStream = inputStream;
        }

        @Override
        public void run() {
            while (true) {
                byte[] bytes = new byte[1024];
                int read = 0;
                try {
                    try {
                        read = inputStream.read(bytes);
                    }catch (Exception e){
                        e.printStackTrace();
                        System.out.println("远程连接断开");
                        break;
                    }
                    while (read > 0) {
                        String s = new String(bytes);
                        char[] chars = s.toCharArray();
                        StringBuilder str = new StringBuilder();
                        for (char aChar : chars) {
                            if (aChar!=' ')
                                str.append(aChar);
                        }
                        System.out.println(str.toString());

                        read = inputStream.read(bytes);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
