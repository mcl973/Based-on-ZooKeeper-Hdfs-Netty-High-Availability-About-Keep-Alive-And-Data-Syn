package Zookeeper.zookeeper_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import Zookeeper.ArgsAndPacket.ArgsInfo;
import java.io.*;
import java.util.ArrayList;

/**
 *  用于在不同的nettyServer之间同步数据
 */
public class ZooKeeperHdfs {
    private Configuration conf = null;
    private FileSystem fs = null;
    public ZooKeeperHdfs(){
        try {
            connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void connect() throws Exception {
        conf = new Configuration(true);
        fs = FileSystem.get(conf);
    }
    public boolean insertData(String path,Object object){
        Path path1 = new Path(path);
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fs.create(path1, true, ArgsInfo.BlockSize, ArgsInfo.BlockBackUpNum, ArgsInfo.BlockSize);
            OutputStream out = fsDataOutputStream.getWrappedStream();
            out.write(ObjectToBytes(object));
            out.flush();
            out.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     *  写入正常的数据
     * @param path
     * @param data  session id
     * @return
     */
    public boolean insertStringData(String path,String data){
        Path path1 = new Path(path);
        FSDataOutputStream fsDataOutputStream = null;
        try {
            if (path1.getFileSystem(conf).exists(path1))
                return false;
            fsDataOutputStream = fs.create(path1, true, ArgsInfo.BlockSize, ArgsInfo.BlockBackUpNum, ArgsInfo.BlockSize);
            OutputStream out = fsDataOutputStream.getWrappedStream();
            out.write(data.getBytes());
            out.flush();
            out.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
    /**
     *  写入正常的数据,但是在写入前需要判断这个文件是否存在，如果存在那么就不执行数据写入，如果不存在，那么创建文件并写入数据。
     * @param path
     * @param data  session id
     * @return
     */
    public boolean insertStringData(String path,String data,boolean isexist){
        Path path1 = new Path(path);
        FSDataOutputStream fsDataOutputStream = null;
        try {
            if (isexist)
                if(path1.getFileSystem(conf).exists(path1))
                    return false;
            fsDataOutputStream = fs.create(path1, true, ArgsInfo.BlockSize, ArgsInfo.BlockBackUpNum, ArgsInfo.BlockSize);
            OutputStream out = fsDataOutputStream.getWrappedStream();
            out.write(data.getBytes());
            out.flush();
            out.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean appendStringData(String path,String data){
        Path path1 = new Path(path);
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fs.create(path1, true, ArgsInfo.BlockSize, ArgsInfo.BlockBackUpNum, ArgsInfo.BlockSize);
            String stringdata = getStringdata(path);
            if (stringdata == null)
                stringdata = "";
            stringdata+=data+"\n";
            OutputStream out = fsDataOutputStream.getWrappedStream();
            out.write(stringdata.getBytes());
            out.flush();
            out.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     *  读取正常的数据
     * @param path
     * @return
     */
    public String getStringdata(String path){
        Path path1 = new Path(path);
        FSDataInputStream fsDataInputStream = null;
        String data = null;
        try {
            if (path1.getFileSystem(conf).exists(path1)){
                try {
                    fsDataInputStream = fs.open(path1);
                    InputStream wrappedStream = fsDataInputStream.getWrappedStream();
                    FileStatus fileStatus = fs.getFileStatus(path1);
                    long len = fileStatus.getLen();
                    byte[] bytes = new byte[(int)len];
                    int read = wrappedStream.read(bytes);
                    if (read>0) {
                        data = new String(bytes).trim();
                        if (data.equals("None"))
                            data = null;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    /**
     * 删除节点
     * @param path
     * @return
     */
    public boolean deletePath(String path){
        Path path1 = new Path(path);
        boolean istrue = false;
        try {
            if (path1.getFileSystem(conf).exists(path1)) {
                istrue = fs.delete(path1, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return istrue;
    }

    /**
     * 清空节点中的数据
     * @param path
     * @return
     */
    public boolean clearPathData(String path){
        Path path1 = new Path(path);
        boolean istrue = false;
        try {
            if (path1.getFileSystem(conf).exists(path1)) {
                istrue = insertStringData(path, "");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return istrue;
    }

    public Object getObject(String path){
        Path path1 = new Path(path);
        FSDataInputStream open = null;
        Object object = null;
        try {
            open = fs.open(path1);
            byte[] bytes = new byte[ArgsInfo.ObjectToByteSize];
            ArrayList<byte[]> list = new ArrayList<>();
            int read = open.read(bytes);
            while (read!=-1){
                list.add(bytes.clone());
                bytes = new byte[ArgsInfo.ObjectToByteSize];
                read = open.read(bytes);
            }
            if (list.size() == 1){
                object = BytesToObject(list.get(0));
            }else if(list.size()>1){
                int len = (list.size()-1)* ArgsInfo.ObjectToByteSize;
                byte[] bytes1 = list.get(list.size() - 1);
                int i = 0;
                for (; i < bytes1.length; i++) {
                    if(bytes1[i]=='\0')
                        break;
                }
                len+=i;
                byte[] bytes2 = new byte[len];
                int k = 0;
                for (byte[] bytes3 : list) {
                    for (byte b : bytes3) {
                        bytes2[k++] = b;
                    }
                }
                object = BytesToObject(bytes2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return object;
    }

    /**
     * 将对象序列化成字节
     * @param object  对象
     * @return
     */
    public byte[] ObjectToBytes(Object object){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutput = null;
        try {
            objectOutput = new ObjectOutputStream(byteArrayOutputStream);
            objectOutput.writeObject(object);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 将字节反序列化成对象
     * @param bytes   数组
     * @return
     */
    public Object BytesToObject(byte[] bytes){
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = null;
        Object object = null;
        try {
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            object = objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return object;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

}
