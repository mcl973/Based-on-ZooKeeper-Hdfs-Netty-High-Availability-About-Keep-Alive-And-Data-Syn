package Zookeeper.ArgsAndPacket;

public class Pakcet {
    private String sendname;
    private int code;
    private String data;
    private String SessionId;
    public Pakcet(String sendname,int code){
        this.sendname = sendname;
        this.code = code;
    }
    public Pakcet(String sendname,int code,String SessionId){
        this.sendname = sendname;
        this.code = code;
        this.SessionId = SessionId;
    }
    public Pakcet(String sendname,int code,String data,String SessionId){
        this.sendname = sendname;
        this.code = code;
        this.data = data;
        this.SessionId = SessionId;
    }

    public String getSendname() {
        return sendname;
    }

    public void setSendname(String sendname) {
        this.sendname = sendname;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getSessionId() {
        return SessionId;
    }

    public void setSessionId(String sessionId) {
        SessionId = sessionId;
    }

    /**
     * 总是第一个是name后一个是状态码
     * @return
     */
    @Override
    public String toString() {
            return "Packet#####"+getSendname()+"#####"+getCode()+"#####"+getData()+"#####"+getSessionId();
    }

}
