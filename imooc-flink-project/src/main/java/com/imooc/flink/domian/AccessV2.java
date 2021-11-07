package com.imooc.flink.domian;

public class AccessV2 {
    public String id;
    public String device;
    public String deviceType;
    public String os;
    public String event;
    public String net;
    public String channel;
    public String uid;
    public int nu;  // 1新
    public int nu2;   // bloom use
    public String ip;  // ==> ip去解析
    public long time;
    public String version;
    public String province;
    public String city;
    public Product product;
    public String date;
    public String hour;

    @Override
    public String toString() {
        return "AccessV2{" +
                "id='" + id + '\'' +
                ", device='" + device + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", os='" + os + '\'' +
                ", event='" + event + '\'' +
                ", net='" + net + '\'' +
                ", channel='" + channel + '\'' +
                ", uid='" + uid + '\'' +
                ", nu=" + nu +
                ", nu2=" + nu2 +
                ", ip='" + ip + '\'' +
                ", time=" + time +
                ", version='" + version + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", product=" + product +
                ", date='" + date + '\'' +
                ", hour='" + hour + '\'' +
                '}';
    }
}
