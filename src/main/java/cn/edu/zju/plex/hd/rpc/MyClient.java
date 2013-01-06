package cn.edu.zju.plex.hd.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

public class MyClient {
    
    private MyProtocol proxy;

    public MyClient(){
        InetSocketAddress addr = new InetSocketAddress("localhost",8888);
        try {
            proxy = (MyProtocol) RPC.waitForProxy(MyProtocol.class, 1, addr , new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void println(String s){
        System.out.println(proxy.println(new Text(s)));
    }

    public void close(){
        RPC.stopProxy(proxy);
    }
    
    public static void main(String[] args) {
        MyClient c = new MyClient();
        c.println("123");
        c.close();
    }
}