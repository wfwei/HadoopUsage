package cn.edu.zju.plex.hd.rpc;

import java.io.IOException;
import java.net.UnknownHostException;
  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer implements MyProtocol{
  private Server server;
  public MyServer(){
    try {
      server = RPC.getServer(this, "localhost", 8888, new Configuration());
      server.start();
      server.join();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
}
       
  public Text println(Text t){
    System.out.println(t);
    return new Text("finish");
  }
        
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return 1;
  }
                 
  public static void main(String[] args) {
    new MyServer();
  }
}
