package study.bigdata.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import javax.naming.NamingException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Random;

public class ZkClient {
    static String url = "192.168.91.130:2181,192.168.91.131:2181,192.168.91.132:2181,192.168.91.133:2181,192.168.91.134:2181";
    static ZooKeeper zk;

    static {
        try {
            zk = new ZooKeeper(url, 10000, new Watcher() {
                public void process(WatchedEvent event) {
                    String path = event.getPath();
                    Event.KeeperState state = event.getState();

                    Event.EventType type = event.getType();
                    if ("/servers".equals(path) && type.equals(Event.EventType.NodeChildrenChanged)) {
                        try {
                            zk.getChildren(path, true);
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<String> getServers() throws KeeperException, InterruptedException {
        return zk.getChildren("/servers", true);
    }

    public static void handle() throws KeeperException, InterruptedException, RemoteException, NamingException {
        List<String> servers  = getServers();
        String path = servers.get(new Random().nextInt(5));
        new RmiClient(path);
    }

    public static void main(String[] args) throws InterruptedException, RemoteException, KeeperException, NamingException {
        for(int i =1; i < 10; i++) {
            handle();
        }
    }
}
