package study.bigdata.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import javax.naming.NamingException;
import java.io.IOException;
import java.rmi.RemoteException;

public class ZkServer {
    static String url = "192.168.91.130:2181,192.168.91.131:2181,192.168.91.132:2181,192.168.91.133:2181,192.168.91.134:2181";
    static ZooKeeper zk;

    static {
        try {
            zk = new ZooKeeper(url, 10000, new Watcher() {
                public void process(WatchedEvent event) {
                    String path = event.getPath();
                    Event.KeeperState state = event.getState();
                    Event.EventType type = event.getType();
                    System.out.println("路径：" + path + "， 状态：" + state + "，事件类型：" + type);
                }
            });

            Stat stat =  zk.exists("/servers", false);
            if (stat == null) {
                zk.create("/servers", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 注册服务
     * @param serverName
     * servers
     * 192.168.91.130
     * 192.168.91.131
     * 192.168.91.132
     * 192.168.91.133
     * 192.168.91.134
     */
    public static void registerServer(String serverName) throws KeeperException, InterruptedException {
        Stat stat = zk.exists("/servers/" + serverName, null);
        if (stat == null) {
            zk.create("/servers/" + serverName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public static void logout(String serverName) throws KeeperException, InterruptedException {
        Stat stat = zk.exists("/servers/" + serverName, null);
        if (stat != null) {
            zk.delete("/servers/" + serverName, -1);
        }
    }

    /**
     * 启动服务器
     * @param serverName
     * @throws RemoteException
     * @throws NamingException
     */
    public static void handle(String serverName) throws RemoteException, NamingException {
        new RmiServer(serverName);
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, RemoteException, NamingException {
        handle(args[0]);
        registerServer(args[0]);
    }
}
