package study.bigdata.zk.Service;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface HelloService extends Remote {
    String sayHello(String data) throws RemoteException;
}
