package study.bigdata.zk;

import study.bigdata.zk.Service.HelloService;
import study.bigdata.zk.ServiceImpl.HelloServiceImpl;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class RmiServer {

    public RmiServer(String serverName) throws RemoteException, NamingException {
        try {
            LocateRegistry.createRegistry(1099);
        } catch (RemoteException e) {
            e.printStackTrace();
            Registry registry = LocateRegistry.getRegistry(1099);
            UnicastRemoteObject.unexportObject(registry, true);
        }
        HelloService service = new HelloServiceImpl(serverName);
        Context context = new InitialContext();
        context.rebind("rmi://" + serverName + ":1099/HelloService", service);
        System.out.println("服务启动了……");
    }
}
