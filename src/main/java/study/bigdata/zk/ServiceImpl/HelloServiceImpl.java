package study.bigdata.zk.ServiceImpl;

import study.bigdata.zk.Service.HelloService;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class HelloServiceImpl extends UnicastRemoteObject implements HelloService {
    private String name;

    public HelloServiceImpl(String name) throws RemoteException {
        super();
        this.name =  name;
    }

    public String sayHello(String data) throws RemoteException {
        System.out.println(data + ",服务被调用");
        return data + name;
    }
}
