package study.bigdata.zk;

import study.bigdata.zk.Service.HelloService;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.rmi.RemoteException;

public class RmiClient {
    public RmiClient(String serverName) throws NamingException, RemoteException {
        String url = "rmi://" + serverName + ":1099/";
        Context context = new InitialContext();
        HelloService service = (HelloService) context.lookup(url + "HelloService");
        String result = service.sayHello("hello invoking");
        System.out.println(result);
    }

    public static void main(String[] args) throws NamingException, RemoteException {
        new RmiClient("192.168.91.2");
    }
}
