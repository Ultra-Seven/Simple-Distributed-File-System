package sdfs.Server;

import sdfs.client.DFSClient;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * Created by lenovo on 2016/10/22.
 */
public class RPC {
    public static <T> T getProxy(final Class<T> clazz,String host,int port) {
        RPCClient client = new RPCClient(host,port);
        InvocationHandler handler = (proxy, method, args) -> {
            Invocation invo = new Invocation();
            invo.setInterfaceName(clazz.getName());
            invo.setMethodName(method.getName());
            invo.setParameterType(method.getParameterTypes());
            invo.setParameters(args);
            client.call(invo);
            return invo.getResult();
        };

        T t = (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[] {clazz}, handler);
        return t;
    }
}
