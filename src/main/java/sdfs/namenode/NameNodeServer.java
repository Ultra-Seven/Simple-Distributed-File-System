package sdfs.namenode;

import sdfs.Constants;
import sdfs.Server.Invocation;
import sdfs.Server.Listener;
import sdfs.Server.Server;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by lenovo on 2016/10/22.
 */
public class NameNodeServer implements Server {
    private Listener listener;
    private int port = NameNode.NAME_NODE_PORT;
    private HashMap<String, Object> service = new HashMap<>();
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    public NameNodeServer() {

    }
    public NameNodeServer(int port) {
        this.port = port;
    }
    @Override
    public void start() {
        System.out.println("NameNode Starting...");
        listener = new Listener(this);
        isRunning.set(true);
        listener.start();
    }

    @Override
    public void stop() {
        isRunning.set(false);
    }

    @Override
    public void register(Object impl) {
        Class<?>[] interfaces = impl.getClass().getInterfaces();
        if (interfaces == null) {
            throw new IllegalArgumentException("Class must implement an interface!");
        }
        for (int i = 0; i < interfaces.length; i++) {
            this.service.put(interfaces[i].getName(), impl);

        }
        //System.out.println(service);
    }

    @Override
    public void call(Invocation invocation) {
        Object object = service.get(invocation.getInterfaceName());
        if (object != null) {
            try {
                Method method = object.getClass().getMethod(invocation.getMethodName(), invocation.getParameterType());
                Object result = method.invoke(object, invocation.getParameters());
                invocation.setResult(result);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public int getPort() {
        return this.port;
    }
}
