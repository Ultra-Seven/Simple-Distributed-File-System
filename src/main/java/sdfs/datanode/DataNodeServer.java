package sdfs.datanode;

import sdfs.Server.Listener;
import sdfs.Server.Server;
import sdfs.Server.Invocation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by lenovo on 2016/10/23.
 */
public class DataNodeServer implements Server {
    private Listener listener;
    private int port = DataNode.DATA_NODE_PORT ;
    private HashMap<String, Object> service = new HashMap<>();
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    @Override
    public void start() {
        System.out.println("DataNode Starting...");
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
        this.service.put(interfaces[0].getName(), impl);
        System.out.println(service);
    }

    @Override
    public void call(Invocation invocation) {
        Object object = service.get(invocation.getInterfaceName());
        if (object != null) {
            try {
                Method method = object.getClass().getMethod(invocation.getMethodName(), invocation.getParameterType());
                Object result = method.invoke(object, invocation.getParameters());
                invocation.setResult(result);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
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
