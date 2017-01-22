package sdfs.Server;

/**
 * Created by lenovo on 2016/10/22.
 */
public interface Server {
    public void start();
    public void stop();
    public void register(Object impl);
    public void call(Invocation invocation);
    public boolean isRunning();
    public int getPort();
}
