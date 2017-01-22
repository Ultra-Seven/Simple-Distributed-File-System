package sdfs.mapreduce.jobTracker;

import sdfs.mapreduce.Task.MapperTask;
import sdfs.mapreduce.Task.ReducerTask;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Created by lenovo on 2016/10/6.
 */
public interface IJobTracker extends Remote{

    public void mapperTask(MapperTask task) throws RemoteException;
    public void reducerTask(List<ReducerTask> reducerTasks) throws RemoteException;
}
