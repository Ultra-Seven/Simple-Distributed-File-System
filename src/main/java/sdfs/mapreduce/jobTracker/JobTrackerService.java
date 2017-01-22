package sdfs.mapreduce.jobTracker;

import sdfs.mapreduce.Task.MapperTask;
import sdfs.mapreduce.Task.ReducerTask;
import sdfs.mapreduce.taskTracker.TaskTracker;

import java.rmi.RemoteException;
import java.util.List;

/**
 * Created by lenovo on 2016/10/7.
 */
public class JobTrackerService implements IJobTracker {
    private JobTracker jobTracker;
    private TaskTracker taskTracker;
    public JobTrackerService(JobTracker jobTracker) throws RemoteException {
        this.jobTracker = jobTracker;
    }
    @Override
    public void mapperTask(MapperTask task) throws RemoteException {
        taskTracker.runMapperTask(task);
    }

    @Override
    public void reducerTask(List<ReducerTask> reducerTasks) throws RemoteException {
        taskTracker.runReducerTask(reducerTasks);
    }
}
