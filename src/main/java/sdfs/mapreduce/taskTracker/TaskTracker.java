package sdfs.mapreduce.taskTracker;

import sdfs.mapreduce.Task.MapperTask;
import sdfs.mapreduce.Task.ReducerTask;
import sdfs.mapreduce.Task.Task;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Created by lenovo on 2016/10/6.
 */
public class TaskTracker {
    private String tempDir = "/tmp/mapreduce-tasktracker";
    private String host;
    private int registryPort;
    private int fileServerPort;
    private int mapperTaskNumber;
    private int reduceTaskNumber;
    private long timestamp;
    private Set<Task> tasks;
    private ExecutorService tPool;

    public TaskTracker() {

    }
    public TaskTracker(String host, int registryPort, int fileServerPort, int mapperTaskNumber, int reduceTaskNumber, long timestamp) {
        this.host = host;
        this.registryPort = registryPort;
        this.fileServerPort = fileServerPort;
        this.mapperTaskNumber = mapperTaskNumber;
        this.reduceTaskNumber = reduceTaskNumber;
        this.timestamp = timestamp;
    }
    //run mapper task
    public void runMapperTask(MapperTask task){
        task.setOutputDir(tempDir);
        task.setFileServerHost(host);
        task.setFileServerPort(fileServerPort);
        task.createTaskFolder();
        tPool.execute(new MapperTaskWorker(task, this));
        increaseMapperTaskNumber();
    }
    //run reducer task
    public void runReducerTask(List<ReducerTask> reducerTasks){
        for(ReducerTask reducerTask : reducerTasks){
            runReducerTask(reducerTask);
        }
    }

    public void increaseReducerTaskAmount(){
        increaseReducerTaskNumber();
    }

    private void increaseReducerTaskNumber() {
        reduceTaskNumber++;
    }

    private void runReducerTask(ReducerTask reducerTask){
        ReducerTaskWorker reducerWorker = new ReducerTaskWorker(reducerTask, this);
        tPool.execute(reducerWorker);
    }

    public void increaseMapperTaskNumber(){
        mapperTaskNumber++;
    }
}
