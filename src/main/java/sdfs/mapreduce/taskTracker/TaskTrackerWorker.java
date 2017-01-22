package sdfs.mapreduce.taskTracker;

import sdfs.mapreduce.Task.Task;

/**
 * Created by lenovo on 2016/10/7.
 */
public abstract class TaskTrackerWorker implements Runnable {
    protected Task task;
    protected TaskTracker taskTracker;

    public TaskTrackerWorker(Task task, TaskTracker taskTracker) {
        this.task = task;
        this.taskTracker = taskTracker;
    }
}
