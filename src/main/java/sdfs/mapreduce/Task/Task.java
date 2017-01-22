package sdfs.mapreduce.Task;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lenovo on 2016/10/6.
 * abstract class contains mapper task and reducer task
 */
public abstract class Task implements Serializable {
    public static final String TASK_FOLDER_PREFIX = "TASK_";
    private static AtomicInteger maxId = new AtomicInteger();
    protected String outputDir;
    protected int taskId;
    protected int jobId;
    protected int type;

    protected Task(int jobId, int type) {
        this.jobId = jobId;
        this.type = type;
    }
    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public static AtomicInteger getMaxId() {
        return maxId;
    }

    public static void setMaxId(AtomicInteger maxId) {
        Task.maxId = maxId;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public void createTaskFolder(){
        File folder = new File(outputDir + "/" + getTaskFolderName());
        if(folder.exists()){
            folder.delete();
        }
        folder.mkdirs();
    }
    public String getTaskFolderName(){
        return TASK_FOLDER_PREFIX + taskId;
    }
}
