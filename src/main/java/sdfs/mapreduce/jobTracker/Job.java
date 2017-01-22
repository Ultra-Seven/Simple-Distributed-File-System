package sdfs.mapreduce.jobTracker;

import sdfs.mapreduce.Task.MapperTask;
import sdfs.mapreduce.Task.ReducerTask;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lenovo on 2016/10/7.
 * job class
 */
public class Job {
    private static AtomicInteger maxId = new AtomicInteger(0);
    //job id
    private int id;
    //mapper task
    private Map<Integer, MapperTask> mapperTasks;
    //reducer task
    private Map<Integer, ReducerTask> reducerTasks;
    //save to sdfs
    private String filePath = "";
    private int fileLen;
    private int mapperCount;
    private int reducerCount;
    public Job() {
        this.mapperTasks = new TreeMap<Integer, MapperTask>();
        this.reducerTasks = new TreeMap<Integer, ReducerTask>();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Map<Integer, MapperTask> getMapperTasks() {
        return mapperTasks;
    }

    public void setMapperTasks(Map<Integer, MapperTask> mapperTasks) {
        this.mapperTasks = mapperTasks;
    }

    public static AtomicInteger getMaxId() {
        return maxId;
    }

    public static void setMaxId(AtomicInteger maxId) {
        Job.maxId = maxId;
    }

    public Map<Integer, ReducerTask> getReducerTasks() {
        return reducerTasks;
    }

    public void setReducerTasks(Map<Integer, ReducerTask> reducerTasks) {
        this.reducerTasks = reducerTasks;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public int getMapperCount() {
        return mapperCount;
    }

    public void setMapperCount(int mapperCount) {
        this.mapperCount = mapperCount;
    }

    public int getFileLen() {
        return fileLen;
    }

    public void setFileLen(int fileLen) {
        this.fileLen = fileLen;
    }

    public int getReducerCount() {
        return reducerCount;
    }

    public void setReducerCount(int reducerCount) {
        this.reducerCount = reducerCount;
    }
}
