package sdfs.mapreduce.jobTracker;

import sdfs.mapreduce.Task.FileBlock;
import sdfs.mapreduce.Task.MapperTask;
import sdfs.mapreduce.Task.ReducerTask;
import sdfs.mapreduce.taskTracker.TaskTracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by lenovo on 2016/10/6.
 */
public class JobTracker {
    private HashMap<String, TaskTracker> taskTrackerHashMap;
    private HashMap<Integer, Job> jobHashMap;
    private PriorityBlockingQueue<MapperTask> mapperTasksQueue;
    private ArrayList<TaskTracker> taskTrackers = new ArrayList<>();
    private ExecutorService threadPool;
    private JobTrackerService service;

    public JobTracker() {
        taskTrackerHashMap = new HashMap<>();
        jobHashMap = new HashMap<>();
        mapperTasksQueue = new PriorityBlockingQueue<>();
        threadPool = Executors.newFixedThreadPool(16);
    }
    //submit the job
    public void submitJob(Job job) {
        jobHashMap.put(job.getId(), job);
        threadPool.execute(new JobTrackerWorker(this, job.getId()));
    }
    //execute the job
    public void executeJob(int jobId) {
        Job job = jobHashMap.get(jobId);
        if(job != null){
            generateMapperTasks(job);
            generateReducerTasks(job);
            dispatchMapperTasks(job);
        }
    }
    //dispatch mapper task
    private void dispatchMapperTasks(Job job) {
        List<MapperTask> mapperTasks = new ArrayList<>(job.getMapperTasks().values());
        mapperTasks.stream().forEach(e-> mapperTasksQueue.offer(e));
    }
    //transform mediate file into output file
    private void generateReducerTasks(Job job) {
        int reducerAmount = job.getReducerCount();
        for(int i = 0; i < reducerAmount; i++){
            ReducerTask task = new ReducerTask(job.getId());
            task.setMapperCount(job.getMapperCount());
            task.setOutputFile(job.getFilePath());
            //task.setLineCount(job.getConfig().getOutputFileBlockSize());
            job.getReducerTasks().put(task.getTaskId(), task);

        }
    }
    //transform file block into mapper task
    private void generateMapperTasks(Job job) {
        List<FileBlock> fileBlocks = splitInputFile(job);
        job.setMapperCount(fileBlocks.size());
        for(FileBlock fileBlock : fileBlocks){
            MapperTask task = new MapperTask(job.getId(), fileBlock, job.getReducerCount());
            job.getMapperTasks().put(task.getTaskId(), task);
        }
    }
    //spilt the job into multiple file blocks
    private List<FileBlock> splitInputFile(Job job) {
        List<FileBlock> blocks = new ArrayList<>();
        int rangeCount = job.getMapperCount();
        int rangeSize = job.getFileLen() / rangeCount;
        for(int i = 0; i < rangeCount; i++){
            long offset = i * rangeSize;
            int size = -1;
            if(i < rangeCount - 1){
                size = (int) rangeSize;
            }
            else
                size = (int) (job.getFileLen() - offset);
            blocks.add(new FileBlock(job.getFilePath(), (int)offset, size));
        }
        return blocks;
    }
}
