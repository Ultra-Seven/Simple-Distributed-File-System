package sdfs.mapreduce.jobTracker;

/**
 * Created by lenovo on 2016/10/7.
 * job tracker worker is a thread
 * execute job
 */
public class JobTrackerWorker implements Runnable {
    private JobTracker jobTracker;
    private int jobId;

    public JobTrackerWorker(JobTracker jobTracker, int jobId){
        this.jobTracker = jobTracker;
        this.jobId = jobId;
    }
    @Override
    public void run() {
        try {
            jobTracker.executeJob(jobId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
