package sdfs.mapreduce.taskTracker;

import sdfs.Constants;
import sdfs.mapreduce.MapReduce;
import sdfs.mapreduce.OutputCollector;
import sdfs.mapreduce.Pair;
import sdfs.mapreduce.Task.Task;

import java.io.*;

/**
 * Created by lenovo on 2016/10/7.
 * mapper task worker
 */
public class MapperTaskWorker extends TaskTrackerWorker {
    public MapperTaskWorker(Task task, TaskTracker taskTracker) {
        super(task, taskTracker);
    }

    @Override
    public void run() {
        OutputCollector collector = getOutputCollector();
    }
    //get mediate file through map function
    private OutputCollector getOutputCollector() {
        //TODO: initiate map and reduce
        MapReduce mapreduce = null;
        OutputCollector collector = new OutputCollector();
        String fileName = Constants.DEFAULT_MAPREDUCE_METAFILE;
        FileReader fr= null;
        try {
            fr = new FileReader(fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader br=new BufferedReader(fr);
        String line = "";

        try {
            while((line = br.readLine()) != null){
                Pair<String, String> entry = null;
                mapreduce.map(entry.getKey(), line, collector);
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return collector;
    }
}
