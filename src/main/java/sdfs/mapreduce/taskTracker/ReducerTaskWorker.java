package sdfs.mapreduce.taskTracker;

import sdfs.Constants;
import sdfs.client.DFSClient;
import sdfs.mapreduce.MapReduce;
import sdfs.mapreduce.OutputCollector;
import sdfs.mapreduce.Pair;
import sdfs.mapreduce.Task.MapperTask;
import sdfs.mapreduce.Task.ReducerTask;
import sdfs.mapreduce.Task.Task;

import java.io.*;
import java.rmi.NotBoundException;
import java.util.*;

/**
 * Created by lenovo on 2016/10/7.
 */
public class ReducerTaskWorker extends TaskTrackerWorker{
    private PriorityQueue<MapperTask> mapperTasks;
    private HashMap<Integer, String> mapperFiles;
    private OutputCollector outputCollector;
    public ReducerTaskWorker(Task task, TaskTracker taskTracker) {
        super(task, taskTracker);
    }
    //get out put file through reduce function and save it into the sdfs
    @Override
    public void run() {
        MapperTask mapperTask = mapperTasks.poll();
        if(mapperTask == null){
            return;
        }
        try{
            if(mapperFiles.size() == ((ReducerTask)task).getMapperCount()){
                List<String> files = new ArrayList<>(mapperFiles.values());
                String reducedFile = "reduceFile/";
                String unreducedFile = "unreducedFile/";
                mergeSortedFiles(files, unreducedFile);

                OutputCollector collector = new OutputCollector();
                MapReduce mr = null;
                reduce(unreducedFile, mr, collector);

                saveResultToLocal(reducedFile, collector);
                saveResultToDFS(reducedFile);

            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    //merge sort file
    private void mergeSortedFiles(List<String> files, String unreducedFile) throws IOException {
        List<BufferedReader> buffers = new ArrayList<>();
        Comparator<Pair<String, BufferedReader>> comparator = new Comparator<Pair<String, BufferedReader>>() {
            @Override
            public int compare(Pair<String, BufferedReader> o1, Pair<String, BufferedReader> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        };
        PriorityQueue<Pair<String, BufferedReader>> lineQueues =
                new PriorityQueue<>(10, comparator);
        files.stream().forEach(file -> {
            try {
                BufferedReader buffer = new BufferedReader(new FileReader(file));
                String line = buffer.readLine();
                lineQueues.add(new Pair<String, BufferedReader>(line, buffer));
                buffers.add(buffer);

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        BufferedWriter writer = new BufferedWriter(new FileWriter(unreducedFile));
        while(lineQueues.size() > 0){
            Pair<String, BufferedReader> entry = lineQueues.poll();
            writer.write(entry.getKey());
            writer.newLine();
            if(buffers.contains(entry.getValue())){
                String line = entry.getValue().readLine();
                if(line != null){
                    lineQueues.offer(new Pair<String, BufferedReader>(line, entry.getValue()));
                } else {
                    entry.getValue().close();
                    buffers.remove(entry.getValue());
                }
            }
        }
        writer.close();
    }
    //reduce function
    private void reduce(String unreducedFile, MapReduce mr, OutputCollector collector) throws IOException {
        String key = null;
        List<String> values = new ArrayList<>();
        mr.reduce(key, values.iterator(), collector);
    }
    //save the result into the local file system
    private void saveResultToLocal(String reducedFile, OutputCollector collector) throws IOException {
        FileWriter writer = new FileWriter(reducedFile);
        Iterator<Pair<String, String>> iterator = collector.getIterator();
        while(iterator.hasNext()){
            Pair<String, String> entry = iterator.next();
            writer.write(entry.getKey() + "/" + entry.getValue() + "\n");
        }
        writer.close();
    }
    //save the file into sdfs
    private void saveResultToDFS(String reducedFile) throws IOException, NotBoundException {
        DFSClient dfsClient = new DFSClient(Constants.DEFAULT_IP,
                Constants.DEFAULT_PORT);
        dfsClient.connect();
        dfsClient.writeTask(reducedFile);
    }

    public OutputCollector getOutputCollector() {
        return outputCollector;
    }

    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
}
