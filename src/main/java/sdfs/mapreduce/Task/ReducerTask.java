package sdfs.mapreduce.Task;

/**
 * Created by lenovo on 2016/10/6.
 */
public class ReducerTask extends Task{
    //output file path
    private String outputFile;
    private int mapperCount;
    private int lineCount;

    protected ReducerTask(int jobId, int type) {
        super(jobId, type);
    }
    public ReducerTask(int jobId) {
        super(jobId, 1);
    }

    public int getLineCount() {
        return lineCount;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }

    public int getMapperCount() {
        return mapperCount;
    }

    public void setMapperCount(int mapperCount) {
        this.mapperCount = mapperCount;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }
}
