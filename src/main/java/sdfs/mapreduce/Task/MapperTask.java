package sdfs.mapreduce.Task;

/**
 * Created by lenovo on 2016/10/6.
 */
public class MapperTask extends Task{
    public static final String PARTITION_FILE_PREFIX = "MAPPER_";
    //input file block
    private FileBlock inputFileBlock;
    //host ip and port
    private String fileServerHost;
    private int fileServerPort;
    private int reducerAmount;
    //mediate file path
    private String outputPath;

    public MapperTask(int jobId, FileBlock inputFileBlock, int reducerAmount) {
        super(jobId, 0);
        this.inputFileBlock = inputFileBlock;
        this.reducerAmount = reducerAmount;
    }

    public String getFileServerHost() {
        return fileServerHost;
    }

    public void setFileServerHost(String fileServerHost) {
        this.fileServerHost = fileServerHost;
    }

    public int getFileServerPort() {
        return fileServerPort;
    }

    public void setFileServerPort(int fileServerPort) {
        this.fileServerPort = fileServerPort;
    }

    public FileBlock getInputFileBlock() {
        return inputFileBlock;
    }

    public void setInputFileBlock(FileBlock inputFileBlock) {
        this.inputFileBlock = inputFileBlock;
    }

    public int getReducerAmount() {
        return reducerAmount;
    }

    public void setReducerAmount(int reducerAmount) {
        this.reducerAmount = reducerAmount;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
}
