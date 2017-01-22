package sdfs.namenode;

import com.fasterxml.jackson.databind.*;
import sdfs.Constants;
import sdfs.namenode.log.CheckPoint;
import sdfs.namenode.log.Log;

import java.io.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by lenovo on 2016/11/25.
 */
public class Logger {
    private ObjectWriter objectWriter;
    private ObjectMapper objectMapper;
    private BufferedWriter fileWriter;
    private ConcurrentLinkedQueue<Log> logs = new ConcurrentLinkedQueue<>();
    private FlushDisk flushDisk = new FlushDisk();
    private static Logger instance = null;
    private NameNodeService nameNodeService;
    private long flushDiskInternalSeconds = Constants.FLUSHDISKINTERNALSECONDS;
    private long start = System.currentTimeMillis();
    private long end = start;
    private long DEV;
    private long interval;
    private Timer timer = new Timer();
    private long size = 0;
    private String path = System.getProperty("sdfs.namenode.dir") == null ? Constants.LOG_PATH : System.getProperty("sdfs.namenode.dir") + "/namenode.log";
    private Logger() throws IOException {
        this.objectMapper = new ObjectMapper();
        this.objectWriter = this.objectMapper.writer();
        this.fileWriter = new BufferedWriter(new FileWriter(path, true));
        nameNodeService = new NameNodeService();
        timer.schedule(flushDisk, 0, Constants.FLUSHDISKINTERNALSECONDS);
    }
    Logger(NameNode nameNode) throws IOException {
        this.objectMapper = new ObjectMapper();
        this.objectWriter = this.objectMapper.writer();
        this.fileWriter = new BufferedWriter(new FileWriter(path, true));
        nameNodeService = nameNode.getNameNodeService();
        timer.schedule(flushDisk, 0, Constants.FLUSHDISKINTERNALSECONDS);
    }
    public static synchronized Logger getLoggerInstance() {
        if (instance == null) {
            try {
                instance = new Logger();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return instance;
    }

    void addLog(Log log) throws IOException {
        logs.add(log);
        if (logs.size() > Constants.LOG_THREHOLD) {
            flushLog();
        }
        size++;
    }
    void flushLog() throws IOException {
        while (logs.size() > 0) {
            Log operation = logs.poll();
            String log = objectWriter.writeValueAsString(operation);
            fileWriter.append(log);
            fileWriter.newLine();
            fileWriter.flush();
        }
    }

    private class FlushDisk extends TimerTask {
        @Override
        public synchronized void run() {
            System.out.println(">>>>>>>>>>>flush is in progress");
            end = System.currentTimeMillis();
            if (size > 0) {
                long elapse = (end - start) / size;
                size = 0;
                flushDiskInternalSeconds = (int) ((1 - Constants.ALPHA) * flushDiskInternalSeconds + Constants.ALPHA * elapse);
                start = end;
                DEV = (long) ((1 - Constants.BETA) * DEV + Constants.BETA * Math.abs(elapse - flushDiskInternalSeconds));
                interval = flushDiskInternalSeconds + 4 * DEV;

                timer.cancel();
                timer = new Timer();
                timer.schedule(new FlushDisk(), 0, interval);

                nameNodeService.saveMetaData();
                CheckPoint checkPoint = new CheckPoint("checkpoint");

                try {
                    addLog(checkPoint);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
