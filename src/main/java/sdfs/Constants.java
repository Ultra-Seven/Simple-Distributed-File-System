package sdfs;

/**
 * Created by lenovo on 2016/10/2.
 * Default constants
 */
public class Constants {
    public static final int DEFAULT_BLOCK_SIZE = 1 << 16;
    public static final String DEFAULT_IP = "localhost";
    public static final int DEFAULT_PORT = 4348;
    public static final String DEFAULT_MASTER_EDIT_LOG_PATH =
            System.getProperty("user.dir") +
                    System.getProperty("file.separator") +
                    "dfs_master_edit.log";
    public static final String DEFAULT_SLAVE_DATA_PATH =
            System.getProperty("user.dir") +
                    System.getProperty("file.separator") +
                    "dfs_slave_data";
    public static final String DEFAULT_DATA_SERVICE_NAME = "datanode";
    public static final String PATH_PREFIX = "sdfs://" + DEFAULT_IP + ":" + DEFAULT_PORT + "/";
    public static final String META_DATA_PATH = "meta/namenode";
    public static final String DEFAULT_MAPREDUCE_METAFILE = "";
    public static final int GARBAGE_LIMIT = 1 << 8;
    public static final int LOG_THREHOLD = 20;
    public static final String LOG_PATH = "log/log";
    public static final int FLUSHDISKINTERNALSECONDS = 2000;
    public static final double ALPHA = 0.125;
    public static final double BETA = 0.25;
}
