package sdfs.namenode;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by lenovo on 2016/11/29.
 */
class DataNodeList {
    private Set<DataNodeListEntry> dataNodeListEntries = new ConcurrentSkipListSet<>();

    DataNodeList() {
        CheckList checkList = new CheckList();
        Timer timer = new Timer();
        timer.schedule(checkList, 0, 1200000);
    }
    //add a new entry or update a primitive entry
    void addEntry(DataNodeListEntry entry) {
        DataNodeListEntry dataNodeListEntry = dataNodeListEntries.stream().filter(entry1 -> entry1.getIp().equals(entry.getIp()) && entry1.getPort() == entry.getPort()).findFirst().orElse(null);
        if (dataNodeListEntry == null)
            dataNodeListEntries.add(entry);
        else
            dataNodeListEntry.setCreateTime(entry.getCreateTime());
    }
    //check list thread running every half an hour to delete dead date node
    private class CheckList extends TimerTask {
        @Override
        public void run() {
            long date = System.currentTimeMillis();
            dataNodeListEntries.forEach(entry -> {
                if (date - entry.getCreateTime() > 1200000)
                    dataNodeListEntries.remove(entry);
            });
        }
    }
}
class DataNodeListEntry {
    //data node ip
    private String ip;
    //data node port
    private int port;
    //data node heart beat time
    private long createTime;
    DataNodeListEntry(String ip, int port, long createTime) {
        this.ip = ip;
        this.port = port;
        this.createTime = createTime;
    }

    String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    long getCreateTime() {
        return createTime;
    }

    void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
}