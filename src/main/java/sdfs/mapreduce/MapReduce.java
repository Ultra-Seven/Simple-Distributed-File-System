package sdfs.mapreduce;

import java.util.Iterator;

/**
 * Created by lenovo on 2016/10/7.
 * map reduce interface
 * client can have their own map and reduce function
 */
public interface MapReduce {
    public void map(String key, String value, OutputCollector collector);
    public void reduce(String key, Iterator<String> values, OutputCollector collector);
}
