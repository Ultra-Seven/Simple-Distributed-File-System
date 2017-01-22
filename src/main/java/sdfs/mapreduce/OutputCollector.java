package sdfs.mapreduce;


import java.util.*;

/**
 * Created by lenovo on 2016/10/7.
 */
public class OutputCollector {
    private PriorityQueue<Pair<String, String>> collection;
    private Set<String> keys;

    public OutputCollector() {
        collection = new PriorityQueue<>(10, (Comparator<Pair<String, String>>) (o1, o2) -> {
            if(o1.getKey().compareTo(o2.getKey()) == 0)
                return o1.getValue().compareTo(o2.getValue());
            return o1.getKey().compareTo(o2.getKey());
        });
        keys = new TreeSet<>();
    }
    public void addOutput(String key, String value){
        collection.add(new Pair(key, value));
        keys.add(key);
    }
    public Iterator<Pair<String, String>> getIterator(){
        return collection.iterator();
    }
    public TreeMap<String, List<String>> getMap(){
        TreeMap<String, List<String>> map = new TreeMap<>();
        for(Pair<String, String> pair : collection){
            if(map.containsKey(pair.getKey())){
                map.get(pair.getKey()).add(pair.getValue());
            } else {
                List<String> values = new ArrayList<>();
                values.add(pair.getValue());
                map.put(pair.getKey(), values);
            }
        }
        return map;
    }
}
