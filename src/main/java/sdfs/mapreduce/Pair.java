package sdfs.mapreduce;

import java.io.Serializable;

/**
 * Created by lenovo on 2016/10/7.
 */
public class Pair<K, V> implements Serializable, Comparable<Pair> {
    private K key;
    private V value;

    public Pair(K key, V value){
        setKey(key);
        setValue(value);
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public int compareTo(Pair o) {
        if(key.hashCode() == o.getKey().hashCode()){
            return value.hashCode() - o.getValue().hashCode();
        }
        return key.hashCode() - o.getKey().hashCode();
    }
}
