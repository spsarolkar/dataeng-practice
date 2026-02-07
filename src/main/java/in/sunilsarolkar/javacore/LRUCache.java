package in.sunilsarolkar.javacore;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache extends LinkedHashMap<Integer,Integer> {
    private int capacity;
    public LRUCache(int capacity){
        super(capacity,0.5f,true);
        this.capacity=capacity;
    }

    //publi/zc removeEldestEntry()
    @Override
    protected boolean removeEldestEntry(Map.Entry<Integer,Integer> eldest) {
        return size()>capacity;
    }
}
