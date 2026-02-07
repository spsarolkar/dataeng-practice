package in.sunilsarolkar.javacore;

import java.util.HashMap;
import java.util.Map;

public class LRUCacheLegacy {

    private static class Node<K,T>{
        public Node<K,T> prev;
        public Node<K,T> next;
        public T value;
        public K key;

        public Node(T value,K key,Node<K,T> prev,Node<K,T> next){
            this.key=key;
            this.value=value;
            this.prev= prev;
            this.next=next;
        }
    }
    Map<Integer,Node<Integer,Integer>> entries=new HashMap<>();
    Node<Integer,Integer> head;
    Node<Integer,Integer> tail;
    final private int capacity;

    public LRUCacheLegacy(int capacity){
        head=new Node<Integer,Integer>(null,null,null,null);
        tail=new Node<Integer,Integer>(null,null,null,null);
        head.next=tail;
        tail.prev=head;
        this.capacity=capacity;
    }

    public Integer get(Integer key) {
        if(!entries.containsKey(key)){
            return null;
        }
        Node<Integer,Integer> node=entries.get(key);
        removeNode(node);
        addToHead(node);
        return node.value;
    }

    public void put(Integer key,Integer val){
        if(entries.containsKey(key)){
            var nn=entries.get(key);
            removeNode(nn);
            nn.value=val;
            addToHead(nn);
        }else{
            entries.put(key,new Node<Integer,Integer>(val,key, null,null));
            addToHead(entries.get(key));
        }

        if(capacity<entries.size()){
            removeNode(tail.prev);
        }
    }

    private void addToHead(Node<Integer,Integer> n){
        var tmp=head.next;
        head.next=n;
        n.next=tmp;
        n.prev=head;
        n.next.prev=n;
    }

    private void removeNode(Node<Integer,Integer> n){
        n.next.prev=n.prev;
        n.prev.next=n.next;
        entries.remove(n.key);
    }

}
