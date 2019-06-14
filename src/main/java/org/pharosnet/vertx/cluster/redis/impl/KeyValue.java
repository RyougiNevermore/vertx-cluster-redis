package org.pharosnet.vertx.cluster.redis.impl;

public class KeyValue<V> {

    public KeyValue() {
    }

    public KeyValue(String key, V value) {
        this.key = key;
        this.value = value;
    }

    private String key;
    private V value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public boolean isEmpty() {
        return null == this.key;
    }

}
