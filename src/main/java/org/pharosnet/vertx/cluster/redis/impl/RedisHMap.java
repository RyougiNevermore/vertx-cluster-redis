package org.pharosnet.vertx.cluster.redis.impl;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.shareddata.impl.ClusterSerializable;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import org.redisson.Redisson;
import org.redisson.RedissonMapEntry;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SentinelServersConfig;

import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.util.*;

public class RedisHMap<K, V> implements Map<K, V> {

    private static final Logger log = LoggerFactory.getLogger(RedisHMap.class);

    public RedisHMap() {
    }

    public RedisHMap(RedisOptions options, String name) {
        Config config = new Config();
        if (options.getType().equals(RedisClientType.STANDALONE)) {
            String host = options.getEndpoint().host();
            int port = options.getEndpoint().port();
            config.useSingleServer()
                    .setAddress(String.format("%s:%d", host, port))
                    .setDatabase(options.getSelect())
                    .setPassword(options.getPassword());
        } else if (options.getType().equals(RedisClientType.SENTINEL)) {
            SentinelServersConfig sentinelConfig = config.useSentinelServers()
                    .setDatabase(options.getSelect())
                    .setMasterName(options.getMasterName())
                    .setPassword(options.getPassword());
            List<SocketAddress> endpoints = options.getEndpoints();
            for (SocketAddress address : endpoints) {
                String host = address.host();
                int port = address.port();
                sentinelConfig.addSentinelAddress(String.format("%s:%d", host, port));
            }
        } else if (options.getType().equals(RedisClientType.CLUSTER)) {
            ClusterServersConfig clusterServersConfig = config.useClusterServers()
                    .setPassword(options.getPassword())
                    .setScanInterval(2000);
            List<SocketAddress> endpoints = options.getEndpoints();
            for (SocketAddress address : endpoints) {
                String host = address.host();
                int port = address.port();
                clusterServersConfig.addNodeAddress(String.format("redis://%s:%d", host, port));
            }
        }
        this.redisson = Redisson.create(config);
        this.name = name;
    }

    private Vertx vertx;
    private RedissonClient redisson;
    private String name;

    String asString(Object object) throws IOException {
        return new String(asByte(object), Charset.forName("UTF-8"));
    }

    byte[] asByte(Object object) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(byteOut);
        if (object instanceof ClusterSerializable) {
            ClusterSerializable clusterSerializable = (ClusterSerializable) object;
            dataOutput.writeBoolean(true);
            dataOutput.writeUTF(object.getClass().getName());
            Buffer buffer = Buffer.buffer();
            clusterSerializable.writeToBuffer(buffer);
            byte[] bytes = buffer.getBytes();
            dataOutput.writeInt(bytes.length);
            dataOutput.write(bytes);
        } else {
            dataOutput.writeBoolean(false);
            ByteArrayOutputStream javaByteOut = new ByteArrayOutputStream();
            ObjectOutput objectOutput = new ObjectOutputStream(javaByteOut);
            objectOutput.writeObject(object);
            dataOutput.write(javaByteOut.toByteArray());
        }
        return byteOut.toByteArray();
    }

    <T> T asObject(byte[] bytes) throws Exception {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(byteIn);
        boolean isClusterSerializable = in.readBoolean();
        if (isClusterSerializable) {
            String className = in.readUTF();
            Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            int length = in.readInt();
            byte[] body = new byte[length];
            in.readFully(body);
            try {
                ClusterSerializable clusterSerializable;
                //check clazz if have a public Constructor method.
                if (clazz.getConstructors().length == 0) {
                    Constructor<T> constructor = (Constructor<T>) clazz.getDeclaredConstructor();
                    constructor.setAccessible(true);
                    clusterSerializable = (ClusterSerializable) constructor.newInstance();
                } else {
                    clusterSerializable = (ClusterSerializable) clazz.newInstance();
                }
                clusterSerializable.readFromBuffer(0, Buffer.buffer(body));
                return (T) clusterSerializable;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to load class " + e.getMessage(), e);
            }
        } else {
            byte[] body = new byte[in.available()];
            in.readFully(body);
            ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(body));
            return (T) objectIn.readObject();
        }
    }

    @Override
    public int size() {
        return redisson.getMap(name).size();
    }

    @Override
    public boolean isEmpty() {
        return this.size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return redisson.getMap(name).containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return redisson.getMap(name).containsValue(value);
    }

    @Override
    public V get(Object key) {
        V v = null;
        try {
            Object value = redisson.getMap(name).get(key);
            v = asObject(value.toString().getBytes(Charset.forName("UTF-8")));
        } catch (Exception e) {
            log.error("sync map get {} failed", e, key);
        }
        return v;
    }

    @Override
    public V put(K key, V value) {
        String k = null;
        String v = null;
        try {
            k = asString(key);
            v = asString(value);
        } catch (Exception e) {
            log.error("sync map put {} {} failed", e, key, value);
        }
        redisson.getMap(name).put(k, v);
        return value;
    }

    @Override
    public V remove(Object key) {
        V v = this.get(key);
        redisson.getMap(name).remove(key);
        return v;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        redisson.getMap(name).putAll(m);
    }

    @Override
    public void clear() {
        redisson.getMap(name).clear();
    }

    @Override
    public Set<K> keySet() {
        Set<K> ks = new HashSet<>();

        Set<Object> set = redisson.getMap(name).keySet();
        for (Object key : set) {
            try {
                K k = asObject(key.toString().getBytes(Charset.forName("UTF-8")));
                ks.add(k);
            } catch (Exception e) {
                log.error("sync map keySet {} failed", e, key);
            }
        }
        return ks;
    }

    public List<K> keys() {
        List<K> ks = new ArrayList<>();
        Set<K> keys = this.keySet();
        for (K k : keys) {
            ks.add(k);
        }
        return ks;
    }

    @Override
    public Collection<V> values() {
        List<V> vs = new ArrayList<>();

        Collection<Object> values = redisson.getMap(name).values();

        for (Object value : values) {
            try {
                V v = asObject(value.toString().getBytes(Charset.forName("UTF-8")));
                vs.add(v);
            } catch (Exception e) {
                log.error("sync map values {} failed", e, value);
            }
        }
        return vs;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> set = new HashSet<>();
        Set<Entry<Object, Object>> entries = redisson.getMap(name).entrySet();
        for (Entry<Object, Object> entry : entries) {
            Object key = entry.getKey();
            Object value = entry.getValue();

            try {
                K k = asObject(key.toString().getBytes(Charset.forName("UTF-8")));
                V v = asObject(value.toString().getBytes(Charset.forName("UTF-8")));

                set.add(new RedissonMapEntry<K, V>(k, v));
            } catch (Exception e) {
                log.error("sync map entrySet {} {} failed", e, key, value);
            }
        }
        return set;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Set<Entry<K, V>> set = this.entrySet();
        for (Entry<K,V> e : set) {
            sb.append(e.getKey()).append(":").append(e.getValue()).append("\n");
        }
        return sb.toString();
    }

}
