package org.pharosnet.vertx.cluster.redis;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import org.pharosnet.vertx.cluster.redis.impl.RedisAsyncMap;
import org.pharosnet.vertx.cluster.redis.impl.RedisAsyncMultiMap;
import org.pharosnet.vertx.cluster.redis.impl.RedisHMap;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RedisClusterManager implements ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

    private static final String LOCK_SEMAPHORE_PREFIX = "__vertx.";
    private static final String NODE_ID_ATTRIBUTE = "__vertx.nodeId";

    private static final String SYNC_MAP_PREFIX = "__vertx.sync.map";

    private String id;
    private boolean active;
    private Redis redis;
    private RedisOptions options;
    private Vertx vertx;

    private Map<String, AsyncMultiMap> asyncMultiMaps;
    private Map<String, AsyncMap> asyncMaps;
    private Map<String, RedisHMap> syncMaps;



    public RedisClusterManager(Vertx vertx, RedisOptions options) {
        this.id = UUID.randomUUID().toString();
        this.options = options;
        this.asyncMaps = new ConcurrentHashMap<>();
        this.asyncMultiMaps = new ConcurrentHashMap<>();
        this.syncMaps = new ConcurrentHashMap<>();
    }

    @Override
    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
        vertx.executeBlocking(bf -> {
            Redis.createClient(vertx, options).connect(r -> {
                if (r.failed()) {
                    bf.fail(r.cause());
                    return;
                }
                this.redis = r.result();
                bf.complete();
            });
        }, br -> {
            if (br.failed()) {
                this.active = false;
                throw new RuntimeException("connect to redis server failed", br.cause());
            }
            this.active = true;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> handler) {
        AsyncMultiMap<K, V> map;
        if (!this.asyncMultiMaps.containsKey(name)) {
            map = new RedisAsyncMultiMap<>(this.redis, name);
            this.asyncMultiMaps.put(name, map);
        } else {
           map = this.asyncMultiMaps.get(name);
        }
        handler.handle(Future.succeededFuture(map));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> handler) {
        AsyncMap<K, V> map;
        if (!this.asyncMaps.containsKey(name)) {
            map = new RedisAsyncMap<K, V>(this.redis, name);
            this.asyncMaps.put(name, map);
        } else {
            map = this.asyncMaps.get(name);
        }
        handler.handle(Future.succeededFuture(map));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        RedisHMap<K, V> map;
        if (!this.syncMaps.containsKey(name)) {
            map = new RedisHMap<>(this.options, name);
            this.syncMaps.put(name, map);
        } else {
            map = this.syncMaps.get(name);
        }
        return map;
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> handler) {

    }

    @Override
    public void getCounter(String name, Handler<AsyncResult<Counter>> handler) {

    }

    @Override
    public String getNodeID() {
        return this.id;
    }

    @Override
    public List<String> getNodes() {
        return null;
    }

    @Override
    public void nodeListener(NodeListener listener) {

    }

    @Override
    public void join(Handler<AsyncResult<Void>> handler) {

    }

    @Override
    public void leave(Handler<AsyncResult<Void>> handler) {

    }

    @Override
    public boolean isActive() {
        return active;
    }

}
