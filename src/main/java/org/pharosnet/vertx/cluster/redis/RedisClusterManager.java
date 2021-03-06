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
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import org.pharosnet.vertx.cluster.redis.impl.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RedisClusterManager implements ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);


    private static final String SYNC_MAP_PREFIX = "__vertx.sync.map";

    private static final String LOCK_PREFIX = "__vertx.sync.lock.";


    private String id;
    private boolean active;
    private Redis redis;
    private RedisAPI api;
    private RedisOptions options;
    private Vertx vertx;
    private NodeListener nodeListener;

    private Map<String, AsyncMultiMap> asyncMultiMaps;
    private Map<String, AsyncMap> asyncMaps;
    private Map<String, RedisHMap> syncMaps;


    private RedisHMap<String, String> nodes;

    public RedisClusterManager(RedisOptions options) {
        this.id = UUID.randomUUID().toString();
        this.options = options;
        this.asyncMaps = new ConcurrentHashMap<>();
        this.asyncMultiMaps = new ConcurrentHashMap<>();
        this.syncMaps = new ConcurrentHashMap<>();
    }

    @Override
    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
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
        if (!this.syncMaps.containsKey(SYNC_MAP_PREFIX + name)) {
            map = new RedisHMap<>(this.options, SYNC_MAP_PREFIX + name);
            this.syncMaps.put(SYNC_MAP_PREFIX + name, map);
        } else {
            map = this.syncMaps.get(SYNC_MAP_PREFIX + name);
        }
        return map;
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> handler) {
        name = Optional.ofNullable(name).orElse("").strip();
        if (name.length() == 0) {
            handler.handle(Future.failedFuture("bad name"));
            return;
        }
        name = LOCK_PREFIX + name;
        RedisLock lock = new RedisLock(name, api, timeout);
        lock.lock(handler);
    }

    @Override
    public void getCounter(String name, Handler<AsyncResult<Counter>> handler) {
        RedisCounter counter = new RedisCounter(name, api);
        handler.handle(Future.succeededFuture(counter));
    }

    @Override
    public String getNodeID() {
        return this.id;
    }

    @Override
    public List<String> getNodes() {
        return this.nodes.keys();
    }

    @Override
    public void nodeListener(NodeListener listener) {
        this.nodeListener = listener;
    }

    @Override
    public void join(Handler<AsyncResult<Void>> handler) {
        if (!this.active) {
            this.active = true;
        }
        vertx.executeBlocking(bf -> {
            Redis.createClient(vertx, options).connect(r -> {
                if (r.failed()) {
                    bf.fail(r.cause());
                    return;
                }
                this.redis = r.result();
                this.api = RedisAPI.api(redis);
                this.nodes = new RedisHMap<>(this.options, "__vertx.nodes");
                if (log.isDebugEnabled()) {
                    log.debug("cluster {}", this.nodes);
                }
                bf.complete();
            });
        }, br -> {
            if (br.failed()) {
                this.active = false;
                handler.handle(Future.failedFuture(br.cause()));
            }
            this.active = true;
            this.nodes.put(this.id, Instant.now().toString());
            handler.handle(Future.succeededFuture());
        });
    }

    @Override
    public void leave(Handler<AsyncResult<Void>> handler) {
        this.active = false;
        this.nodes.remove(this.id);
        handler.handle(Future.succeededFuture());
    }

    @Override
    public boolean isActive() {
        return active;
    }

}
