package org.pharosnet.vertx.cluster.redis;

import io.vertx.core.AsyncResult;
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

import java.util.List;
import java.util.Map;

public class RedisClusterManager implements ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

    private static final String LOCK_SEMAPHORE_PREFIX = "__vertx.";
    private static final String NODE_ID_ATTRIBUTE = "__vertx.nodeId";

    private String id;
    private boolean active;
    private Redis redis;
    private RedisOptions options;
    private Vertx vertx;

    public RedisClusterManager(RedisOptions options) {
        this.options = options;

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

    @Override
    public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {

    }

    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {

    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        return null;
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {

    }

    @Override
    public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {

    }

    @Override
    public String getNodeID() {
        return null;
    }

    @Override
    public List<String> getNodes() {
        return null;
    }

    @Override
    public void nodeListener(NodeListener listener) {

    }

    @Override
    public void join(Handler<AsyncResult<Void>> resultHandler) {

    }

    @Override
    public void leave(Handler<AsyncResult<Void>> resultHandler) {

    }

    @Override
    public boolean isActive() {
        return active;
    }

}
