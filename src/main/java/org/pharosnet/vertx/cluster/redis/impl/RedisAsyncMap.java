package org.pharosnet.vertx.cluster.redis.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.ResponseType;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RedisAsyncMap<K, V> extends RedisHMap<K, V> implements AsyncMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(RedisAsyncMap.class);


    private static final String map_key_map_key_prefix = "_io.vertx.async.map.key_";


    public RedisAsyncMap(Redis redis) {
        this.redis = redis;
    }

    private Redis redis;
    private RedisAPI api;

    @Override
    public void get(K k, Handler<AsyncResult<V>> handler) {

        if (k == null) {
            handler.handle(Future.failedFuture("key is empty"));
            return;
        }
        String key;
        try {
            key = map_key_map_key_prefix + asString(k);
        } catch (Exception e) {
            log.error("redis async map get failed at key to string, {}", e, k);
            handler.handle(Future.failedFuture(e));
            return;
        }

        api.get(key, gr -> {
            if (gr.failed()) {
                log.error("redis async map get failed, {}", gr.cause(), k);
                handler.handle(Future.failedFuture(gr.cause()));
                return;
            }
            if (gr.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis get failed," + gr.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            V v;
            try {
                v = asObject(gr.result().toBytes());
            } catch (Exception e) {
                log.error("redis async map get value failed at bytes to object", e);
                handler.handle(Future.failedFuture(e));
                return;
            }
            handler.handle(Future.succeededFuture(v));
        });


    }

    @Override
    public void put(K k, V v, Handler<AsyncResult<Void>> handler) {

        if (k == null) {
            handler.handle(Future.failedFuture("key is empty"));
            return;
        }
        String key;
        try {
            key = map_key_map_key_prefix + asString(k);
        } catch (Exception e) {
            log.error("redis async map put failed at key to string, {}", e, k);
            handler.handle(Future.failedFuture(e));
            return;
        }

        if (v == null) {
            handler.handle(Future.failedFuture("value is empty"));
            return;
        }
        String value;
        try {
            value = asString(v);
        } catch (Exception e) {
            log.error("redis async map put failed at value to string, {}", e, v);
            handler.handle(Future.failedFuture(e));
            return;
        }

        api.set(List.of(key, value), sr -> {
            if (sr.failed()) {
                log.error("redis async map put failed, {}", sr.cause(), k);
                handler.handle(Future.failedFuture(sr.cause()));
                return;
            }
            if (sr.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis put failed," + sr.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            handler.handle(Future.succeededFuture());
        });

    }

    @Override
    public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> handler) {

        if (k == null) {
            handler.handle(Future.failedFuture("key is empty"));
            return;
        }
        String key;
        try {
            key = map_key_map_key_prefix + asString(k);
        } catch (Exception e) {
            log.error("redis async map put with ttl failed at key to string, {}", e, k);
            handler.handle(Future.failedFuture(e));
            return;
        }

        if (v == null) {
            handler.handle(Future.failedFuture("value is empty"));
            return;
        }
        String value;
        try {
            value = asString(v);
        } catch (Exception e) {
            log.error("redis async map put with ttl failed at value to string, {}", e, v);
            handler.handle(Future.failedFuture(e));
            return;
        }

        if (ttl <= 0) {
            handler.handle(Future.failedFuture("redis async map put with ttl failed, bad ttl"));
            return;
        }

        api.set(List.of(key, value, "" + ttl), sr -> {
            if (sr.failed()) {
                log.error("redis async map put with ttl failed, {}", sr.cause(), k);
                handler.handle(Future.failedFuture(sr.cause()));
                return;
            }
            if (sr.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis put with ttl failed," + sr.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            handler.handle(Future.succeededFuture());
        });

    }

    @Override
    public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> handler) {
        if (k == null) {
            handler.handle(Future.failedFuture("key is empty"));
            return;
        }
        String key;
        try {
            key = map_key_map_key_prefix + asString(k);
        } catch (Exception e) {
            log.error("redis async map put with absent failed at key to string, {}", e, k);
            handler.handle(Future.failedFuture(e));
            return;
        }

        api.exists(List.of(key), er -> {
            if (er.failed()) {
                log.error("redis async map put with absent failed at check key, {}", er.cause(), k);
                handler.handle(Future.failedFuture(er.cause()));
                return;
            }
            if (er.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis async map put with absent failed," + er.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            if (Optional.ofNullable(er.result().toInteger()).orElse(0) == 1) {
                handler.handle(Future.failedFuture("put failed, exists key"));
                return;
            }
            put(k, v, pr -> {
                if (pr.failed()) {
                    handler.handle(Future.failedFuture(pr.cause()));
                    return;
                }
                handler.handle(Future.succeededFuture(v));
            });
        });
    }

    @Override
    public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> handler) {
        if (k == null) {
            handler.handle(Future.failedFuture("key is empty"));
            return;
        }
        String key;
        try {
            key = map_key_map_key_prefix + asString(k);
        } catch (Exception e) {
            log.error("redis async map put with absent and ttl failed at key to string, {}", e, k);
            handler.handle(Future.failedFuture(e));
            return;
        }

        api.exists(List.of(key), er -> {
            if (er.failed()) {
                log.error("redis async map put with absent and ttl failed at check key, {}", er.cause(), k);
                handler.handle(Future.failedFuture(er.cause()));
                return;
            }
            if (er.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis async map put with absent and ttl failed," + er.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            if (Optional.ofNullable(er.result().toInteger()).orElse(0) == 1) {
                handler.handle(Future.failedFuture("put with absent and ttl failed, exists key"));
                return;
            }
            put(k, v, ttl, pr -> {
                if (pr.failed()) {
                    handler.handle(Future.failedFuture(pr.cause()));
                    return;
                }
                handler.handle(Future.succeededFuture(v));
            });
        });

    }

    @Override
    public void remove(K k, Handler<AsyncResult<V>> handler) {
        if (k == null) {
            handler.handle(Future.failedFuture("key is empty"));
            return;
        }
        String key;
        try {
            key = map_key_map_key_prefix + asString(k);
        } catch (Exception e) {
            log.error("redis async map remove failed at key to string, {}", e, k);
            handler.handle(Future.failedFuture(e));
            return;
        }
        get(k, gr -> {
            V gv = null;
            if (gr.succeeded()) {
                gv = gr.result();
            }
            final V v = gv;
            api.del(List.of(key), r -> {
                if (r.failed()) {
                    log.error("redis async map remove failed at check key, {}", r.cause(), k);
                    handler.handle(Future.failedFuture(r.cause()));
                    return;
                }
                if (r.result().type() == ResponseType.ERROR) {
                    handler.handle(Future.failedFuture("redis async map remove failed," + r.result().toString(Charset.forName("UTF-8"))));
                    return;
                }
                handler.handle(Future.succeededFuture(v));
            });
        });
    }

    @Override
    public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> handler) {
        get(k, gr -> {
            if (gr.failed()) {
                handler.handle(Future.failedFuture(gr.cause()));
                return;
            }
            if (!gr.result().equals(v)) {
                handler.handle(Future.succeededFuture(false));
                return;
            }
            remove(k, dr -> {
                if (dr.failed()) {
                    handler.handle(Future.failedFuture(dr.cause()));
                    return;
                }
                handler.handle(Future.succeededFuture(true));
            });
        });

    }

    @Override
    public void replace(K k, V v, Handler<AsyncResult<V>> handler) {
        put(k, v, r -> {
            if (r.failed()) {
                handler.handle(Future.failedFuture(r.cause()));
                return;
            }
            handler.handle(Future.succeededFuture(v));
        });
    }

    @Override
    public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> handler) {
        get(k, gr -> {
            if (gr.failed()) {
                handler.handle(Future.failedFuture(gr.cause()));
                return;
            }
            if (!gr.result().equals(oldValue)) {
                handler.handle(Future.succeededFuture(false));
                return;
            }
            put(k, newValue, pr -> {
                if (pr.failed()) {
                    handler.handle(Future.failedFuture(pr.cause()));
                    return;
                }
                handler.handle(Future.succeededFuture(true));
            });
        });
    }

    @Override
    public void clear(Handler<AsyncResult<Void>> handler) {
        api.keys(map_key_map_key_prefix + "*", kr -> {
            if (kr.failed()) {
                handler.handle(Future.failedFuture(kr.cause()));
                return;
            }
            if (kr.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis async map clear failed," + kr.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            int size = kr.result().size();
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                String key = kr.result().get(i).toString(Charset.forName("UTF-8"));
                keys.add(key);
            }
            api.del(keys, dr -> {
                if (dr.failed()) {
                    handler.handle(Future.failedFuture(dr.cause()));
                    return;
                }
                handler.handle(Future.succeededFuture());
            });
        });
    }

    @Override
    public void size(Handler<AsyncResult<Integer>> handler) {
        api.keys(map_key_map_key_prefix + "*", kr -> {
            if (kr.failed()) {
                handler.handle(Future.failedFuture(kr.cause()));
                return;
            }
            if (kr.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis async map size failed," + kr.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            int size = kr.result().size();
            handler.handle(Future.succeededFuture(size));
        });
    }

    @Override
    public void keys(Handler<AsyncResult<Set<K>>> handler) {
        api.keys(map_key_map_key_prefix + "*", kr -> {
            if (kr.failed()) {
                handler.handle(Future.failedFuture(kr.cause()));
                return;
            }
            if (kr.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis async map keys failed," + kr.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            int size = kr.result().size();
            Set<K> keys = new HashSet<>();
            for (int i = 0; i < size; i++) {
                byte[] key = kr.result().get(i).toBytes();
                try {
                    keys.add(asObject(key));
                } catch (Exception e) {
                    handler.handle(Future.failedFuture(e));
                    return;
                }
            }
            handler.handle(Future.succeededFuture(keys));
        });
    }

    @Override
    public void values(Handler<AsyncResult<List<V>>> handler) {
        keys(kr -> {
            if (kr.failed()) {
                handler.handle(Future.failedFuture(kr.cause()));
                return;
            }
            Set<K> keySet = kr.result();
            List<String> keys = new ArrayList<>();
            for (K k : keySet) {
                try {
                    keys.add(asString(k));
                } catch (Exception e) {
                    handler.handle(Future.failedFuture(e));
                    return;
                }
            }
            api.mget(keys, gr -> {
                if (gr.failed()) {
                    handler.handle(Future.failedFuture(gr.cause()));
                    return;
                }
                if (gr.result().type() == ResponseType.ERROR) {
                    handler.handle(Future.failedFuture("redis async map values failed," + gr.result().toString(Charset.forName("UTF-8"))));
                    return;
                }
                List<V> values = new ArrayList<>();
                int size = gr.result().size();
                for (int i = 0; i < size; i++) {
                    try {
                        values.add(asObject(gr.result().get(i).toBytes()));
                    } catch (Exception e) {
                        handler.handle(Future.failedFuture(e));
                        return;
                    }
                }
                handler.handle(Future.succeededFuture(values));
            });
        });
    }

    @Override
    public void entries(Handler<AsyncResult<Map<K, V>>> handler) {
        keys(kr -> {
            if (kr.failed()) {
                handler.handle(Future.failedFuture(kr.cause()));
                return;
            }

            Set<K> keySet = kr.result();
            List<Future> futures = new ArrayList<>();
            for (K k : keySet) {
                Future<KeyValue<K, V>> future = Future.future();
                get(k, gr -> {
                    if (gr.failed()) {
                        future.fail(gr.cause());
                        return;
                    }
                    future.complete(new KeyValue<>(k, gr.result()));
                });
            }
            CompositeFuture.all(futures).setHandler(r -> {
                if (r.failed()) {
                    handler.handle(Future.failedFuture(r.cause()));
                }
                Map<K, V> map = new ConcurrentHashMap<>();
                List<KeyValue<K, V>> result = r.result().list();
                for (KeyValue<K, V> kv : result) {
                    map.put(kv.getKey(), kv.getValue());
                }
                handler.handle(Future.succeededFuture(map));
            });
        });
    }

}
