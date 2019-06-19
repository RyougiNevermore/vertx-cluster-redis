package org.pharosnet.vertx.cluster.redis.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.ResponseType;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class RedisAsyncMultiMap<K, V> extends RedisHMap<K, V> implements AsyncMultiMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(RedisAsyncMultiMap.class);

    public RedisAsyncMultiMap(Redis redis, String name) {
        this.name = name;
        this.api = RedisAPI.api(redis);
        multi_map_key_map_key_prefix = String.format("_io.vertx.async.multi.map.%s.key_", name);
    }

    private String name;
    private String multi_map_key_map_key_prefix;
    private RedisAPI api;

    @Override
    public void add(K k, V v, Handler<AsyncResult<Void>> handler) {
        if (k == null) {
            handler.handle(Future.failedFuture("key is empty"));
            return;
        }
        String key;
        try {
            key = multi_map_key_map_key_prefix + asString(k);
        } catch (Exception e) {
            log.error("redis async multi map add failed at key to string, {}", e, k);
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
            log.error("redis async multi map add failed at value to string, {}", e, v);
            handler.handle(Future.failedFuture(e));
            return;
        }
        api.lpush(List.of(key, value), r -> {
            if (r.failed()) {
                handler.handle(Future.failedFuture(r.cause()));
                return;
            }
            if (r.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis lpush failed," + r.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            handler.handle(Future.succeededFuture());
        });

    }

    @Override
    public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> handler) {
        if (k == null) {
            handler.handle(Future.failedFuture("get failed, key is empty"));
            return;
        }
        String key;
        try {
            key = multi_map_key_map_key_prefix + asString(k);
        } catch (Exception e) {
            log.error("redis async multi map get failed at key to string, {}", e, k);
            handler.handle(Future.failedFuture(e));
            return;
        }

        api.llen(key, lr -> {
            if (lr.failed()) {
                handler.handle(Future.failedFuture(lr.cause()));
                return;
            }
            if (lr.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis llen failed," + lr.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            Long length = Optional.ofNullable(lr.result().toLong()).orElse(0L);
            if (length <= 0L) {
                handler.handle(Future.succeededFuture(new ChoosableSet<>(0)));
                return;
            }

            api.lrange(key, "0", length.toString(), r -> {
                if (r.failed()) {
                    handler.handle(Future.failedFuture(r.cause()));
                    return;
                }
                if (r.result().type() == ResponseType.ERROR) {
                    handler.handle(Future.failedFuture("redis lrange failed," + r.result().toString(Charset.forName("UTF-8"))));
                    return;
                }
                int size = r.result().size();
                ChoosableSet<V> set = new ChoosableSet<>(size);
                for (int i = 0; i < size; i++) {
                    V v;
                    try {
                        v = asObject(r.result().get(i).toBytes());
                    } catch (Exception e) {
                        log.error("redis lrange failed at value to object", e);
                        handler.handle(Future.failedFuture(e));
                        return;
                    }
                    set.add(v);
                }
                handler.handle(Future.succeededFuture(set));
            });

        });
    }

    @Override
    public void remove(K k, V v, Handler<AsyncResult<Boolean>> handler) {

        if (k == null) {
            handler.handle(Future.failedFuture("key is empty"));
            return;
        }
        String key;
        try {
            key = multi_map_key_map_key_prefix + asString(k);
        } catch (Exception e) {
            log.error("redis async multi map remove failed at key to string, {}", e, k);
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
            log.error("redis async multi map remove failed at value to string, {}", e, v);
            handler.handle(Future.failedFuture(e));
            return;
        }

        api.lrem(key, "0", value, r -> {
            if (r.failed()) {
                handler.handle(Future.failedFuture(r.cause()));
                return;
            }
            if (r.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis lrem failed," + r.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            boolean flag = Optional.ofNullable(r.result().toInteger()).orElse(0) > 0;
            handler.handle(Future.succeededFuture(flag));
        });

    }

    @Override
    public void removeAllForValue(V v, Handler<AsyncResult<Void>> handler) {
        String value;
        try {
            value = asString(v);
        } catch (Exception e) {
            log.error("redis async multi map remove all for key at value to string, {}", e, v);
            handler.handle(Future.failedFuture(e));
            return;
        }

        api.keys(multi_map_key_map_key_prefix + "*", kr -> {
            if (kr.failed()) {
                handler.handle(Future.failedFuture(kr.cause()));
                return;
            }
            if (kr.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis keys failed," + kr.result().toString(Charset.forName("UTF-8"))));
                return;
            }


            int size = kr.result().size();
            List<Future> futures = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                String key = kr.result().get(i).toString(Charset.forName("UTF-8"));
                Future<Response> future = Future.future();
                api.lrem(key, "0", value, future);
                futures.add(future);
            }

            CompositeFuture compositeFuture = CompositeFuture.all(futures);
            compositeFuture.setHandler(r -> {
                if (r.failed()) {
                    log.error("redis async multi map remove all failed", r.cause());
                    handler.handle(Future.failedFuture(r.cause()));
                    return;
                }
                handler.handle(Future.succeededFuture());
            });
        });
    }

    @Override
    public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> handler) {
        api.keys(multi_map_key_map_key_prefix + "*", kr -> {
            if (kr.failed()) {
                handler.handle(Future.failedFuture(kr.cause()));
                return;
            }
            if (kr.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis keys failed," + kr.result().toString(Charset.forName("UTF-8"))));
                return;
            }

            List<Future> futures = new ArrayList<>();


            int size = kr.result().size();

            for (int i = 0; i < size; i++) {
                String key = kr.result().get(i).toString(Charset.forName("UTF-8"));
                api.llen(key, lr -> {
                    Future<KeyValue<String, V>> future = Future.future();
                    futures.add(future);
                    if (lr.failed()) {
                        future.fail(lr.cause());
                        return;
                    }
                    if (lr.result().type() == ResponseType.ERROR) {
                        future.fail("redis llen failed," + lr.result().toString(Charset.forName("UTF-8")));
                        return;
                    }
                    Long length = Optional.ofNullable(lr.result().toLong()).orElse(0L);
                    if (length <= 0L) {
                        future.complete(new KeyValue());
                        return;
                    }

                    api.lrange(key, "0", length.toString(), r -> {
                        if (r.failed()) {
                            future.fail(r.cause());
                            return;
                        }
                        if (r.result().type() == ResponseType.ERROR) {
                            future.fail("redis lrange failed," + r.result().toString(Charset.forName("UTF-8")));
                            return;
                        }
                        int size1 = r.result().size();
                        for (int j = 0; j < size1; j++) {
                            V v;
                            try {
                                v = asObject(r.result().get(j).toBytes());
                            } catch (Exception e) {
                                log.error("redis lrange failed at value to object", e);
                                future.fail(e);
                                return;
                            }
                            if (p.test(v)) {
                                future.complete(new KeyValue(key, v));
                            } else {
                                future.complete(new KeyValue());
                            }
                        }
                    });
                });
            }

            CompositeFuture compositeFuture = CompositeFuture.all(futures);
            compositeFuture.setHandler(r -> {
                if (r.failed()) {
                    log.error("redis async multi map remove all matched failed", r.cause());
                    handler.handle(Future.failedFuture(r.cause()));
                    return;
                }

                List<KeyValue<String, V>> keyValues = r.result().list();
                // TODO LREM WITH COMPOSITE
                List<Future> remFutures = new ArrayList<>();
                for (KeyValue<String, V> keyValue : keyValues) {
                    if (keyValue.isEmpty()) {
                        continue;
                    }


                    Future<Void> remFuture = Future.future();

                    String key = keyValue.getKey();

                    String value;
                    try {
                        value = asString(keyValue.getValue());
                    } catch (Exception e) {
                        remFuture.fail(e);
                        continue;
                    }

                    api.lrem(key, "0", value, remr -> {
                        if (remr.failed()) {
                            remFuture.fail(r.cause());
                            return;
                        }
                        if (remr.result().type() == ResponseType.ERROR) {
                            remFuture.fail("redis lrem failed," + remr.result().toString(Charset.forName("UTF-8")));
                            return;
                        }
                        remFuture.complete();
                    });

                }

                CompositeFuture compositeRemFuture = CompositeFuture.all(remFutures);

                compositeRemFuture.setHandler(ar -> {
                    if (ar.failed()) {
                        handler.handle(Future.failedFuture(ar.cause()));
                        return;
                    }
                    handler.handle(Future.succeededFuture());
                });
            });
        });
    }

}
