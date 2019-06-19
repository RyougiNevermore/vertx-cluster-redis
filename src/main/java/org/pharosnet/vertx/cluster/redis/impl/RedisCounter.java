package org.pharosnet.vertx.cluster.redis.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.shareddata.Counter;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.ResponseType;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;

public class RedisCounter implements Counter {

    private static final String COUNT_PREFIX = "__vertx.sync.count.";

    public RedisCounter(String name, RedisAPI api) {
        this.name = name = COUNT_PREFIX + name;
        this.api = api;
        api.set(List.of(name, "0"),  r -> {});
    }

    private String name;
    private RedisAPI api;

    @Override
    public void get(Handler<AsyncResult<Long>> handler) {
        api.get(name, r -> {
            if (r.failed()) {
                handler.handle(Future.failedFuture(r.cause()));
                return;
            }
            if (r.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis count get failed," + r.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            handler.handle(Future.succeededFuture(r.result().toLong()));
        });
    }

    @Override
    public void incrementAndGet(Handler<AsyncResult<Long>> handler) {
        api.incr(name, r -> {
            if (r.failed()) {
                handler.handle(Future.failedFuture(r.cause()));
                return;
            }
            if (r.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis count incr failed," + r.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            handler.handle(Future.succeededFuture(r.result().toLong()));
        });
    }

    @Override
    public void getAndIncrement(Handler<AsyncResult<Long>> handler) {
        this.get(gr -> {
            if (gr.failed()) {
                handler.handle(Future.failedFuture(gr.cause()));
                return;
            }
            Long v = gr.result();
            this.incrementAndGet(ir -> {
                if (ir.failed()) {
                    handler.handle(Future.failedFuture(ir.cause()));
                    return;
                }
                handler.handle(Future.succeededFuture(v));
            });
        });
    }

    @Override
    public void decrementAndGet(Handler<AsyncResult<Long>> handler) {
        api.decr(name, r -> {
            if (r.failed()) {
                handler.handle(Future.failedFuture(r.cause()));
                return;
            }
            if (r.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis count decr failed," + r.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            handler.handle(Future.succeededFuture(r.result().toLong()));
        });
    }

    @Override
    public void addAndGet(long value, Handler<AsyncResult<Long>> handler) {
        if (value > 0) {
            api.incrby(name, "" + value, r -> {
                if (r.failed()) {
                    handler.handle(Future.failedFuture(r.cause()));
                    return;
                }
                if (r.result().type() == ResponseType.ERROR) {
                    handler.handle(Future.failedFuture("redis count addAndGet failed at incr," + r.result().toString(Charset.forName("UTF-8"))));
                    return;
                }
                handler.handle(Future.succeededFuture(r.result().toLong()));
            });
        } else {
            api.decrby(name, "" + (value * -1), r -> {
                if (r.failed()) {
                    handler.handle(Future.failedFuture(r.cause()));
                    return;
                }
                if (r.result().type() == ResponseType.ERROR) {
                    handler.handle(Future.failedFuture("redis count addAndGet failed at decr," + r.result().toString(Charset.forName("UTF-8"))));
                    return;
                }
                handler.handle(Future.succeededFuture(r.result().toLong()));
            });
        }
    }

    @Override
    public void getAndAdd(long value, Handler<AsyncResult<Long>> handler) {
        this.get(gr -> {
            if (gr.failed()) {
                handler.handle(Future.failedFuture(gr.cause()));
                return;
            }
            Long v = gr.result();
            this.addAndGet(value, ir -> {
                if (ir.failed()) {
                    handler.handle(Future.failedFuture(ir.cause()));
                    return;
                }
                handler.handle(Future.succeededFuture(v));
            });
        });
    }

    @Override
    public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> handler) {
        this.get(gr -> {
            if (gr.failed()) {
                handler.handle(Future.failedFuture(gr.cause()));
                return;
            }
            if (expected == Optional.ofNullable(gr.result()).orElse(-1L)) {
              api.set(List.of(name, "" + value), sr -> {
                  if (sr.failed()) {
                      handler.handle(Future.failedFuture(sr.cause()));
                      return;
                  }
                  if (sr.result().type() == ResponseType.ERROR) {
                      handler.handle(Future.failedFuture("redis count compareAndSet failed," + sr.result().toString(Charset.forName("UTF-8"))));
                      return;
                  }
                  handler.handle(Future.succeededFuture(true));
              });
              return;
            }
            handler.handle(Future.succeededFuture(false));
        });
    }

}
