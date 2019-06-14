package org.pharosnet.vertx.cluster.redis.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Counter;
import io.vertx.redis.client.Redis;

public class RedisInternalCounter implements Counter {

    private Redis redis;
    private Vertx vertx;

    @Override
    public void get(Handler<AsyncResult<Long>> resultHandler) {

    }

    @Override
    public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {

    }

    @Override
    public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {

    }

    @Override
    public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {

    }

    @Override
    public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {

    }

    @Override
    public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {

    }

    @Override
    public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {

    }

}
