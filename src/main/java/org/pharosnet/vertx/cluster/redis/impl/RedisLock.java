package org.pharosnet.vertx.cluster.redis.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.shareddata.Lock;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.ResponseType;

import java.nio.charset.Charset;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class RedisLock implements Lock {

    private static final String LOCK_PREFIX = "__vertx.sync.lock.";

    public RedisLock(String name, RedisAPI api, long timeout) {
        this.name =  name = LOCK_PREFIX + name;
        this.api = api;
        this.deadline = Instant.now().plusSeconds(timeout);
    }

    private String name;
    private RedisAPI api;
    private Instant deadline;

    public void lock(Handler<AsyncResult<Lock>> handler) {
        if (!Instant.now().isBefore(this.deadline)) {
            handler.handle(Future.failedFuture("timeout"));
            return;
        }
        api.exists(List.of(name), r -> {
            if (r.failed()) {
                handler.handle(Future.failedFuture(r.cause()));
                return;
            }
            if (r.result().type() == ResponseType.ERROR) {
                handler.handle(Future.failedFuture("redis lock failed," + r.result().toString(Charset.forName("UTF-8"))));
                return;
            }
            if (Optional.ofNullable(r.result().toInteger()).orElse(0) == 1) {
//                try {
//                    Thread.sleep(500);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
                this.lock(handler);
                return;
            }
            api.set(List.of(name, "1"), sr -> {
                if (sr.failed()) {
                    handler.handle(Future.failedFuture(sr.cause()));
                    return;
                }
                if (sr.result().type() == ResponseType.ERROR) {
                    handler.handle(Future.failedFuture("redis lock failed at put," + sr.result().toString(Charset.forName("UTF-8"))));
                    return;
                }
                handler.handle(Future.succeededFuture(this));
            });
        });
    }


    @Override
    public void release() {
        api.del(List.of(name), r -> {

        });
    }

}
