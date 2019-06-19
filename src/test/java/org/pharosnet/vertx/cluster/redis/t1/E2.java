package org.pharosnet.vertx.cluster.redis.t1;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import org.pharosnet.vertx.cluster.redis.RedisClusterManager;

public class E2 {

    public static void main(String[] args) {

        RedisOptions redisOptions = new RedisOptions();
        redisOptions.setPassword("123456@yl");
        redisOptions.setSelect(0);
        redisOptions.setEndpoint(SocketAddress.inetSocketAddress(6379, "47.101.181.225"));
        redisOptions.setType(RedisClientType.STANDALONE);

        ClusterManager mgr = new RedisClusterManager(redisOptions);

        VertxOptions options = new VertxOptions().setClusterManager(mgr);

        Vertx.clusteredVertx(options, res -> {
            if (res.succeeded()) {
                Vertx vertx = res.result();
                vertx.createHttpServer().requestHandler(request -> {

                    // This handler gets called for each request that arrives on the server
                    HttpServerResponse response = request.response();
                    response.putHeader("content-type", "text/plain");

                    // Write to the response and end it
                    response.end("Hello World!");
                }).listen(8181);
            } else {
                // failed!
                res.cause().printStackTrace();
            }
        });
    }

}
