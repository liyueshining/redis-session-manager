/*-
 *  Copyright 2015 Crimson Hexagon
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package prt.shining.rsm.lettuce.cluster;

import com.crimsonhexagon.rsm.RedisSession;
import com.crimsonhexagon.rsm.RedisSessionClient;
import com.crimsonhexagon.rsm.RedisSessionManager;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LettuceClusterSessionManager extends RedisSessionManager {
    public static final String DEFAULT_URI = "redis://localhost:6379";

    protected final Log log = LogFactory.getLog(getClass());

    private AbstractRedisClient client = RedisClient.create();
    private StatefulConnection<String, Object> connection;
    StatefulRedisPubSubConnection<String, String> pubSubConnection;
    private String nodes = DEFAULT_URI;
    private String password;
    private String sentinelMaster;

    @Override
    protected synchronized void startInternal() throws LifecycleException {
        super.startInternal();

        //start a thread to subscribe redis key expire event
        RedisPubSubCommands<String, String> redisPubSubCommands = pubSubConnection.sync();
        redisPubSubCommands.getStatefulConnection().addListener(new RedisPubSubListener<String, String>() {
            @Override
            public void message(String channel, String message) {
                log.info("msg=" + message +  "on channel " + channel);

                if (message == null) {
                    log.info("message can not ne null");
                    return;
                }

                String sessionKeyId = message;
                if (sessionKeyId.startsWith(getSessionKeyPrefix())) {
                    expireExpiredSessionInRedis(sessionKeyId);
                }
            }

            @Override
            public void message(String pattern, String channel, String message) {

            }

            @Override
            public void subscribed(String channel, long count) {

            }

            @Override
            public void psubscribed(String pattern, long count) {

            }

            @Override
            public void unsubscribed(String channel, long count) {

            }

            @Override
            public void punsubscribed(String pattern, long count) {

            }
        });

        if (redisPubSubCommands instanceof RedisClusterPubSubCommands) {
            ((RedisClusterPubSubCommands<String, String>) redisPubSubCommands).masters().commands().subscribe("__keyevent@0__:expired");
        } else {
            redisPubSubCommands.subscribe("__keyevent@0__:expired");
        }
    }

    private void expireExpiredSessionInRedis(String sessionKeyId) {
        RedisSession session = new RedisSession(null);
        session.setValid(true);
        session.setId(sessionKeyId.replaceFirst(getSessionKeyPrefix(), ""));
        session.setManager(this);

        //invoke session expire to fire session destroy event when session expired in redis
        session.expire();
    }

    @Override
    protected final RedisSessionClient buildClient() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (nodes == null || nodes.trim().length() == 0) {
            log.error("Nodes can not be empty");
            throw new IllegalStateException("Manager must specify node string. e.g., nodes=\"redis://node1.com:6379 redis://node2.com:6379\"");
        }
        RedisCodec<String, Object> codec = new ContextClassloaderJdkSerializationCodec(getContainerClassLoader());
        List<String> nodes = Arrays.asList(getNodes().trim().split("\\s+"));
        this.connection = createRedisConnection(nodes, codec);
        return new LettuceClusterSessionClient(connection, codec);
    }

    private StatefulConnection<String, Object> createRedisConnection(List<String> nodes, RedisCodec<String, Object> codec) {
        if (nodes.size() == 1) {
            RedisURI redisURI = RedisURI.create(nodes.get(0));

            if (sentinelMaster != null) {
                redisURI = RedisURI.Builder.sentinel(nodes.get(0), sentinelMaster).build();
            }

            if (password != null) {
                redisURI.setPassword(password);
            }

            pubSubConnection = ((RedisClient)client).connectPubSub(redisURI);
            StatefulRedisConnection<String, Object> connection = ((RedisClient)client).connect(codec, redisURI);
            return connection;
        } else {
            if (sentinelMaster != null) {
                RedisURI.Builder redisURIBuilder = RedisURI.Builder.sentinel(nodes.get(0), sentinelMaster);
                for (int index = 1; index <= nodes.size() - 1; index++) {
                    redisURIBuilder.withSentinel(nodes.get(index));
                }
                RedisURI redisURI = redisURIBuilder.build();
                if (password != null) {
                    redisURI.setPassword(password);
                }

                pubSubConnection = ((RedisClient)client).connectPubSub(redisURI);
                StatefulRedisConnection<String, Object> connection = ((RedisClient)client).connect(codec, redisURI);
                return connection;
            }

            ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                    .enableAdaptiveRefreshTrigger(ClusterTopologyRefreshOptions.RefreshTrigger.MOVED_REDIRECT, ClusterTopologyRefreshOptions.RefreshTrigger.PERSISTENT_RECONNECTS)
                    .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(30))
                    .build();

            List<RedisURI> uris = nodes.stream()
                    .map(RedisURI::create)
                    .map(uri -> {
                        if (password != null) {
                            uri.setPassword(password);
                        }
                        return uri; })
                    .collect(Collectors.toList());
            client = RedisClusterClient.create(uris);

            ((RedisClusterClient) client).setOptions(ClusterClientOptions.builder()
                    .topologyRefreshOptions(topologyRefreshOptions)
                    .build());

            pubSubConnection = ((RedisClusterClient) client).connectPubSub();
            ((StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection).setNodeMessagePropagation(true);

            StatefulRedisClusterConnection<String, Object> connection = ((RedisClusterClient) client).connect(codec);
            return connection;
        }
    }

    @Override
    public Session findSession(String id) throws IOException {
        Session session = super.findSession(id);
        if (session != null) {
            log.trace("update session's access time and end access time when get session from redis to avoid remove session that not expired in redis");
            session.access();
            session.endAccess();
        }

        return session;
    }

    @Override
    public void unload() throws IOException {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSentinelMaster() {
        return sentinelMaster;
    }

    public void setSentinelMaster(String sentinelMaster) {
        this.sentinelMaster = sentinelMaster;
    }
}
