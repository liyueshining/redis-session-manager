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
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.apache.catalina.LifecycleException;
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
    StatefulRedisPubSubConnection<String, Object> pubSubConnection;
    private String nodes = DEFAULT_URI;
    private String password;
    private String sentinelMaster;

    @Override
    protected synchronized void startInternal() throws LifecycleException {
        super.startInternal();

        //start a thread to subscribe redis key expire event
        RedisPubSubCommands<String, Object> redisPubSubCommands = pubSubConnection.sync();
        redisPubSubCommands.getStatefulConnection().addListener(new RedisPubSubListener<String, Object>() {
            @Override
            public void message(String channel, Object message) {
                log.info("msg=" + message +  "on channel " + channel);

                String sessionKeyId = (String) message;

                if (sessionKeyId.startsWith(getSessionKeyPrefix())) {
                    RedisSession session = createEmptySession();
                    session.setId(sessionKeyId.replaceFirst(getSessionKeyPrefix(), ""));

                    //invoke session expire to fire session destroy event when session expired in redis
                    session.expire();
                }

            }

            @Override
            public void message(String pattern, String channel, Object message) {

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
        redisPubSubCommands.subscribe("__keyevent@0__:expired");
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

            pubSubConnection = ((RedisClient)client).connectPubSub(codec, redisURI);
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

                pubSubConnection = ((RedisClient)client).connectPubSub(codec, redisURI);
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

            pubSubConnection = ((RedisClusterClient) client).connectPubSub(codec);
            StatefulRedisClusterConnection<String, Object> connection = ((RedisClusterClient) client).connect(codec);
            return connection;
        }
    }

    @Override
    public void unload() throws IOException {
        if (connection != null) {
            connection.close();
        }
        client.shutdown();
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
