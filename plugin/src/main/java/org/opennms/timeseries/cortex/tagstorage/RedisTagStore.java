package org.opennms.timeseries.cortex.tagstorage;

import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.timeseries.cortex.CortexTSSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import static org.opennms.timeseries.cortex.tagstorage.TagStoreUtils.*;

public class RedisTagStore implements TagStorage {
    private static final Logger LOG = LoggerFactory.getLogger(RedisTagStore.class);
    private final JedisPool jedisPool;

    public RedisTagStore(CortexTSSConfig config) {
        String host = config.getExternalTagStorageHost();
        long port = config.getExternalTagStoragePort();

        JedisPoolConfig jedisConfig = generateJedisConfig();
        jedisPool = new JedisPool(jedisConfig, host, (int) port);

//        jedisPool = new JedisPool(jedisConfig, host, (int) port, 1000,
//                "PASSWORD", true);
    }

    private static JedisPoolConfig generateJedisConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }

    @Override
    public void storeTags(Sample sample, String tenantID) {
        String oldValue = getStoredString(sample.getMetric(), tenantID);

        final String tagString = combineTagsToString(sample.getMetric().getExternalTags(), oldValue);
        final String key = generateCacheKey(sample.getMetric().getKey(), tenantID);
        try (Jedis jedis = jedisPool.getResource()) {
            String i = jedis.set(key, tagString);
        }
    }

    @Override
    public Metric retrieveTags(Metric metric, String tenantID) {
        String value = getStoredString(metric, tenantID);
        return insertStringTagsInMetric(metric, value);
    }

    private String getStoredString(Metric metric, String tenantID) {
        final String key = generateCacheKey(metric.getKey(), tenantID);
        String value = null;
        try (Jedis jedis = jedisPool.getResource()) {
            value = jedis.get(key);
        }
        return value;
    }

}
