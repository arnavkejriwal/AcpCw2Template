package uk.ac.ed.acp.cw2.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

@RestController
@RequestMapping("/api/v1/redis")  // Changed endpoint path
public class RedisController {     // Renamed class

    private static final Logger logger = LoggerFactory.getLogger(RedisController.class);
    private final JedisPool jedisPool;
    private final RuntimeEnvironment environment;

    public RedisController(RuntimeEnvironment environment) {
        this.environment = environment;
        this.jedisPool = new JedisPool(
                environment.getRedisHost(),
                environment.getRedisPort()
        );
    }

    // ==================== ASSIGNMENT-REQUIRED METHODS ====================

    public Integer getVersion(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(key);
            return value != null ? Integer.parseInt(value) : null;
        }
    }

    public void setVersion(String key, int version) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(key, String.valueOf(version));
        }
    }

    public void deleteKey(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
        }
    }

    // ==================== EXISTING ENDPOINTS (PRESERVED) ====================

    @GetMapping("/cache/{cacheKey}")
    public String retrieveFromCache(@PathVariable String cacheKey) {
        logger.info("Retrieving {} from cache", cacheKey);
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(cacheKey) ? jedis.get(cacheKey) : null;
        }
    }

    @PutMapping("/cache/{cacheKey}/{cacheValue}")
    public void storeInCache(
            @PathVariable String cacheKey,
            @PathVariable String cacheValue
    ) {
        logger.info("Storing {} with key {}", cacheValue, cacheKey);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(cacheKey, cacheValue);
        }
    }
}