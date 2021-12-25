package Utils;

import redis.clients.jedis.Jedis;

public class Redis {
    private static volatile Jedis redis;

    public static Jedis getInstance() {
        if (null == redis) {
            redis = new Jedis("localhost", 6379, 1000 * 100);
        }
        return redis;
    }

    public static void main(String[] args) {
    }
}
