package Utils;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

//mongodb 连接数据库工具类
public class Mongodb {
    //不通过认证获取连接数据库对象
    private static volatile MongoDatabase mongoDatabase;

    public static MongoDatabase getInstance() {
        //连接到 mongodb 服务
        if (mongoDatabase == null) {
            MongoClient mongoClient = new MongoClient("localhost", 27017);
            mongoDatabase = mongoClient.getDatabase("recommend");
            System.out.println();
        }
        return mongoDatabase;
        //连接到数据库
    }
}