package online;

import Utils.Mongodb;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import grpc.grpc_profile.Request;
import grpc.grpc_profile.Response;
import grpc.grpc_profile.ServerResponseGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Client {

    private final ServerResponseGrpc.ServerResponseBlockingStub blockingStub;
    public Client(Channel channel) {

        blockingStub = ServerResponseGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) throws Exception {
        while(true){
            System.out.println("请输入:");
            Scanner scanner = new Scanner(System.in);
            String next = scanner.next();
            int user = Integer.parseInt(next);
            String target = "localhost:50051";

            ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                    .usePlaintext()
                    .build();
            try {
                Client client = new Client(channel);
                client.greet(user);
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }
    public HashMap<String, String> getInfo(int movieID) {
        HashMap<String, String> map = new HashMap<>();
        String movieid = String.valueOf(movieID);

        MongoDatabase mongoDatabase = Mongodb.getInstance();
        MongoCollection<Document> collection = mongoDatabase.getCollection("movie_info");

        FindIterable<Document> movies = collection.find(Filters.eq("movieId", movieid));

        for (Document row : movies) {
            String str = row.get("name").toString();
            map.put("name", str);

            str = row.get("releaseYear").toString();
            map.put("releaseYear", str);

            str = row.get("info").toString().replace("|", ",");
            String[] str_spl = str.split(",");
            StringBuilder str2 = new StringBuilder();
            for (int i = 2; i < str_spl.length; i++) {
                str2.append(str_spl[i]).append(",");
            }
            map.put("length", str_spl[1]);
            map.put("tag", str2.toString());
            map.put("description", row.get("description").toString());

            str = row.get("type").toString();
            map.put("type", str);

            str = row.get("director").toString();
            map.put("director", str);

            str = row.get("writer").toString().replace("|", ",");
            map.put("writer", str);

            str = row.get("actor").toString().replace("|", ",");
            map.put("actor", str);
        }
        return map;
    }
    /**
     * Say hello to server.
     */
    public void greet(int username) {

        Request request = Request.newBuilder().setUserid(username).build();
        Response response;
        try {
            response = blockingStub.response(request);
            List<Integer> moviesList = response.getMoviesList();
            System.out.println("推荐结果");
            for (Integer integer : moviesList) {
                printMovieInfo(getInfo(integer));
            }
        } catch (StatusRuntimeException ignored) {

        }
    }

    private void printMovieInfo(HashMap<String, String> map){
        for(String key : map.keySet()){
            System.out.println(key + ": " + map.get(key));
        }
        System.out.println("------------------------------------------------------------------------------------------------------------------------");
    }
}
