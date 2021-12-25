package online;

import Utils.Redis;
import com.google.protobuf.InvalidProtocolBufferException;
import grpc.grpc_profile.*;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import offline.Embedding;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Server {
    private io.grpc.Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        final Server server = new Server();
        server.start();
        server.blockUntilShutdown();
    }


    private void start() throws IOException {
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new ServerResponseImpl())
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Server.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }));
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}

class ServerResponseImpl extends ServerResponseGrpc.ServerResponseImplBase {
    Embedding embedding;

    public ServerResponseImpl() {
        System.out.println("正在加载服务器配置...");
        embedding = new Embedding();
        HashMap<Integer, Tag> tags = new HashMap<>();
        Set<byte[]> keys = Redis.getInstance().keys(("Tag*").getBytes());
        try {
            for (byte[] key : keys) {
                Tag tag = Tag.parseFrom(Redis.getInstance().get(key));
                tags.put(tag.getTagId(), tag);
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        embedding.tags = tags;

        keys = Redis.getInstance().keys(("movie*").getBytes());
        HashMap<Integer, Float> movies_IDF = new HashMap<>();
        try {
            for (byte[] key : keys) {
                Movie movie = Movie.parseFrom(Redis.getInstance().get(key));
                movies_IDF.put(movie.getMovieId(), movie.getIDF());
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        embedding.movies_IDF = movies_IDF;
        System.out.println("加载完成");
    }


    HashSet<Integer> getResponse(int userId) {
        System.out.println("为user" + userId + "推荐");
        System.out.println("召回...");
        List<Movie_rating> ucf = embedding.ucf_als(userId);
        List<Movie_rating> icf_word2vec = embedding.icf_word2vec(userId);
        List<Movie_rating> icf_als = embedding.icf_als(userId);
        List<Movie_rating> tag = embedding.tag(userId);

        HashSet<Integer> movies = new HashSet<>();
        for (Movie_rating movie_rating : ucf) {
            movies.add(movie_rating.getMovieId());
        }
        for (Movie_rating movie_rating : icf_als) {
            movies.add(movie_rating.getMovieId());
        }
        for (Movie_rating movie_rating : icf_word2vec) {
            movies.add(movie_rating.getMovieId());
        }
        for (Movie_rating movie_rating : tag) {
            movies.add(movie_rating.getMovieId());
        }
        Process proc;

        try {
            System.out.println("打分...");
            StringBuilder s = new StringBuilder("E://Recommend_system//venv3.7//Scripts//python.exe E:\\Recommend_system\\123\\model.py " + userId + " ");
            for (Integer movie : movies) {
                s.append(movie).append(" ");
            }
            movies.clear();
            proc = Runtime.getRuntime().exec(s.toString());// 执行py文件
            InputStream inputStream = proc.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            StringBuilder stringBuilder = new StringBuilder();
            while ((line = in.readLine()) != null) {
                stringBuilder.append(line);
            }
            in.close();
            String substring = stringBuilder.substring(1, stringBuilder.length() - 1);
            String[] split = substring.split(", ");
            for (String s1 : split) {
                movies.add(Integer.parseInt(s1));
            }
            System.out.println("推荐结果:");
            System.out.println(movies);
            return movies;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return movies;
    }

    private Response getMovies(int userId) {
        HashSet<Integer> movies = getResponse(userId);
        Response.Builder builder = Response.newBuilder();
        for (Integer movie : movies) {
            builder.addMovies(movie);
        }
        return builder.build();
    }

    @Override
    public void response(Request req, StreamObserver<Response> responseObserver) {
        Response response = getMovies(req.getUserid());
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
