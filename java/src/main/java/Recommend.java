import Utils.Redis;
import com.google.protobuf.InvalidProtocolBufferException;
import grpc.grpc_profile.Movie;
import grpc.grpc_profile.Movie_rating;
import grpc.grpc_profile.RecommendResult;
import grpc.grpc_profile.Tag;
import offline.Embedding;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Recommend {
    Embedding embedding;
    int size;
    int threadNum = 5;

    public Recommend() {
        embedding = new Embedding();
        size = Redis.getInstance().keys(("user*").getBytes()).size();
    }

    public static void main(String[] args) {
        System.out.println("加载中...");
        Recommend main = new Recommend();
        System.out.println("加载完毕");

        main.loadTagRecommendResults();
        main.loadICFAlsRecommendResult();

        main.loadICFWord2vecRecommendResult();
        main.loadUCFAlsRecommendResults();
    }

    @Test
    public void del() {
        Set<String> keys = Redis.getInstance().keys(("RecommendResult*"));
        for (String key : keys) {
            Redis.getInstance().del(key);
        }
        System.out.println("删除完成.");
    }

    HashSet<Integer> getMovies(int userId) {
        List<Movie_rating> ucf = embedding.ucf_als(userId);
        List<Movie_rating> icf_als = embedding.icf_als(userId);
        List<Movie_rating> icf_word2vec = embedding.icf_word2vec(userId);
        List<Movie_rating> tag = embedding.tag(userId);

        HashSet<Integer> movies = new HashSet<>();
        for (Movie_rating movie_rating : ucf) {
            movies.add(movie_rating.getMovieId());
        }
        for (Movie_rating movie_rating : icf_als) {
            movies.add(movie_rating.getMovieId());
        }
        for (Movie_rating movie_rating : tag) {
            movies.add(movie_rating.getMovieId());
        }
        for (Movie_rating movie_rating : icf_word2vec) {
            movies.add(movie_rating.getMovieId());
        }

        return movies;
    }

    /**
     * 1. first to execute cause this function is fast
     * load recommend results by tag
     */
    public void loadTagRecommendResults() {
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
        for (int i = 1; i <= size; i++) {
            RecommendResult.Builder builder = RecommendResult.newBuilder();
            List<Movie_rating> tag = embedding.tag(i);
            builder.setUserId(i);
            for (Movie_rating movie_rating : tag) {
                builder.addMovieIds(movie_rating.getMovieId());
            }
            RecommendResult build = builder.build();
            Redis.getInstance().set(("RecommendResult" + i).getBytes(), build.toByteArray());
        }
        System.out.println("基于Tag推荐加载完成");
        embedding.tags = null;
    }

    public void loadUCFAlsRecommendResults() {
        Set<byte[]> keys = Redis.getInstance().keys(("movie*").getBytes());
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
        for (int i = 1; i <= size; i += 1) {
            System.out.println(i);
            RecommendResult recommendResult = null;
            try {
                recommendResult = RecommendResult.parseFrom(Redis.getInstance().get(("RecommendResult" + i).getBytes()));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            RecommendResult.Builder builder = RecommendResult.newBuilder(recommendResult);
            List<Movie_rating> movie_ratings = embedding.ucf_als(i);
            for (Movie_rating movie_rating : movie_ratings) {
                builder.addMovieIds(movie_rating.getMovieId());
            }
            System.out.println(i);
            Redis.getInstance().set(("RecommendResult" + i).getBytes(), builder.build().toByteArray());
        }

        embedding.movies_IDF = null;
    }

    public void loadICFWord2vecRecommendResult() {
        for (int i = 1; i <= size; i += 1) {
            System.out.println(i);
            RecommendResult recommendResult = null;
            try {
                recommendResult = RecommendResult.parseFrom(Redis.getInstance().get(("RecommendResult" + i).getBytes()));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            RecommendResult.Builder builder = RecommendResult.newBuilder(recommendResult);
            List<Movie_rating> tag = embedding.icf_word2vec(i);
            for (Movie_rating movie_rating : tag) {
                builder.addMovieIds(movie_rating.getMovieId());
            }
            Redis.getInstance().set(("RecommendResult" + i).getBytes(), builder.build().toByteArray());
        }
    }


    public void loadICFAlsRecommendResult() {
        for (int i = 1; i <= size; i += 1) {
            System.out.println(i);
            RecommendResult recommendResult = null;
            try {
                recommendResult = RecommendResult.parseFrom(Redis.getInstance().get(("RecommendResult" + i).getBytes()));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            RecommendResult.Builder builder = RecommendResult.newBuilder(recommendResult);
            List<Movie_rating> tag = embedding.icf_als(i);
            for (Movie_rating movie_rating : tag) {
                builder.addMovieIds(movie_rating.getMovieId());
            }
            Redis.getInstance().set(("RecommendResult" + i).getBytes(), builder.build().toByteArray());
        }
        System.out.println("加载完成");
    }



}
