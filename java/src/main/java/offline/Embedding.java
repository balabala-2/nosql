package offline;

import Utils.Redis;
import com.google.protobuf.InvalidProtocolBufferException;
import grpc.grpc_profile.*;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;

public class Embedding {
    public Word2VecModel i2iModel;
    public HashMap<Integer, Tag> tags;
    public HashMap<Integer, Float> movies_IDF;
    public HashMap<Integer, User> users;

    int icf_word2vec_num = 7;
    int icf_als_num = 7; // 改这个需要去spark.embedding下面修改loadAlsU2I
    int ucf_als_num = 7; // 改这个需要去spark.embedding下面修改loadUserEmb
    int tag_num = 7;


    public Embedding() {
        System.out.println("加载user");
        Set<byte[]> keys = Redis.getInstance().keys(("user*").getBytes());
        users = new HashMap<>();
        try {
            for (byte[] key : keys) {
                byte[] bytes = Redis.getInstance().get(key);
                User user = User.parseFrom(bytes);
                users.put(user.getUserId(), user);
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        SparkSession spark = SparkSession
                .builder()
                .config(new SparkConf()
                        .setMaster("local")
                        .setAppName("embedding")
                        .set("spark.submit.deployMode", "client"))
                .getOrCreate();
        String root_path = "C://Users//0021//Desktop//recommend//src//main//resources//";
        System.out.println("加载模型");
        String userModel_path = root_path + "model//userEmbModel";
        i2iModel = Word2VecModel.load(spark.sparkContext(), userModel_path);
    }

    /**
     * icf
     *
     * @param userId: 1, 2, 3
     * @return movie id list
     */
    public List<Movie_rating> icf_word2vec(int userId) {
        List<Movie_rating> movieIds = new ArrayList<>();

        User user = users.get(userId);
        ArrayList<Rating> ratingsList = new ArrayList<>(user.getRatingsList());
        ratingsList.sort((o1, o2) -> -Float.compare(o1.getScore(), o2.getScore()));
        int size = Math.min(ratingsList.size(), 4);
        for (int i = 0; i < size; i++) {
            if (ratingsList.get(i).getScore() < 3.5)
                break;
            //存
            Tuple2<String, Object>[] synonyms;
            try {
                synonyms = i2iModel.findSynonyms(ratingsList.get(i).getMovieId() + "", 2);
            } catch (IllegalStateException e) {
                continue;
            }
            for (Tuple2<String, Object> synonym : synonyms) {
                Movie_rating movie_rating = Movie_rating.newBuilder().
                        setMovieId(Integer.parseInt(synonym._1)).
                        setRating(Float.parseFloat(synonym._2.toString())).build();
                movieIds.add(movie_rating);
            }
        }

        movieIds.sort((o1, o2) -> Float.compare(o1.getRating(), o2.getRating()));
        return movieIds.subList(0, Math.min(movieIds.size(), icf_word2vec_num));
    }

    /**
     * use als algorithm to carry out ucf and icf
     * ucf: input the userid, return the movies with respect to the users which are similar to the specific user.
     * use tf-idf algorithm to reduce the weight of hot movies
     *
     * @param userId
     * @return
     */
    public List<Movie_rating> icf_als(int userId) {
        // 50
        //icf
        U2I u2I;
        List<Movie_rating> movies = new ArrayList<>();

        try {
            u2I = U2I.parseFrom(Redis.getInstance().get(("U2I" + userId).getBytes()));
        } catch (InvalidProtocolBufferException | NullPointerException e) {
            return movies;
        }
        List<RecommendMovies> moviesList = u2I.getMoviesList().subList(0, 7);

        for (RecommendMovies recommendMovies : moviesList) {
            movies.add(Movie_rating.newBuilder().setMovieId(recommendMovies.getMovieIds()).setRating(recommendMovies.getRating()).build());
        }
        return movies;
    }


    /**
     * usf: calculate the similarity between users
     * @param userId
     * @return
     */
    public List<Movie_rating> ucf_als(int userId) {
        // 50
        List<Movie_rating> movies = new ArrayList<>();
        List<Integer> userIdsList;
        try {
            userIdsList = U2U.parseFrom(Redis.getInstance().get(("U2U" + userId).getBytes())).getUserIdsList();
        } catch (InvalidProtocolBufferException | NullPointerException e) {
            return movies;
        }
        User user;

        for (Integer integer : userIdsList) {
            user = users.get(integer);
            if (user == null) {
                continue;
            }
            int userMovieCount = user.getRatingCount();
            ArrayList<Rating> ratingsList = new ArrayList<>(user.getRatingsList());
            ratingsList.sort(((o1, o2) -> -Float.compare(o1.getScore(), o2.getScore())));
            int size = Math.min(ratingsList.size(), 1);
            for (int i = 0; i < size; i++) {
                if (ratingsList.get(i).getScore() < 3.5)
                    break;
                float TF = (float) (1.0 / userMovieCount);

                float IDF = 1;
                if(movies_IDF.containsKey(ratingsList.get(i).getMovieId())){
                    IDF = movies_IDF.get(ratingsList.get(i).getMovieId());
                }
                Movie_rating movie_rating = Movie_rating.newBuilder().
                        setMovieId(ratingsList.get(i).getMovieId()).
                        setRating(ratingsList.get(i).getScore() * TF * IDF).build();
                movies.add(movie_rating);
            }
        }
        return movies;
    }

    public List<Movie_rating> tag(int userId) {
        User user = users.get(userId);
        List<User_tag_relevance> tagsList = user.getTagsList();
        List<Movie_rating> movie_ratings = new ArrayList<>();
        for (User_tag_relevance user_tag_relevance : tagsList) {
            List<Movie_relevance> moviesList = tags.get(user_tag_relevance.getTagId()).getMoviesList();

            int tagMovieNum = 1;
            int end = Math.min(moviesList.size(), tagMovieNum);
            for (int i = 0; i < end; i++) {
                movie_ratings.add(Movie_rating.newBuilder()
                        .setMovieId(moviesList.get(i).getMovieId())
                        .setRating(moviesList.get(i).getRelevance()).build());
            }
        }
        return movie_ratings;
    }

    private float calculateSimilarity(UserEmb emb1, UserEmb emb2){
        float dotProduct = 0;
        float denominator1 = 0;
        float denominator2 = 0;
        List<Float> embList = emb1.getEmbList();
        List<Float> embList1 = emb2.getEmbList();
        for (int i = 0; i < embList.size(); i++){
            dotProduct += embList.get(i) * embList1.get(i);
            denominator1 += embList.get(i) * embList.get(i);
            denominator2 += embList1.get(i) * embList1.get(i);
        }
        return (float) (dotProduct / (Math.sqrt(denominator1) * Math.sqrt(denominator2)));
    }

    static class UserSimilarity{
        int userId;
        float similarity;
        public UserSimilarity(int userId, float similarity) {
            this.userId = userId;
            this.similarity = similarity;
        }
    }
    @Test
    public void calU2U() {
        Set<byte[]> keys = Redis.getInstance().keys(("UserEmb*").getBytes());
        List<UserEmb> embs = new ArrayList<>();
        for (byte[] key : keys) {
            try {
                embs.add(UserEmb.parseFrom(Redis.getInstance().get(key)));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
        int i = 0;
        for (UserEmb emb : embs) {
            System.out.println(i++);
            U2U.Builder builder = U2U.newBuilder().setUserId(emb.getUserID());
            PriorityQueue<UserSimilarity> userSimilarities = new PriorityQueue<>(
                    ((o1, o2) -> Float.compare(o1.similarity, o2.similarity)));
            for (UserEmb userEmb : embs) {
                if(userEmb.getUserID() != emb.getUserID()){
                    userSimilarities.add(new UserSimilarity(userEmb.getUserID(), calculateSimilarity(emb, userEmb)));
                }
                if(userSimilarities.size() > 10){
                    userSimilarities.poll();
                }
            }
            for (UserSimilarity userSimilarity : userSimilarities) {
                builder.addUserIds(userSimilarity.userId);
            }
            U2U build = builder.build();
            Redis.getInstance().set(("U2U" + emb.getUserID()).getBytes(), build.toByteArray());
        }
    }
}