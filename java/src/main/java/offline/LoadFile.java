package offline;

import Utils.Redis;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;
import grpc.grpc_profile.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LoadFile {
    //根目录
    static final String root_path = "C://Users//0021//Desktop//recommend//src//main//resources//";
    //电影信息F:
    static final String movie_path = root_path + "original_data//movies.csv";
    //用户评分信息
    static final String rating64_path = root_path + "data_split//rating64.csv";
    static final String rating64_movie_path = root_path + "data_split//rating64_movie.csv";
    //用户为电影贴标签数据
    static final String tag_path = root_path + "original_data//tags.csv";
    static final String link_path = root_path + "original_data//links.csv";
    //标签数据
    static final String genome_tags_path = root_path + "original_data//genome-tags.csv";
    static final String genome_scores_path = root_path + "original_data//genome-scores.csv";

    public static void main(String[] args) throws IOException {
        LoadFile loadFile = new LoadFile();
        loadFile.load_movie_genres();
//        loadFile.loadData();
    }

    public void loadData() {
        load_genome_tags(); // tag_name: id
        load_genome_scores(); // relevance_movieId_tagId: relevance
        load_movie(); // movie profile
        load_user(); // user file
        load_Tag(); // Tag
        load_user_tag_relevance(); // user tag relevance
        calculateIDF();
        System.out.println("All data have been loaded.");
    }


    /**
     * 1.
     * tagid: TagName
     * tag_name: id
     */
    public void load_genome_tags() {
        System.out.println("load genome_tags...");
        try {
            CsvReader csvReader = new CsvReader(LoadFile.genome_tags_path, ',', StandardCharsets.UTF_8);
            //去表头
            csvReader.readHeaders();
            while (csvReader.readRecord()) {
                String[] values = csvReader.getValues();

                Redis.getInstance().set("tag" + values[0], values[1]); //tag1
                Redis.getInstance().set("tag_" + values[1], values[0]); //tag_name
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 2.
     * relevance_movieId_tagId: relevance
     */
    public void load_genome_scores() {
        System.out.println("load genome_scores...");
        try {
            CsvReader csvReader = new CsvReader(LoadFile.genome_scores_path, ',', StandardCharsets.UTF_8);
            csvReader.readHeaders();

            String preId = "1";
            MovieTags.Builder tags = MovieTags.newBuilder();
            List<MovieTag> tagsList = new ArrayList<>();
            while (csvReader.readRecord()) {
                String[] values = csvReader.getValues();
                if (!preId.equals(values[0])) {
                    tagsList.sort((t1, t2) -> Float.compare(t1.getRelevance(), t2.getRelevance()));
                    tagsList = tagsList.subList(tagsList.size() - 10, tagsList.size());

                    tags.addAllTags(tagsList);
                    Redis.getInstance().set(("MovieTags" + preId).getBytes(), tags.build().toByteArray());
                    tagsList = new ArrayList<>();
                    tags = MovieTags.newBuilder();
                    preId = values[0];
                }
                tagsList.add(MovieTag.newBuilder().setTagId(Integer.parseInt(values[1]))
                        .setTagName(Redis.getInstance().get("tag" + values[1]))
                        .setRelevance(Float.parseFloat(values[2])).build());
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * message Movie{
     * int32 movieId = 1;
     * string title = 2;
     * int32 releaseYear = 3;
     * <p>
     * repeated MovieTag genres = 4;
     * <p>
     * int32 ratingCount = 5;
     * float avgScore = 6;
     * repeated Rating ratings = 7;
     * repeated Rating topRatings = 8;
     * }
     */
    @Test
    public void load_movie() {
        System.out.println("load movie...");
        try {
            CsvReader movieReader = new CsvReader(LoadFile.movie_path, ',', StandardCharsets.UTF_8);
            CsvReader ratingReader = new CsvReader(LoadFile.rating64_movie_path, ',', StandardCharsets.UTF_8);

            movieReader.readHeaders();
            ratingReader.readHeaders();

            ratingReader.readRecord();

            Pattern pattern = Pattern.compile("[\\d]{4}");

            float sum = 0;

            Movie.Builder movie = Movie.newBuilder();
            while (movieReader.readRecord() || ratingReader.readRecord()) {
                String[] values = movieReader.getValues();
                String preId = values[0];
                movie.setMovieId(Integer.parseInt(values[0]));
                movie.setTitle(values[1]);

                Matcher matcher = pattern.matcher(values[1]);
                if (matcher.find()) {
                    movie.setReleaseYear(Integer.parseInt(matcher.group()));
                }
                if (Redis.getInstance().exists(("MovieTags" + preId).getBytes())) {
                    List<MovieTag> tagsList = MovieTags.parseFrom(Redis.getInstance().get(("MovieTags" + preId).getBytes())).getTagsList();
                    for (MovieTag movieTag : tagsList) {
                        movie.addGenres(movieTag);
                    }
                }
                if (ratingReader.getValues().length > 1 && preId.equals(ratingReader.getValues()[1])) {
                    do {
                        values = ratingReader.getValues();
                        if (!preId.equals(values[1])) {
                            movie.setRatingCount(movie.getRatingsList().size());
                            movie.setAvgScore(sum / movie.getRatingCount());

                            Redis.getInstance().set(("movie" + preId).getBytes(), movie.build().toByteArray());

                            movie = Movie.newBuilder();
                            movie.addRatings(Rating.newBuilder()
                                    .setUserId(Integer.parseInt(values[0]))
                                    .setMovieId(Integer.parseInt(values[1]))
                                    .setScore(Float.parseFloat(values[2]))
                                    .setTimestamp(Long.parseLong(values[3])).build());
                            sum = 0;
                            break;
                        }
                        sum += Float.parseFloat(values[2]);
                        movie.addRatings(Rating.newBuilder().setUserId(Integer.parseInt(values[0])).setMovieId(Integer.parseInt(values[1]))
                                .setScore(Float.parseFloat(values[2])).setTimestamp(Long.parseLong(values[3])).build());
                    } while (ratingReader.readRecord());
                } else {
                    Redis.getInstance().set(("movie" + preId).getBytes(), movie.build().toByteArray());
                    movie = Movie.newBuilder();
                }
            }

            Set<byte[]> keys = Redis.getInstance().keys(("movie*").getBytes());
            try {
                Movie movie_;
                int movieNum = keys.size();
                for (byte[] key : keys) {
                    movie_ = Movie.parseFrom(Redis.getInstance().get(key));
                    movie_ = Movie.newBuilder(movie_).setIDF((float) Math.log(movieNum / (movie_.getRatingCount() + 1.0))).build();
                    Redis.getInstance().set(key, movie_.toByteArray());
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void load_movie_genres() {
        System.out.println("load movie genres...");
        try {
            CsvReader movieReader = new CsvReader(LoadFile.movie_path, ',', StandardCharsets.UTF_8);

            movieReader.readHeaders();

            while (movieReader.readRecord()) {
                Movie_genres.Builder builder = Movie_genres.newBuilder();

                String[] values = movieReader.getValues();
                builder.setMovieId(Integer.parseInt(values[0]));
                String[] split = values[2].split("\\|");
                for (String s : split) {
                    builder.addGenre(s.toLowerCase());
                }
                Movie_genres build = builder.build();
                Redis.getInstance().set(("Movie_genres" + values[0]).getBytes(), build.toByteArray());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * message User{
     * int32 userId = 1;
     * double avgRating = 2;
     * double highestRating = 3;
     * double lowestRating = 4;
     * int32 ratingCount = 5;
     * repeated Rating ratings = 6;
     * }decimalFormat
     */
    @Test
    public void load_user() {
        System.out.println("load user...");
        try {
            CsvReader csvReader = new CsvReader(LoadFile.rating64_path, ',', StandardCharsets.UTF_8);
            csvReader.readHeaders();

            String preId = "1";

            User.Builder user = User.newBuilder();
            double sum = 0;
            while (csvReader.readRecord()) {
                String[] values = csvReader.getValues();

                if (!preId.equals(values[0])) {
                    user.setRatingCount(user.getRatingsList().size());
                    user.setAvgRating(sum / user.getRatingCount());
                    user.setUserId(Integer.parseInt(preId));

                    Redis.getInstance().set(("user" + preId).getBytes(), user.build().toByteArray());

                    user = User.newBuilder();
                    user.setUserId(Integer.parseInt(values[0]));

                    preId = values[0];
                    sum = 0;
                }
                user.addRatings(Rating.newBuilder().setUserId(Integer.parseInt(values[0])).setMovieId(Integer.parseInt(values[1]))
                        .setScore(Float.parseFloat(values[2])).setTimestamp(Long.parseLong(values[3])).build());
                sum += Float.parseFloat(values[2]);

            }
            Redis.getInstance().set(("user" + preId).getBytes(), user.build().toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }

        Set<byte[]> keys = Redis.getInstance().keys(("user*").getBytes());
        for (byte[] key : keys) {
            try {
                User user = User.parseFrom(Redis.getInstance().get(key));
                User.Builder builder = User.newBuilder(user);
                builder.setRatingCount(user.getRatingsList().size());
                List<Rating> ratingsList = user.getRatingsList();
                double sum = 0.0;
                for (Rating rating : ratingsList) {
                    sum += rating.getScore();
                }
                builder.setAvgRating(sum / ratingsList.size());
                User build = builder.build();
                Redis.getInstance().set(key, build.toByteArray());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }

        load_user_tag_relevance();
    }

    /**
     * message Tag{
     * repeated Movie_relevance movies = 1;
     * int32 tagId = 2;
     * string tagName = 3;
     * }
     */
    @Test
    public void load_Tag() {
        System.out.println("load tag...");
        try {
            Tag.Builder tag;

            Set<byte[]> keys = Redis.getInstance().keys(("movie*").getBytes());
            // load the tags
            for (byte[] key : keys) {
                Movie movie = Movie.parseFrom(Redis.getInstance().get(key));
                List<MovieTag> genresList = movie.getGenresList();
                for (MovieTag movieTag : genresList) {
                    int tagId = Integer.parseInt(Redis.getInstance().get("tag_" + movieTag.getTagName()));
                    float relevance = movieTag.getRelevance();
                    if (Redis.getInstance().exists("Tag" + tagId)) {
                        tag = Tag.parseFrom(Redis.getInstance().get(("Tag" + tagId).getBytes())).toBuilder();
                    } else {
                        tag = Tag.newBuilder();
                        tag.setTagName(movieTag.getTagName());
                        tag.setTagId(tagId);
                    }
                    tag.addMovies(Movie_relevance.newBuilder().setMovieId(movie.getMovieId())
                            .setRelevance(relevance)
                            .setAvgScore(movie.getAvgScore())
                            .setReleaseYear(movie.getReleaseYear())
                            .setTitle(movie.getTitle()).build());
                    Tag tag_ = sortTags(tag);
                    Redis.getInstance().set(("Tag" + tagId).getBytes(), tag_.toByteArray());
                }
            }
            //sort tags by relevance
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * sort the Tag movies by relevance
     *
     * @param tag tag builder
     * @return tag
     */
    public Tag sortTags(Tag.Builder tag) {
        List<Movie_relevance> moviesList = new ArrayList<>(tag.getMoviesList());
        moviesList.sort((o1, o2) -> Float.compare(o2.getRelevance(), o1.getRelevance()));
        tag.clearMovies();
        for (Movie_relevance movie_relevance : moviesList) {
            tag.addMovies(movie_relevance);
        }
        return tag.build();
    }

    /**
     * get movieAvgRating.csv, it has two columns
     * movieId, avgRating
     *
     * @throws IOException csv
     */
    public void writeRatingMovieAvgRating() throws IOException {
        CsvReader csvReader = new CsvReader(movie_path);
        csvReader.readHeaders();
        CsvWriter csvWriter = new CsvWriter(root_path + "data//movieAvgRating.csv");
        String[] values = new String[2];
        values[0] = "movieId";
        values[1] = "AvgRating";
        csvWriter.writeRecord(values);

        while (csvReader.readRecord()) {
            values = csvReader.getValues();
            System.out.println(values[0]);
            Movie movie = Movie.parseFrom(Redis.getInstance().get(("movie" + values[0]).getBytes()));
            String[] val = new String[2];
            val[0] = movie.getMovieId() + "";
            val[1] = movie.getAvgScore() + "";
            csvWriter.writeRecord(val);
        }
        csvReader.close();
        csvWriter.close();
    }

    @Test
    public void load_user_tag_relevance() {
        System.out.println("load user_tag_relevance...");
        Set<byte[]> userKeys = Redis.getInstance().keys(("user*").getBytes());
        Set<byte[]> movieKeys = Redis.getInstance().keys(("movie*").getBytes());
        Set<byte[]> keys = Redis.getInstance().keys(("Movie_genres*").getBytes());
        HashMap<Integer, Movie> movies = new HashMap<>();
        HashMap<Integer, Movie_genres> movie_genres = new HashMap<>();
        try {
            //load movies
            for (byte[] movieKey : movieKeys) {
                Movie movie = Movie.parseFrom(Redis.getInstance().get(movieKey));
                movies.put(movie.getMovieId(), movie);
            }
            for (byte[] key : keys) {
                Movie_genres movieGenres = Movie_genres.parseFrom(Redis.getInstance().get(key));
                movie_genres.put(movieGenres.getMovieId(), movieGenres);
            }

            //traverse users
            int i = 0;
            for (byte[] key : userKeys) {
                System.out.println(i++);
                if (!Redis.getInstance().exists(key))
                    continue;
                User user = User.parseFrom(Redis.getInstance().get(key));
                HashMap<Integer, Integer> map = new HashMap<>();

                List<Rating> ratingsList = user.getRatingsList();
                //traverse user ratings
                for (Rating rating : ratingsList) {
                    Movie movie = movies.get(rating.getMovieId());

                    if (movie == null)
                        continue;

                    ProtocolStringList genreList = movie_genres.get(movie.getMovieId()).getGenreList();

                    for (String s : genreList) {
                        if(Redis.getInstance().exists("tag_" + s)){
                            int tag_id = Integer.parseInt(Redis.getInstance().get("tag_" + s));
                            if (map.containsKey(tag_id))
                                map.put(tag_id, map.get(tag_id) + 1);
                            else {
                                map.put(tag_id, 1);
                            }
                        }
                    }
                }
                List<HashMap.Entry<Integer, Integer>> list = new ArrayList<>(map.entrySet());
                list.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

                User.Builder builder = User.newBuilder(user);
                builder.clearTags();
                for (Map.Entry<Integer, Integer> integerIntegerEntry : list) {
                    builder.addTags(User_tag_relevance
                            .newBuilder()
                            .setTagId(integerIntegerEntry.getKey())
                            .setRelevance(integerIntegerEntry.getValue()).build());
                }

                user = builder.build();
                Redis.getInstance().set(key, user.toByteArray());
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void removeUserTag() throws InvalidProtocolBufferException {
        Set<byte[]> keys = Redis.getInstance().keys(("user*").getBytes());
        for (byte[] key : keys) {
            User user = User.parseFrom(Redis.getInstance().get(key));
            user = User.newBuilder(user).clearTags().build();
            Redis.getInstance().set(key, user.toByteArray());
        }
    }

    /**
     * based on TF-IDF algorithm
     * calculate the IDF value
     */
    @Test
    public void calculateIDF() {
        System.out.println("load IDF...");
        //从redis中读取movie对象
    }


    @Test
    public void func_movie() {
        Set<byte[]> keys = Redis.getInstance().keys(("movie*").getBytes());
        for (byte[] key : keys) {
            try {
                Movie movie = Movie.parseFrom(Redis.getInstance().get(key));
                Movie.Builder builder = Movie.newBuilder(movie);
                builder.setRatingCount(movie.getRatingsList().size());
                List<Rating> ratingsList = movie.getRatingsList();
                double sum = 0.0;
                for (Rating rating : ratingsList) {
                    sum += rating.getScore();
                }
                builder.setAvgScore((float) (sum / ratingsList.size()));
                Movie build = builder.build();
                Redis.getInstance().set(key, build.toByteArray());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void f() {
//        Set<byte[]> keys = Redis.getInstance().keys(("user162541").getBytes());
        Set<byte[]> keys = Redis.getInstance().keys(("user*").getBytes());
        for (byte[] key : keys) {
            try {
                User user = User.parseFrom(Redis.getInstance().get(key));
                if (user.getUserId() == 162541) {
                    System.out.println(1);
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }

}
