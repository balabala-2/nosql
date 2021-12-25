package Utils;

import com.csvreader.CsvWriter;
import com.google.protobuf.InvalidProtocolBufferException;
import grpc.grpc_profile.Movie;
import grpc.grpc_profile.Movie_genres;
import grpc.grpc_profile.RecommendResult;
import grpc.grpc_profile.User;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Model {
    //根目录
    static final String root_path = "C://Users//0021//Desktop//recommend//src//main//resources//";
    //用户评分信息
    static final String rating64_path = root_path + "data_split//rating64.csv";
    static final String rating16_path = root_path + "data_split//rating16.csv";
    static final String rating20_path = root_path + "data_split//rating20.csv";

    public static void main(String[] args) {
        Model model = new Model();
    }

    @Test
    public void saveRecallResult() throws IOException {
        Set<byte[]> keys = Redis.getInstance().keys(("RecommendResult*").getBytes());
        CsvWriter csvWriter = new CsvWriter(root_path + "recall.csv");
        String[] head = {"userId", "movieId"};
        csvWriter.writeRecord(head);
        for (byte[] key : keys) {
            try {
                RecommendResult recommendResult = RecommendResult.parseFrom(Redis.getInstance().get(key));
                List<Integer> movieIdsList = recommendResult.getMovieIdsList();
                for (Integer integer : movieIdsList) {
                    String[] row = {String.valueOf(recommendResult.getUserId()), String.valueOf(integer)};
                    csvWriter.writeRecord(row);
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
        csvWriter.close();
    }


    @Test
    public void file() {
        try {
            CsvWriter csvWriter_user = new CsvWriter(root_path + "modelInput_user.csv");
            CsvWriter csvWriter_movie = new CsvWriter(root_path + "modelInput_movie.csv");
            String[] head_user = {"userId", "userRatingCount", "userAvgRating", "userGenre1", "userGenre2", "userGenre3", "userGenre4", "userGenre5"};

            String[] head_movie = {"movieId", "movieGenre1", "movieGenre2", "movieGenre3", "releaseYear", "movieRatingCount", "movieAvgRating"};

            csvWriter_user.writeRecord(head_user);
            csvWriter_movie.writeRecord(head_movie);

            HashMap<Integer, Movie_genres> movie_genres = new HashMap<>();
            Set<byte[]> keys = Redis.getInstance().keys(("user*").getBytes());
            DecimalFormat decimalFormat = new DecimalFormat("0.000");
            for (byte[] key : keys) {
                String[] row_user = new String[8];
                User user = User.parseFrom(Redis.getInstance().get(key));
                row_user[0] = String.valueOf(user.getUserId());
                row_user[1] = String.valueOf(user.getRatingCount());
                row_user[2] = String.valueOf(decimalFormat.format(user.getAvgRating()));
                int end = Math.min(user.getTagsList().size(), 5);
                for (int i = 0; i < end; i++) {
                    row_user[3 + i] = Redis.getInstance().get("tag" + user.getTags(i).getTagId());
                }
                for (int i = end; i < 5; i++) {
                    row_user[3 + i] = "";
                }
                csvWriter_user.writeRecord(row_user);
            }
            csvWriter_user.close();

            keys = Redis.getInstance().keys(("Movie_genres*").getBytes());
            for (byte[] key : keys) {
                Movie_genres genres = Movie_genres.parseFrom(Redis.getInstance().get(key));
                movie_genres.put(genres.getMovieId(), genres);
            }

            keys = Redis.getInstance().keys(("movie*").getBytes());
            for (byte[] key : keys) {
                Movie movie = Movie.parseFrom(Redis.getInstance().get(key));
                String[] row_movie = new String[7];
                row_movie[0] = String.valueOf(movie.getMovieId());
                int end = 0;
                try {
                    end = Math.min(movie_genres.get(movie.getMovieId()).getGenreCount(), 3);
                    for (int i = 0; i < end; i++) {
                        String genre = movie_genres.get(movie.getMovieId()).getGenre(i);
                        row_movie[1 + i] = genre.equals("(no genres listed)") ? "" : genre;
                    }
                } catch (NullPointerException e) {
                    for (int i = end; i < 3; i++) {
                        row_movie[1 + i] = "";
                    }
                }
                row_movie[4] = String.valueOf(movie.getReleaseYear());
                row_movie[5] = String.valueOf(movie.getRatingCount());
                row_movie[6] = String.valueOf(decimalFormat.format(movie.getAvgScore()));
                csvWriter_movie.writeRecord(row_movie);
            }
            csvWriter_movie.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
