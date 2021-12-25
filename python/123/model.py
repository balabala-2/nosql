import os
import sys

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

import pandas as pd
import tensorflow as tf

tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
os.environ["CUDA_VISIBLE_DEVICES"] = "1, 2, 3"

model = tf.keras.models.load_model('E://Recommend_system//keras_model_tf_version')


def recommend(userId, movieIds):
    movies = pd.read_csv('E://Recommend_system//modelData//modelInput_movie.csv')
    users = pd.read_csv('E://Recommend_system//modelData//modelInput_user.csv')

    users = users[users['userId'] == userId]
    users = users.append([users.loc[users.index[0]] for _ in range(len(movieIds) - 1)])

    users['movieId'] = movieIds
    movies = movies[movies['movieId'].isin(movieIds)]
    df = pd.merge(users, movies, on=None)
    df.to_csv('C://Users//0021//.keras//datasets//data.csv', index=False)
    dataset = tf.data.experimental.make_csv_dataset(
        tf.keras.utils.get_file("data.csv", 'file:///C://Users//0021//.keras//datasets//data.csv'),
        batch_size=1,
        na_value="0",
        num_epochs=1,
        ignore_errors=True)
    predictions = list(model.predict(dataset).reshape((1, -1))[0])
    results = zip(movieIds, predictions)
    results = sorted(results, key=lambda x: x[1])
    return [result[0] for result in results[-5:]]

if __name__ == '__main__':
    userId = int(sys.argv[1])
    movieIds = []
    for i in range(2, len(sys.argv)):
        movieIds.append((int(sys.argv[i])))

    print(recommend(userId, movieIds))
