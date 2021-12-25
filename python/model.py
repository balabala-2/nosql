import os

import tensorflow as tf

os.environ['CUDA_VISIBLE_DEVICES'] = '0'

# Training samples path, change to your local path
training_samples_file_path = tf.keras.utils.get_file("modelInput64_.csv",
                                                     "file:///E:/Recommend_system/traindata/modelOutput64.csv")


tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)


# load sample as tf dataset
def get_dataset(file_path):
    dataset = tf.data.experimental.make_csv_dataset(
        file_path,
        batch_size=64,
        label_name='label',
        na_value="0",
        num_epochs=1,
        ignore_errors=True)

    return dataset


# split as test dataset and training dataset
train_dataset = get_dataset(training_samples_file_path)

# genre features vocabulary
genre_vocab = ['drama', 'musical', 'action', 'film-noir', 'ac125523tion', 'fantasy', 'thriller', 'children',
               'crime', 'sci-fi', 'comedy', 'animation', 'western', 'mystery', 'romance', 'imax', 'war', 'adventure',
               'horror', 'documentary']

GENRE_FEATURES = {
    'userGenre1': genre_vocab,
    'userGenre2': genre_vocab,
    'userGenre3': genre_vocab,
    'userGenre4': genre_vocab,
    'userGenre5': genre_vocab,
    'movieGenre1': genre_vocab,
    'movieGenre2': genre_vocab,
    'movieGenre3': genre_vocab
}

# all categorical features
categorical_columns = []
for feature, vocab in GENRE_FEATURES.items():
    cat_col = tf.feature_column.categorical_column_with_vocabulary_list(
        key=feature, vocabulary_list=vocab)
    emb_col = tf.feature_column.embedding_column(cat_col, 10)
    categorical_columns.append(emb_col)
# movie id embedding feature
movie_col = tf.feature_column.categorical_column_with_identity(key='movieId', num_buckets=300001)
movie_emb_col = tf.feature_column.embedding_column(movie_col, 10)
categorical_columns.append(movie_emb_col)

# user id embedding feature
user_col = tf.feature_column.categorical_column_with_identity(key='userId', num_buckets=300001)
user_emb_col = tf.feature_column.embedding_column(user_col, 10)
categorical_columns.append(user_emb_col)

# all numerical features
numerical_columns = [tf.feature_column.numeric_column('releaseYear'),
                     tf.feature_column.numeric_column('movieRatingCount'),
                     tf.feature_column.numeric_column('movieAvgRating'),
                     tf.feature_column.numeric_column('userRatingCount'),
                     tf.feature_column.numeric_column('userAvgRating'), ]

# embedding + MLP model architecture
model = tf.keras.Sequential([
    tf.keras.layers.DenseFeatures(numerical_columns + categorical_columns),
    tf.keras.layers.Dense(128, activation='relu'),
    tf.keras.layers.Dense(128, activation='relu'),
    tf.keras.layers.Dense(1, activation='sigmoid'),
])

# compile the model, set loss function, optimizer and evaluation metrics
model.compile(
    loss='binary_crossentropy',
    optimizer='adam',
    metrics=['accuracy', tf.keras.metrics.AUC(curve='ROC'), tf.keras.metrics.AUC(curve='PR')])

# train the model
model.fit(train_dataset, epochs=5, use_multiprocessing=True, batch_size=64)

model.save('keras_model_tf_version', save_format='tf')
