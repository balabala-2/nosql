import tensorflow as tf

# Test samples path, change to your local path
# test_samples_file_path = tf.keras.utils.get_file("modelInput20_.csv",
#                                                  "file:///E:/Recommend_system/traindata/modelOutput20.csv")
test_samples_file_path = tf.keras.utils.get_file("demo.csv",
                                                 "file:///E:/Recommend_system/modelData/demo.csv")
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)


# load sample as tf dataset
def get_dataset(file_path):
    dataset = tf.data.experimental.make_csv_dataset(
        file_path,
        batch_size=12,
        label_name='label',
        na_value="0",
        num_epochs=1,
        ignore_errors=True)
    return dataset


test_dataset = get_dataset(test_samples_file_path)

new_model = tf.keras.models.load_model('keras_model_tf_version')
test_loss, test_accuracy, test_roc_auc, test_pr_auc = new_model.evaluate(test_dataset, use_multiprocessing=True)
print('\n\nTest Loss {}, Test Accuracy {}, Test ROC AUC {}, Test PR AUC {}'.format(test_loss, test_accuracy,
                                                                                   test_roc_auc, test_pr_auc))
predictions = new_model.predict(test_dataset)

for prediction, goodRating in zip(predictions, list(test_dataset)[0][1]):
    print("Predicted good rating: {:.2%}".format(prediction[0]),
          " | Actual rating label: ",
          ("Good Rating" if bool(goodRating) else "Bad Rating"))
