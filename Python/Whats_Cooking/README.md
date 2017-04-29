What's Cooking?

Use recipe ingredients to categorize the cuisine

Data descriptions:

train.json - the training set containing recipes id, type of cuisine, and list of ingredients
test.json - the test set containing recipes id, and list of ingredients

## Agenda

1. Reading in and exploring the data from Kaggle
2. Feature engineering
3. Model evaluation using **`train_test_split`** and **`cross_val_score`**
4. Making predictions for new data
5. Searching for optimal tuning parameters using **`GridSearchCV`**
6. Extracting features from text using **`CountVectorizer`**
7. Chaining steps into a **`Pipeline`**
8. Combining **`GridSearchCV`** with **`Pipeline`**
9. Efficiently search for tuning parameters using **`RandomizedSearchCV`**
10. Adding features to a document-term matrix (using **`FeatureUnion`**)
11. Ensembling models
12. Calculating cuisine similarity