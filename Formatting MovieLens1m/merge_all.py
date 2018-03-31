import pandas as pd

genres=pd.read_csv('/Users/manasakumar/Desktop/ml-1m/movieid+genres.csv')
users=pd.read_csv('/Users/manasakumar/Desktop/ml-1m/user_ratings_final.csv')
merged=pd.merge(genres, users, on=['movie_id'])
merged=merged.sort_values('user_id')
merged.to_csv('/Users/manasakumar/Desktop/ml-1m/trial.csv')
