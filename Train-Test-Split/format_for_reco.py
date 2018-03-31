import pandas as pd

df1=pd.read_csv('/users/manasakumar/Desktop/ml-1m/train.csv')

dfnew_user=pd.DataFrame()
dfnew_user['user.id']=df1['user_id']

dfnew_user['doc.id']=df1['movie_id']

dfnew_user['authors']=df1['genres_list']

dfnew_user.to_csv('/Users/manasakumar/Desktop/ml-1m/like_proc_train.csv',index=False)
