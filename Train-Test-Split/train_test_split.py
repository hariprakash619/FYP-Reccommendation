import pandas as pd
import csv
import operator as op
import numpy
from scipy.special import comb

#user.id - user_id
#doc.id - movie_id
#authors - genres_list

import itertools 

df1=pd.read_csv('/Users/manasakumar/Desktop/ml-1m/trial.csv')
num=len(df1)


user_id=1
yoo=[]
whole_yoo=[]
for i in xrange(num):#num
    now_user_id=df1.iloc[i]['user_id']
    
    if not (now_user_id==user_id):
       
        whole_yoo.append(yoo)
        user_id+=1
        yoo=[]
    else:
        yu=list(df1.iloc[i])
        yoo.append(yu)

whole_yoo.append(yoo)    

num_users=user_id
df_train=pd.DataFrame()
df_test=pd.DataFrame()
for index in xrange(num_users): #num_users
    #####single user
    df_train_single=[]
    df_test_single=[]
    len_now=len(whole_yoo[index])
    divis=.8*len_now
    split_index=int(divis)
   
 
    
    
    df_train_single=pd.DataFrame(whole_yoo[index][0:split_index])
    
    df_test_single=pd.DataFrame(whole_yoo[index][split_index:])
    
    df_train=df_train.append(df_train_single)
    df_test=df_test.append(df_test_single)
    
#print df_train
#print df_test

df_train.to_csv('/users/manasakumar/ml-1m/train.csv')
df_test.to_csv('/users/manasakumar/ml-1m/test.csv')

    
    
    
df_train.to_csv('/users/manasakumar/Desktop/ml-1m/train.csv')
df_test.to_csv('/users/manasakumar/Desktop/ml-1m/test.csv')


df_train.columns = ['not_req', 'movie_id', 'user_id', 'genres_list']
df_train.to_csv('/users/manasakumar/Desktop/ml-1m/train.csv')
df_test.columns = ['not_req', 'movie_id', 'user_id', 'genres_list']
df_test.to_csv('/users/manasakumar/Desktop/ml-1m/test.csv')



print len(df_train)
print len(df_test)
