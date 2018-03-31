sc.stop()
import pyspark
from pyspark import SparkContext, SparkConf
sc =SparkContext()
from functools import reduce
import operator as op
import numpy as np

csv_data=sc.textFile("/Users/manasakumar/Desktop/like_proc_train.csv").map(lambda line: line.split(","))
header =csv_data.first() #extract header
csv_data = csv_data.filter(lambda row:row != header)

user_author=csv_data.map(lambda x:(x[0],x[2]))
article_author=csv_data.map(lambda x:(x[1],x[2]))
user_article_rdd=csv_data.map(lambda x:(x[0],x[1])).collect()

user_author_rdd=user_author.mapValues(lambda x:x.split(" "))
article_author_rdd=article_author.mapValues(lambda x:x.split(" ")).collect()

article_count=article_author.map(lambda x: x[0]).distinct().collect()

data1 =user_author_rdd.groupByKey()
user_author_list=data1.map(lambda x: (x[0], list(x[1]))).collect()
user_count=len(user_author_list)

data2=user_author_rdd.combineByKey(lambda value:(value,1),lambda x,value:(x[0]+value,x[1]+1),lambda x,y:(x[0]+y[0],x[1]+y[1]))
user_author_commonlist=data2.collect()

num=[];
lengths=[];
for i in range(0,user_count):
    length=user_author_commonlist[i][1][1]
    lengths.append(length)
    num1=0;
    for j in range(0,length):
        for k in range(j+1,length):
            output=list(set(user_author_list[i][1][j]).intersection(user_author_list[i][1][k]))
            if len(output)>0:
                num1=num1+1
            
    num.append(num1)

def ncr(n, r):
    r = min(r, n-r)
    if r == 0: return 1
    numer = reduce(op.mul, range(n, n-r, -1))
    denom = reduce(op.mul, range(1, r+1))
    return numer/denom

fe1s=[];
for i in range (0,len(lengths)):
    if(num[i]>0):
        den1=ncr(lengths[i],2)
        fe1=num[i]/float(den1)
        
    else:
        fe1=0
    fe1s.append(fe1)
    #print (fe1s[i])


from collections import Counter
fe2s=[]
num_common=[]
for i in range (0,len(lengths)):
    most_common,num_most_common = Counter(user_author_commonlist[i][1][0]).most_common(1)[0]
  
    if(num_most_common>1):
        num_common.append(num_most_common)
    else:
        num_common.append(1)
    
for i in range (0,len(lengths)):  
    fe2=float(num_common[i])/float(lengths[i])
    fe2s.append(fe2)
    #print (fe2s[i])

skim1=[]
count=0;
print max(fe1s)
print max(fe2s)
for i in fe1s:
	count=count+1
	if (i>=1):
		skim1.append(count)
skim2=[]
count=0;
for i in fe2s:
	count=count+1
	if (i>=1):
		skim2.append(count)
        
def intersection(lst1, lst2):
	return list(set(lst1) & set(lst2))

skim_list= intersection(skim1,skim2)
skim = list(map(str,skim_list))
skimmed=csv_data.filter(lambda x: x[0] in (skim))

user_author_skimmed=skimmed.map(lambda x:(x[0],x[2]))
article_author_skimmed=skimmed.map(lambda x:(x[1],x[2]))
user_article_skimmed_rdd=skimmed.map(lambda x:(x[0],x[1])).collect()

user_author_skimmed_rdd=user_author_skimmed.mapValues(lambda x:x.split(" "))
article_author_skimmed_rdd=article_author_skimmed.mapValues(lambda x:x.split(" ")).collect()

article_distinct_skimmed=article_author_skimmed.map(lambda x: int(x[0]) ).distinct().collect()
article_count_skimmed=len(article_distinct_skimmed)
data3 =user_author_skimmed_rdd.groupByKey()
user_author_skimmed_list=data3.map(lambda x: (x[0], list(x[1]))).collect()
user_count_skimmed=len(user_author_skimmed_list)

data4=user_author_skimmed_rdd.combineByKey(lambda value:(value,1),lambda x,value:(x[0]+value,x[1]+1),lambda x,y:(x[0]+y[0],x[1]+y[1]))
user_author_skimmed_commonlist=data4.collect()

import numpy
waa=numpy.zeros((article_count_skimmed,article_count_skimmed),dtype=int)

# finding matrix WAA where WAA(i,j)=WAA(j,i) symmetric matrix Article-Article

for i in range(0,article_count_skimmed):
    for j in range(i,article_count_skimmed):
        output=list(set(article_author_skimmed_rdd[i][1]).intersection(article_author_skimmed_rdd[j][1]))   
        if len(output)>=1:
            waa[j][i]=1
            
#print (waa)


#count=-1
wra=numpy.zeros((user_count_skimmed,article_count_skimmed),dtype=int)  # finding matrix WRA Research-Article
for i in range(0,user_count_skimmed):
    for j in range(0,article_count_skimmed):
        if skim_list[i] == int(user_article_skimmed_rdd[j][0]):
		wra[i][j]=1
#print(wra)
war=wra.transpose()
#print(war)


wrr=numpy.zeros((user_count_skimmed,user_count_skimmed),dtype=int)            #Given in paper Researcher-Researcher is null

sum_ra1=[]   
    
sum_ra=0
for i in range(0,user_count_skimmed):
    sum_ra=0
    for j in range(0,article_count_skimmed):
        sum_ra=sum_ra+wra[i][j]
    sum_ra1.append(sum_ra)
#print(sum_ra1)

sum_ar1=[]   
    
sum_ar=0
for i in range(0,article_count_skimmed):
    sum_ar=0
    for j in range(0,user_count_skimmed):
        sum_ar=sum_ar+war[i][j]
    sum_ar1.append(sum_ar)
#print(sum_ar1)

sum_aa1=[]

sum_aa=0
for i in range(0,article_count_skimmed):
    sum_aa=0  
    for j in range(0,article_count_skimmed):
        sum_aa=sum_aa+waa[i][j]
    sum_aa1.append(sum_aa)
#print(sum_aa1)



tra=numpy.zeros((user_count_skimmed,article_count_skimmed),dtype=float)  # finding matrix WRA Research-Article
numpy.set_printoptions(precision=4)

 #Transition probability moving to an article vertex from researcher
for i in range(0,user_count_skimmed):
    for j in range(0,article_count_skimmed):
        tra[i][j]=wra[i][j]/sum_ra1[i]
#print(tra)



trr=numpy.zeros((user_count_skimmed,user_count_skimmed),dtype=float) 
#print (trr)



tar=numpy.zeros((article_count_skimmed,user_count_skimmed),dtype=float)  
numpy.set_printoptions(precision=4)

for i in range(0,article_count_skimmed):
    for j in range(0,user_count_skimmed):
        tar[i][j]=float(war[i][j])/(float(sum_ar1[i])+float(sum_aa1[i]))
#print(tar)


#Transition probability moving to an article vertex from article vertex

taa=numpy.zeros((article_count_skimmed,article_count_skimmed),dtype=float)  
numpy.set_printoptions(precision=4)

  #Transition probability moving to an article vertex from researcher

for i in range(0,article_count_skimmed):
    for j in range(0,article_count_skimmed):
        taa[i][j]=float(waa[i][j])/(float(sum_ar1[i])+float(sum_aa1[i]))
#print(taa)

t=np.asarray(np.bmat([[trr,tra],[tar,taa]]))
print (t)


g=np.asarray(np.bmat([[wrr,wra],[war,waa]]))
print (g)


#Random walk
#for target researcher vertex- v0


num_vertices=user_count_skimmed+article_count_skimmed

#print (num_vertices)
ScoreArticle=numpy.zeros((article_count_skimmed),dtype=float)
ScoreAll=numpy.zeros((num_vertices),dtype=float)
numpy.set_printoptions(precision=8)
#print(ScoreAll)


print len(skim_list)
print skim_list
all_users_top_20_articles=[]
for index in xrange(len(skim_list)):
    v0=index #0,1,2
    ScoreAll[v0]=1
    #print(ScoreAll)
    maxStep=6
    step=0
    alpha=0.2
    for step in range(maxStep):
        for i in range(num_vertices):
            tmpScore=numpy.zeros((num_vertices),dtype=float)
        for v1 in range(num_vertices):
            for v2 in range(num_vertices):
                tmpScore[v2]=(alpha*ScoreAll[v1]*t[v1][v2])+(tmpScore[v2])
            if (v1==v0):
                tmpScore[v1]=tmpScore[v1]+(1-alpha)
            #print (tmpScore)
        ScoreAll=tmpScore
    #print(ScoreAll)
    ScoreArticle=ScoreAll[user_count_skimmed:user_count_skimmed+article_count_skimmed]


    dic ={k:v for k,v in zip(article_distinct_skimmed,ScoreArticle)}  
    from collections import OrderedDict
    final_list = OrderedDict(sorted(dic.items(),key=lambda kv: kv[1],reverse=True))
  

    counter=0
    user_top_20_articles=[]
    user_top_20_articles.append(skim_list[index])
    user_data=[]
    for i in final_list:
        #print(i," ",dic[i])
        if dic[i]>0.7:
            user_top_20_articles.append(i)
        #counter=counter+1
      
            
    all_users_top_20_articles.append(user_top_20_articles)
        
    #print skim_list[index]
    #print user_top_20_articles
    
#print all_users_top_20_articles
   
import pandas as pd
df_final=pd.DataFrame(all_users_top_20_articles)
#print df_final #first column will be user_id
            
df_final.to_csv('/Users/manasakumar/Desktop/results.csv')


import pandas as pd
df1=pd.read_csv('/Users/manasakumar/Desktop/ml-1m/test.csv')
df2=pd.read_csv('/Users/manasakumar/Desktop/results-2.csv')
user_ids_df=df2['0']
user_ids=list(user_ids_df)
print user_ids
all_user_movie_ids=[]
for index in xrange(len(user_ids)):#len(user_ids)
    single_user_movie_ids=[]
    single_user_id=user_ids[index]
    #if index>single_user_id:
    #    break
    for i in xrange(len(df1)):
        if (df1.iloc[i]['user_id']==single_user_id):
            single_user_movie_ids.append(df1.iloc[i]['movie_id'])
    all_user_movie_ids.append(single_user_movie_ids)
#print all_user_movie_ids

all_user_movie_ids_res=[]
for index2 in xrange(len(df2)): #len(df2)
    single_user_movie_ids=[]
    list_form=list(df2.iloc[index2])
    single_user_movie_ids=list_form[2:]
    all_user_movie_ids_res.append(single_user_movie_ids)
    
#print all_user_movie_ids_res

print 'Checkpoint 1'

predicted_articles=all_user_movie_ids_res
real_articles=all_user_movie_ids


from __future__ import division

def calc_precision(pa,ra):
    len_predicted_list=len(pa)
    ia=set(pa).intersection(ra)
    len_intersected_list=len(ia)
    user_prec=len_intersected_list/len_predicted_list
    return user_prec

def calc_recall(pa,ra):
    len_real_list=len(ra)
    ia=set(pa).intersection(ra)
    len_intersected_list=len(ia)
    user_recall=len_intersected_list/len_real_list
    return user_recall

def calc_f1(user_precision,user_recall):
    num=(2*user_precision*user_recall)
    den=(user_precision+user_recall)
    user_f1=num/den
    return user_f1


total_precision=0
total_recall=0
total_f1=0

num_of_users=len(predicted_articles)

for user in xrange(num_of_users):
    
    pa=predicted_articles[user]
    ra=real_articles[user]
    
    user_precision=calc_precision(pa,ra)
    total_precision+=user_precision
    
    user_recall=calc_recall(pa,ra)
    total_recall+=user_recall
    
    user_f1=calc_f1(user_precision,user_recall)
    total_f1+=user_f1
    
scaled_precision = total_precision/num_of_users
scaled_recall=total_recall/num_of_users
scaled_f1=total_f1/num_of_users
    
print scaled_precision
print scaled_recall
print scaled_f1

