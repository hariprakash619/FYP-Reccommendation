with open('/Users/manasakumar/Desktop/ml-1m/movies.dat', 'r') as input_file:
    lines = input_file.readlines()
    newLines = []
    total_list=[]
    for line in lines:
        single_movie=[]
        newLine = line.split('::')
        genres_single_movie_list=newLine[2].strip().split("|")

        single_movie.append(newLine[0])
        genres_single_movie_list = [w.replace("Action",'0') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Adventure",'1') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Animation", '2') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Children's", '3') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Comedy", '4') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Crime", '5') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Documentary", '6') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Drama", '7') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Fantasy", '8') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Film-Noir", '9') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Horror", '10') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Musical", '11') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Mystery", '12') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Romance", '13') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Sci-Fi", '14') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Thriller", '15') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("War", '16') for w in genres_single_movie_list]
        genres_single_movie_list = [w.replace("Western", '17') for w in genres_single_movie_list]
        
        genres=""
        for index in xrange(len(genres_single_movie_list)):
            genres=genres+" "+genres_single_movie_list[index]
            
        genres=genres.lstrip()
        single_movie.append(genres)
                    
        total_list.append(single_movie)
        
print total_list
        
import pandas as pd

df=pd.DataFrame(total_list)

df.to_csv('/Users/manasakumar/Desktop/ml-1m/movieid+genres.csv')
        
