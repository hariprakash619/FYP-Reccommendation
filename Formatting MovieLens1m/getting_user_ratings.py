with open('/Users/manasakumar/Desktop/ml-1m/ratings.dat', 'r') as input_file:
    lines = input_file.readlines()
    all_rows=[]
    for line in lines:
        single_row=[]
        newLine = line.split('::')
        rating=int(newLine[2])
        if rating>=4: 
            single_row.append(newLine[0])
            single_row.append(newLine[1])
            
            all_rows.append(single_row)

#print all_rows

       
import pandas as pd



df=pd.DataFrame(all_rows)
print df
df.to_csv('/Users/manasakumar/Desktop/ml-1m/user_ratings.csv')
       
