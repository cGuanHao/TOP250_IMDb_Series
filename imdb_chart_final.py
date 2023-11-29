import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.io as pio
pio.renderers.default='browser'
import time

start = time.time()

df = pd.read_csv('tv_series.csv')
tag = df['Genres']


count_tag = []
for row in tag:
    process = row.replace('[','').replace(']',''). \
              replace('\'','').replace(' ','').split(',')
    for i in range(len(process)):
        count_tag.append(process[i])   

label_tag = list(set(count_tag))

sum_tag = []
for j in range(len(label_tag)):
    num = count_tag.count(label_tag[j])
    sum_tag.append(num)
    
#########################################################

# 圓餅圖
sum_tag_new = [0]*len(label_tag)
label_tag_new = []
sum_tag_for_pie = sum_tag.copy()
label_tag_for_pie = label_tag.copy()

for k in range(len(label_tag_for_pie)):
    index_tag_pie = sum_tag_for_pie.index(max(sum_tag_for_pie))
    sum_tag_new[k] = sum_tag_for_pie.pop(index_tag_pie)
    label_tag_new.append(label_tag_for_pie.pop(index_tag_pie))

data_tag = {'Label_tag_new': label_tag_new, 
            'Sum_tag_new': sum_tag_new}

data_frame_pie = pd.DataFrame(data_tag)
    
fig = px.pie(data_frame_pie,
              values='Sum_tag_new',
              names='Label_tag_new',
              title='Pie of categories percentage',
              width=1000,
              height=800)

fig.update_traces(textinfo='percent+label')
fig.update_layout(showlegend=False)

fig.show()


#############################################################
    
# 長條圖_最多的種類
sum_tag_for_bar = sum_tag.copy()
sum_tag_for_bar.sort(reverse=True)

plt.figure(figsize = (30,15))
plt.bar(label_tag_new,
        sum_tag_for_bar,
        width = 0.8,
        tick_label = label_tag_new)
plt.xlabel('Categories', fontsize=30)
plt.ylabel('Quantity', fontsize=30)
plt.title('Categories of TV series', fontsize=70)

plt.savefig("Bar of categories.jpg",
            bbox_inches='tight',
            pad_inches=0.0)
plt.show()
plt.close()


# 年分_上榜
years = df['Release Year']

list_year = list(set((years)))
list_year.sort()

total_year = [0]*(max(list_year)-min(list_year)+1)

new_list_year = []
new_list_year.append(min(list_year))
for n in range(len(total_year)-1):
    each_year = new_list_year[n]+1
    new_list_year.append(each_year)
    

for l in range(len(years)):
    for m in range(len(new_list_year)):
        if years[l] == new_list_year[m]:
            total_year[m] += 1

plt.figure(figsize = (30,15))
plt.bar(new_list_year,
        total_year,
        width = 0.8,
        tick_label = new_list_year, 
        color='darkgoldenrod')
plt.xlabel('Year', fontsize=30)
plt.xticks(rotation=30)
plt.ylabel('Quantity', fontsize=30)
plt.title('Total of each release year', fontsize=70)

plt.savefig("Bar of Years.jpg",
            bbox_inches='tight',
            pad_inches=0.0)

plt.show()
plt.close()  
    

# 年分_評分
score = df['Score']

plt.figure(figsize = (20,15))
plt.scatter(years, score, color='maroon')
plt.xlabel('Year', fontsize=30)
plt.ylabel('Score', fontsize=30)
plt.title('Score of each year', fontsize=70)


plt.savefig("Score of each Year.jpg",
            bbox_inches='tight',
            pad_inches=0.0)

plt.show()
plt.close() 


# 觀影人次與其排名和片名
population = df['Population']
name = df['Name']
rank = df['Rank']

pop_new = [0]*len(population)
name_new = []
rank_new =[]
name_and_rank = []
lis_pop = list(population)
lis_name = list(name)
lis_rank = list(rank)

for o in range(len(name)):
    index_pop = lis_pop.index(max(lis_pop))
    pop_new[o] = lis_pop.pop(index_pop)
    name_new.append(lis_name.pop(index_pop))
    rank_new.append(lis_rank.pop(index_pop))
    name_and_rank.append(str(rank_new[o]) + '. ' + name_new[o])

plt.figure(figsize = (20,15))

plt.bar(name_and_rank[:25], 
        pop_new[:25], 
        color='darkgreen')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei'] 

plt.xlabel('Series Name', fontsize=30)
plt.xticks(rotation=35)
plt.ylabel('Viewers', fontsize=30)
plt.title('Viewers of top 25 series', fontsize=70)


plt.savefig("Viewers of top 25 Series.jpg",
            bbox_inches='tight',
            pad_inches=0.0)

plt.show()
plt.close() 


# 歷年觀影給評用戶人數_折線圖
lis_pop_viewers = list(population)
list_years = list(years) #250
pop_sum = [0]*len(new_list_year) #72

for p in range(len(list_years)):
    index_year = new_list_year.index(list_years[p])
    
    if lis_pop_viewers[p][-1] == 'K':
        pop_sum[index_year] += float(lis_pop_viewers[p][0:-1]) * 1000
    elif lis_pop_viewers[p][-1] == 'M':
        pop_sum[index_year] += float(lis_pop_viewers[p][0:-1]) * 1000000

plt.figure(figsize = (30,15))
plt.plot(new_list_year,
        pop_sum,
        marker="o", 
        color='indigo')
plt.xlabel('Year', fontsize=30)
plt.xticks(new_list_year, rotation=30)
plt.ylabel('Number of People', fontsize=30)
plt.title('Viewers of each year', fontsize=70)

plt.savefig("Viewers of each Year.jpg",
            bbox_inches='tight',
            pad_inches=0.0)

plt.show()
plt.close()


end = time.time()
print('All completed. It took %.2f sec.' % (end-start))