
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import requests
import re
import time
import pandas as pd


################################################################

# Sub-link function
def crawler_genres_ori_lan_intro(url):
    header = {"user-agent": 
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
              AppleWebKit/537.36 (KHTML, like Gecko) \
              Chrome/104.0.0.0 Safari/537.36"}
    
    response = requests.get(url, headers = header)
    soup = BeautifulSoup(response.content, "lxml")
    
    if response.status_code == requests.codes.ok:

        try:                        
            # genres # introduction # origin # language
            sub_page_tag = soup.find('div', class_="ipc-chip-list__scroller")           
            genre = sub_page_tag.find_all('span', class_="ipc-chip__text")                           
                       
            intro = [soup.find('span', class_="sc-466bb6c-2 chnFO")]           
                       
            sub_page_origin = soup.find('li', {'data-testid':"title-details-origin"})
            origin = sub_page_origin.find_all('a', class_="ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link")                           
                       
            sub_page_language = soup.find_all('a', {'class':"ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})

        except Exception as e: print(e)
 
        genre_content = []     
        for k in range(len(genre)):            
            clean_genre = re.findall('[A-Z][a-z]+', str(genre[k]))
            genre_content.append(clean_genre.pop(0))

        clean_intro = re.findall('>.+<', str(intro[0]))[0][1:-1]
        if clean_intro[-1] == '>': intro_content = re.findall('.+\.\.\.', clean_intro)[0]
        else: intro_content = clean_intro

        origin_content = re.findall('>.+<', str(origin[0]))[0][1:-1]

        language_content = []
        for page in sub_page_language:           
            clean_language_1 = re.findall('language=.+</a>', str(page))           
            if clean_language_1 != []:        
                clean_language_2 = re.findall('>.+<', clean_language_1[0])
                clean_language_3 = clean_language_2[0][1:-1]
                language_content.append(clean_language_3)

    return genre_content, origin_content, language_content, intro_content
          
    time.sleep(1)

################################################################

start = time.time()

# Crawler
if __name__ == '__main__':
       
    # TOP250 page main data
    headers = {'User-Agent': 
               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) \
               AppleWebKit/537.36 (KHTML, like Gecko) \
               Chrome/50.0.2661.102 Safari/537.36'}

    url_250 = 'https://www.imdb.com/chart/toptv/'
    web = requests.get(url_250, headers=headers)
    soup_250 = BeautifulSoup(web.text, 'lxml')
       
    # detail content
    series = soup_250.find_all('div', class_=
                                "sc-479faa3c-0 fMoWnh cli-children")       
    rank, name = [], []
    release_year, final_year, duration, status, episodes, TVPG = [], [], [], [] ,[] ,[]
    link, demographic_link = [], []
    
    for row in series:
                      
        # rank # name
        row_rank_and_name = row.find('div', class_=
          "ipc-title ipc-title--base ipc-title--title ipc-title-link-no-icon ipc-title--on-textPrimary sc-479faa3c-9 dkLVoC cli-title")
        row_title = str(row_rank_and_name.text)
        row_title_split = row_title.split('. ')
        
        rank.append(row_title_split.pop(0))
        name.append(row_title_split.pop(0))
   
        # release_year
        row_years_episode_TVPG = row.find_all('span', class_=
                                              "sc-479faa3c-8 bNrEFi cli-title-metadata-item")
        year = re.findall('\d{4,}\S', str(row_years_episode_TVPG))
        clean_release_year = year[0].rstrip(year[0][-1])
        release_year.append(clean_release_year)

        # final_year # duration # status
        if len(year) == 2: 
            clean_final_year = year[1].rstrip(year[1][-1])
            interval = int(clean_final_year) - int(clean_release_year)
            condition = 'end'
            
        elif year[0][-1] == '<': 
            clean_final_year = year[0].rstrip(year[0][-1])
            interval = 0
            condition = 'end'
        
        else: 
            clean_final_year = ''
            interval = time.localtime().tm_year - int(clean_release_year)
            condition = 'ongoing'
            
        final_year.append(clean_final_year)
        duration.append(interval)
        status.append(condition)

        # episodes
        row_episodes = re.findall('\d+\s.[eps]', str(row_years_episode_TVPG))
        episodes.append(row_episodes[0].rstrip(' eps'))
  
        # TVPG(TV Parental Guidelines)
        if len(row_years_episode_TVPG) == 3:
            row_TVPG = re.findall('[^>]+[$<]', str(row_years_episode_TVPG[-1]))
            clean_row_TVPG = row_TVPG[0].strip('<')
        else: clean_row_TVPG = ''
        TVPG.append(clean_row_TVPG)
        
        # link
        link_add = re.findall('/title/.{9,10}/', str(row))
        link.append('https://www.imdb.com' + link_add[0])
                
    # score
    series = soup_250.find_all('span', class_=
      "ipc-rating-star ipc-rating-star--base ipc-rating-star--imdb ratingGroup--imdb-rating")    
    score = []
    
    for row in series:     
        row_score = re.findall('IMDb rating: \d+.\d', str(row))
        score.append(row_score[0][-3:])
    
    # population    '''specific voting population number has got deleted''' 
    series = soup_250.find_all('span', class_="ipc-rating-star--voteCount")    
    population = []
    
    for row in series:     
        row_population = re.findall('\d.\d?[KM]', str(row))   
        population.append(row_population[0])

################################################################

    # sublink data
    genres, origin, language, introduction = [], [], [], []  
    future_d = {}
    tp = ThreadPoolExecutor(12)
    
    for j in range(len(link)):  # 非同步非阻塞 asynchronous non-blocking
        ret = tp.submit(crawler_genres_ori_lan_intro, link[j])
        future_d[j] = ret

    for key in future_d:  # 同步阻塞 synchronous blocking
        print(key, future_d[key].result())
        
        genres.append(future_d[key].result()[0])    
        origin.append(future_d[key].result()[1])       
        language.append(future_d[key].result()[2])
        introduction.append(future_d[key].result()[3])


################################################################

    # combine data
    data_main = {'Rank': rank, 
                 'Name': name, 
                 'Release Year': release_year, 
                 'Final Year': final_year, 
                 'Duration': duration,
                 'Status': status, 
                 'Episodes': episodes, 
                 'TVPG': TVPG, 
                 'Score': score, 
                 'Population': population, 
                 'Genres': genres,
                 'Origin': origin, 
                 'Language': language, 
                 'Introduction': introduction}
    
    data_frame = pd.DataFrame(data_main)
    
    
    # save as CSV file
    data_frame.to_csv('tv_series.csv', index=False)

################################################################

end = time.time()
print('All completed. It took %.2f sec.' % (end-start))