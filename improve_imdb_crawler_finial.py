'''
94 sec
Use package concurrent.futures
Rewrite improve_imdb_crawler4.py
Integrate the original file


Next step will crawl gender and age data.
'''

from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import requests
import re
import time
import pandas as pd


################################################################

# label function
def crawler_tags(url):
    header = {"user-agent": 
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
              AppleWebKit/537.36 (KHTML, like Gecko) \
              Chrome/104.0.0.0 Safari/537.36"}
    
    response = requests.get(url, headers = header)
    soup = BeautifulSoup(response.content, "lxml")
    
    if response.status_code == requests.codes.ok:

        try:
            filter1 = soup.find('div', class_=
                      "ipc-chip-list--baseAlt ipc-chip-list sc-16ede01-4 bMBIRz")
            filter2 = filter1.find_all('span', class_="ipc-chip__text")               
        except:
            filter1 = soup.find('div', class_=
                      "ipc-chip-list--baseAlt ipc-chip-list sc-16ede01-5 ggbGKe")
            filter2 = filter1.find_all('span', class_="ipc-chip__text")
        

        tag_content = []
        for k in range(len(filter2)):
            filter3 = re.findall('[A-Z][a-z]+', str(filter2[k]))
            tag_content.append(filter3.pop(0))
    
    return tag_content
          
    time.sleep(1)

################################################################

start = time.time()

# Crawl tags
if __name__ == '__main__':

    # TOP250 page main data
    url_250 = 'https://www.imdb.com/chart/toptv/'
    web = requests.get(url_250)
    soup_250 = BeautifulSoup(web.text, 'lxml')
    
       
    # rank, series name, years, link
    series = soup_250.find_all('td', class_="titleColumn")
    
    rank = []
    name = []
    release_year = []
    link = []
    demographic_link = []
    for row in series:
        
        # rank done
        order = re.findall('\d{1,3}', str(row.text))
        rank.append(order.pop(0))
        
        # series name done
        del_sp1 = str(row.text.replace('\n',''))
        del_sp2 = del_sp1.lstrip(' ')
        del_sp3 = re.sub('\d{1,3}.\s{6,}', '', del_sp2)
        del_sp4 = del_sp3.split('(')
        del_sp4.pop(1)
        name.append(del_sp4[0])
        
        # years done
        year = re.findall('\S\d{4,}', str(del_sp2))
        del_sp5 = year[0].lstrip('(')
        release_year.append(del_sp5)
        
        # link done
        link_add = re.findall('/title/.{9,10}/', str(row))
        link.append('https://www.imdb.com' + link_add[0])
        
        # demographic link done
        demographic_link.append('https://www.imdb.com' + 
                                link_add[0] + 
                                'ratings/?ref_=tt_ov_rt/')

    # score, population
    rating = soup_250.find_all('td', class_="ratingColumn imdbRating")
    
    score = []
    base_on = []
    for i in range(len(rating)):
        # score, population done
        num = re.findall('\d+.\d+.?\d*', str(rating[i]))
        score.append(num.pop(0))
        base_on.append(int(num.pop(0).replace(',', '').rstrip(' ')))

################################################################
    
    # sublink data
    tag = []    
    future_d = {}
    tp = ThreadPoolExecutor(4)
    
    for j in range(len(link)):  # 非同步非阻塞 asynchronous non-blocking
        ret = tp.submit(crawler_tags, link[j])
        future_d[j] = ret

    for key in future_d:  # 同步阻塞 synchronous blocking
        print(key, future_d[key].result())
        tag.append(future_d[key].result())

################################################################

    # combine data
    data_main = {'Rank': rank, 
                 'Name': name, 
                 'Release Year': release_year, 
                 'Score': score, 
                 'Population': base_on, 
                 'Tags': tag}
    
    data_frame = pd.DataFrame(data_main)
    
    
    # save as CSV file
    data_frame.to_csv('tv_series.csv', index=False)

################################################################

end = time.time()
print('All completed. It took %.2f sec.' % (end-start))



