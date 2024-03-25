import numpy as np
import json
import wget
from bs4 import BeautifulSoup

base_url = 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v6/index.html'
output_directory = 'data/'

file = wget.download(base_url)


f = open(file, "r")
file_content = f.read()

soup = BeautifulSoup(file_content, 'html.parser')

cosmos_files = []
for temp in soup.find_all('a'):
    if ('cosweb' in temp['href']) & ('_sci' in temp['href']):
        cosmos_files.append(temp['href'])
    
print('\n')
for i in range(len(cosmos_files)):
    temp = cosmos_files[i].split('_')[0].split('-')
    if temp[-1] != 'clear':
        print('filter: ', temp[-1])
    else:
        print('filter: ', temp[-2])
    print('url: ', cosmos_files[i])

    file = wget.download(cosmos_files[i], out=output_directory)
