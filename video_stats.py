import requests
from secrets import yt_key
import json


#Build the url for the get request
part = 'ContentDetails'
forHandle = 'MrBeast'
url = f'https://youtube.googleapis.com/youtube/v3/channels?part={part}&forHandle={forHandle}&key={yt_key}'


#response = requests.get(url)

# print(response)

# data = response.json()

# json.dumps(data , indent = 4)
