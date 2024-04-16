import json
import psycopg2
import requests

conn = psycopg2.connect(
    host="10.28.51.4",
    port="5433",
    database="postgres",
    user="hackathon-user-02",
    password="P2o3PqRs4T"
)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 OPR/107.0.0.0'}
#парсинг видео за неделю
cur = conn.cursor()
dates = ['06', '07', '08', '09', '10', '11', '12', '13']
pages = 1

for x in dates:
    url = f'https://rutube.ru/api/video/category/8/{x}042024/?client=wdp&date_type=publication&page=1'
    flag = True
    while flag:
        url_2 = f'https://rutube.ru/api/video/category/8/{x}042024/?client=wdp&date_type=publication&page={pages}'
        pages += 1
        responce = requests.get(url, timeout=20, headers=headers)
        if not (bool(responce.json()['has_next'])):
            flag = False
            pages = 1
        fl = open(f'{x}_{pages}.json', 'a')
        json.dump(responce.json(), fl, indent=4)
        data = responce.json()
        # print(data)
        for i in range(len(data['results'])):
            url = data['results'][i]['video_url']
            parts = url.split("/")
            video_id = parts[4]
            link_for_like = 'https://rutube.ru/api/numerator/video/' + video_id + '/vote?client=wdp'
            link_for_comms = 'https://rutube.ru/api/comments/video/' + video_id + '/?client=wdp'
            print(link_for_like)
            likes = json.loads(requests.get(url=link_for_like).text)['positive']
            dislikes = json.loads(requests.get(url=link_for_like).text)['negative']
            comments = json.loads(requests.get(url=link_for_comms).text)['comments_count']
            # print(data['results'][i]['description'], data['results'][i]['video_url'])
            cur.execute(f'''INSERT INTO public.channel
              (
              id_channel,
              channel_name,
              subscribers_count)
            VALUES(
              {data['results'][i]['author']['id']},
              '{data['results'][i]['author']['name']}',
              {data['results'][i]['hits']}) on conflict (id_channel) do nothing;

                INSERT INTO public.category
              (
              id_category,
              category_name)
            VALUES(
              {data['results'][i]['category']['id']},
              '{data['results'][i]['category']['name']}'
            ) on conflict (id_category) do nothing;

                INSERT INTO public.video
              (
              link,  
              id_channel,
              id_category, 
              video_name,
              date_publication,
              duration_ms, 
              like_count, 
              disike_count,
              view_count, 
              commentary_count,
              description, 
             pg_rating, 
              is_paid, 
             is_official)
              VALUES(
              '{data['results'][i]['video_url']}', 
              {data['results'][i]['author']['id']},
              {data['results'][i]['category']['id']},
              '{data['results'][i]['feed_name']}',
              '{data['results'][i]['created_ts'].replace('T', ' ')}',
              {int(data['results'][i]['duration']) * 100},
              {likes}, 
              {dislikes},
              {data['results'][i]['hits']}, 
              {comments}, 
              '{data['results'][i]['description']}',
              {data['results'][i]['pg_rating']['age']},
              {data['results'][i]['is_paid']},
             {data['results'][i]['is_official']});''')
            conn.commit()
        if x == '13':
            break