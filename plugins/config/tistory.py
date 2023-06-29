import requests

def get_access_token():
    url='https://www.tistory.com/oauth/access_token'
    params={
    'client_id':'{client_id}',
    'client_secret':'{client_secret}',
    'redirect_uri':'http://tistory.com',
    'code':'{access_code}',
    'grant_type':'authorization_code'
    }
    resp = requests.get(url, params=params)
    print(resp.text)
    
    
def set_tistory_post(access_token, blog_name, title, content, tag_lst):
    tags = ','.join(tag_lst)
    params={
        'access_token':access_token,
        'blogName':blog_name,
        'title': title,
        'content':content,
        'visibility':3,        # (0: 비공개 - 기본값, 1: 보호, 3: 발행)
        'category':0,
        'tag':tags,
        'acceptComment':0     # 댓글 허용 (0, 1 - 기본값)
    }

    url = 'https://www.tistory.com/apis/post/write'
    resp = requests.post(url=url, params=params)
    
    return resp.text


