import requests 
import json
def get_chatgpt_response(api_key, prompt, temperature=0.7, model='gpt-3.5-turbo'):
    headers={'Authorization': f'Bearer {api_key}'}
    data={
        "model": model,
        "messages": [{"role": "system", "content":"You are a reporter"},
                        {"role": "user", "content": prompt}
                    ],
        "temperature": temperature
    }
    print(f'headers: {headers}, json:{data}')
    rslt = requests.post(url='https://api.openai.com/v1/chat/completions', headers=headers, json=data)
    msg = json.loads(rslt.text)['choices'][0]['message']['content']
    print(f'Response of ChatGPT: {rslt.text}')

    return msg