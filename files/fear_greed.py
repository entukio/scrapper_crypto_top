import requests as r

def get_fear_greed():
    data = r.get('https://api.alternative.me/fng')
    try:
        obj = data.json()
        classification = obj['data'][0]['value_classification']
        value = obj['data'][0]['value']
        update_time = obj['data'][0]['timestamp']    
        return [1,{'fear_greed_value':value,'fear_greed_class': classification,'fear_greed_update_time': update_time}]
    except Exception as e:
        return [0,'e']