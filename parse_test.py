import json
with open('test.json', 'r') as f:
    data = json.load(f)
    print(json.dumps(data.get('filter_decision'), indent=2))
