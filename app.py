from flask import Flask, request, Response
import requests

app = Flask(__name__)

# Use lite-api which is accessible outside Telstra
JUPITER_BASE = "https://lite-api.jup.ag"

@app.route('/quote', methods=['GET'])
def quote():
    try:
        r = requests.get(
            f"{JUPITER_BASE}/swap/v1/quote",
            params=request.args,
            timeout=15
        )
        return Response(r.content, status=r.status_code, content_type='application/json')
    except Exception as e:
        return {"error": str(e)}, 500

@app.route('/swap', methods=['POST'])
def swap():
    try:
        r = requests.post(
            f"{JUPITER_BASE}/swap/v1/swap",
            json=request.get_json(),
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        return Response(r.content, status=r.status_code, content_type='application/json')
    except Exception as e:
        return {"error": str(e)}, 500

@app.route('/health')
def health():
    return {"status": "ok", "jupiter": JUPITER_BASE}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
