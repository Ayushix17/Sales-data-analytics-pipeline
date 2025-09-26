from flask import Flask, jsonify
import os
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def dashboard():
    return '''
    <html>
    <head><title>Sales Analytics Dashboard</title></head>
    <body style="font-family: Arial; margin: 40px;">
        <h1>📊 Sales Analytics Dashboard</h1>
        <p><strong>Status:</strong> <span style="color: green;">Running</span></p>
        <p><strong>Time:</strong> ''' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '''</p>
        <h3>Quick Links:</h3>
        <ul>
            <li><a href="/health">Health Check</a></li>
            <li><a href="/api/kpis">View KPIs</a></li>
        </ul>
    </body>
    </html>
    '''

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "time": datetime.now().isoformat()})

@app.route('/api/kpis')
def kpis():
    return jsonify({
        "total_revenue": 0,
        "active_customers": 0,
        "message": "Run ETL pipeline first to see real data"
    })

if __name__ == '__main__':
    print("🚀 Starting Dashboard at http://localhost:8050")
    app.run(host='0.0.0.0', port=8050, debug=True)