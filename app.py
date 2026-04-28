from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Configuration
CONFIG = {
    'initial_balance': 5000,
    'max_drawdown': 2000,
    'daily_loss_limit': 400,
    'profit_target': 3000,
    'instruments': ['ES', 'NQ']
}

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/api/status')
def status():
    return jsonify({
        'status': 'online',
        'timestamp': datetime.now().isoformat(),
        'config': CONFIG,
        'message': 'QBot Pro is running!'
    })

@app.route('/api/backtest', methods=['POST'])
def run_backtest():
    """Simple demo backtest endpoint"""
    data = request.json
    instrument = data.get('instrument', 'NQ')
    
    # Demo result (will be replaced with real backtest later)
    return jsonify({
        'success': True,
        'instrument': instrument,
        'result': {
            'total_trades': 487,
            'win_rate': 67.8,
            'total_profit': 5323.75,
            'max_drawdown': 1847.25,
            'sharpe_ratio': 1.84,
            'passed': True,
            'message': 'This strategy PASSES TopStepX requirements!'
        },
        'generated_at': datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 STARTING QBOT PRO SERVER...")
    print("=" * 60)
    print(f"📊 Dashboard: http://127.0.0.1:5000")
    print(f"⏰ Started at: {datetime.now()}")
    print("=" * 60)
    print("Press CTRL+C to stop the server\n")
    
    app.run(debug=True, port=5000, host='127.0.0.1')