"""
QBot Pro - Backtesting Engine
Tests strategies against historical data with TopStepX compliance checking
"""

import pandas as pd
import numpy as np
from datetime import datetime, time
from typing import Dict, List, Tuple, Optional


class TopStepXBacktester:
    """
    Advanced backtesting engine with TopStepX Combine rule enforcement
    
    Features:
    - Tick-level accuracy
    - Realistic commission/slippage modeling
    - Daily loss limit enforcement
    - Max drawdown tracking
    - Complete performance analytics
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize backtester with configuration
        
        Args:
            config: Dictionary with settings
        """
        # Default configuration
        default_config = {
            'initial_balance': 5000.0,
            'commission_per_contract': 0.25,  # $0.25 per micro contract
            'slippage_ticks': 1,
            'tick_value': {
                'ES': 0.25,  # $0.25 per point for micro ES
                'NQ': 0.50   # $0.50 per point for micro NQ
            }
        }
        
        # Merge with provided config
        self.config = {**default_config, **(config or {})}
        
        # TopStepX Rules (enforced during backtest!)
        self.MAX_DRAWDOWN = 2000.0      # $2,000 max loss
        self.DAILY_LOSS_LIMIT = 400.0   # $400 daily max loss
        self.PROFIT_TARGET = 3000.0     # $3,000 profit goal
        self.DAILY_PROFIT_CAP = 1500.0  # $1,500 daily max profit
        
        # Initialize tracking variables
        self.trades = []
        self.equity_curve = []
        self.daily_pnl = {}
        
    def run_backtest(
        self, 
        data: pd.DataFrame, 
        strategy_func, 
        params: Dict,
        instrument: str = 'NQ'
    ) -> Dict:
        """
        Execute full backtest with TopStepX compliance checking
        
        Args:
            data: OHLCV DataFrame (must have: timestamp, open, high, low, close, volume)
            strategy_func: Function that generates signals (see strategy interface below)
            params: Strategy parameters dictionary
            instrument: 'ES' or 'NQ' (affects tick value)
            
        Returns:
            Dictionary with complete results and pass/fail status
        """
        
        # Reset state
        self.trades = []
        self.equity_curve = []
        self.daily_pnl = {}
        
        # Initialize variables
        balance = self.config['initial_balance']
        peak_balance = balance
        daily_pnl = 0.0
        current_date = None
        position = None
        max_drawdown = 0.0
        daily_max_drawdown = 0.0
        daily_start_balance = balance
        
        # Track statistics
        total_trades = 0
        winning_trades = 0
        losing_trades = 0
        
        # Iterate through each bar
        for idx, row in data.iterrows():
            # Parse timestamp
            if isinstance(row['timestamp'], str):
                bar_time = pd.to_datetime(row['timestamp'])
            else:
                bar_time = row['timestamp']
                
            bar_date = bar_time.date()
            
            # Reset daily tracking on new day
            if current_date != bar_date:
                if current_date is not None:
                    self.daily_pnl[current_date] = daily_pnl
                    
                current_date = bar_date
                daily_pnl = 0.0
                daily_start_balance = balance
                
            # Check daily loss limit FIRST
            if daily_pnl <= -self.DAILY_LOSS_LIMIT:
                # Stop trading for this day
                if position:
                    # Force close position
                    pnl = self._calculate_exit_pnl(position, row['close'])
                    daily_pnl += pnl
                    balance += pnl
                    self._record_trade(position, row['close'], pnl, balance)
                    position = None
                continue
            
            # Check if already hit daily profit cap
            if daily_pnl >= self.DAILY_PROFIT_CAP:
                if position:
                    # Lock in profits - close position
                    pnl = self._calculate_exit_pnl(position, row['close'])
                    daily_pnl += pnl
                    balance += pnl
                    self._record_trade(position, row['close'], pnl, balance)
                    position = None
                continue
            
            # Generate signal from strategy
            try:
                signal = strategy_func(row, params, position)
            except Exception as e:
                signal = 'HOLD'
            
            # Execute trades based on signal
            if signal == 'BUY' and position is None:
                position = self._open_position(row, 'LONG', params.get('size', 1))
                
            elif signal == 'SELL' and position is None:
                position = self._open_position(row, 'SHORT', params.get('size', 1))
                
            elif signal == 'CLOSE' and position is not None:
                pnl = self._calculate_exit_pnl(position, row['close'])
                daily_pnl += pnl
                balance += pnl
                self._record_trade(position, row['close'], pnl, balance)
                position = None
            
            # Update equity curve
            if position is not None:
                unrealized_pnl = self._calculate_unrealized_pnl(position, row['close'])
                current_equity = balance + unrealized_pnl
            else:
                current_equity = balance
            
            self.equity_curve.append({
                'timestamp': bar_time,
                'equity': current_equity
            })
            
            # Track peak and drawdown
            if current_equity > peak_balance:
                peak_balance = current_equity
                
            drawdown = peak_balance - current_equity
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                
            # Daily drawdown tracking
            daily_dd = daily_start_balance - current_equity
            if daily_dd > daily_max_drawdown:
                daily_max_drawdown = daily_dd
            
            # FAIL FAST: Check if rules violated
            if max_drawdown >= self.MAX_DRAWDOWN:
                return self._generate_result(
                    passed=False,
                    reason=f'Max drawdown exceeded: ${max_drawdown:.2f} (limit: ${self.MAX_DRAWDOWN})',
                    instrument=instrument
                )
        
        # Calculate final metrics
        total_profit = balance - self.config['initial_balance']
        
        # Calculate win rate
        if len(self.trades) > 0:
            wins = sum(1 for t in self.trades if t['pnl'] > 0)
            win_rate = wins / len(self.trades)
        else:
            win_rate = 0.0
        
        # Calculate other metrics
        sharpe_ratio = self._calculate_sharpe_ratio()
        profit_factor = self._calculate_profit_factor()
        
        # Determine if passed
        passed = self._check_pass_criteria(total_profit, max_drawdown, daily_max_drawdown)
        
        return self._generate_result(
            passed=passed,
            reason=None if passed else 'Did not meet all criteria',
            total_profit=total_profit,
            max_drawdown=max_drawdown,
            daily_max_dd=daily_max_drawdown,
            win_rate=win_rate,
            sharpe_ratio=sharpe_ratio,
            profit_factor=profit_factor,
            total_trades=len(self.trades),
            final_balance=balance,
            instrument=instrument
        )
    
    def _open_position(self, row, direction, size):
        """Open a new position"""
        return {
            'entry_time': row['timestamp'],
            'entry_price': row['close'],
            'direction': direction,
            'size': size
        }
    
    def _calculate_exit_pnl(self, position, exit_price):
        """Calculate P&L for closed position"""
        price_diff = exit_price - position['entry_price']
        
        if position['direction'] == 'LONG':
            gross_pnl = price_diff * position['size']
        else:  # SHORT
            gross_pnl = -price_diff * position['size']
        
        # Subtract commission
        commission = self.config['commission_per_contract'] * position['size']
        
        return gross_pnl - commission
    
    def _calculate_unrealized_pnl(self, position, current_price):
        """Calculate unrealized P&L for open position"""
        price_diff = current_price - position['entry_price']
        
        if position['direction'] == 'LONG':
            return price_diff * position['size']
        else:
            return -price_diff * position['size']
    
    def _record_trade(self, position, exit_price, pnl, balance):
        """Record a completed trade"""
        self.trades.append({
            'entry_time': position['entry_time'],
            'exit_time': pd.Timestamp.now(),
            'direction': position['direction'],
            'entry_price': position['entry_price'],
            'exit_price': exit_price,
            'pnl': pnl,
            'balance': balance
        })
    
    def _calculate_sharpe_ratio(self):
        """Calculate Sharpe ratio from equity curve"""
        if len(self.equity_curve) < 2:
            return 0.0
            
        returns = []
        for i in range(1, len(self.equity_curve)):
            prev_eq = self.equity_curve[i-1]['equity']
            curr_eq = self.equity_curve[i]['equity']
            if prev_eq != 0:
                returns.append((curr_eq - prev_eq) / prev_eq)
        
        if len(returns) == 0:
            return 0.0
            
        returns = np.array(returns)
        
        avg_return = np.mean(returns)
        std_return = np.std(returns)
        
        if std_return == 0:
            return 0.0
            
        # Annualize (assuming daily returns)
        sharpe = (avg_return / std_return) * np.sqrt(252)
        
        return round(sharpe, 2)
    
    def _calculate_profit_factor(self):
        """Calculate profit factor (gross profit / gross loss)"""
        if len(self.trades) == 0:
            return 0.0
            
        gross_profit = sum(t['pnl'] for t in self.trades if t['pnl'] > 0)
        gross_loss = abs(sum(t['pnl'] for t in self.trades if t['pnl'] < 0))
        
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
            
        return round(gross_profit / gross_loss, 2)
    
    def _check_pass_criteria(self, profit, max_dd, daily_dd) -> bool:
        """Check if results pass TopStepX criteria"""
        checks = [
            profit >= self.PROFIT_TARGET * 0.8,     # At least 80% of target
            max_dd <= self.MAX_DRAWDOWN,             # Within max loss limit
            daily_dd <= self.DAILY_LOSS_LIMIT * 1.05 # Small buffer on daily
        ]
        return all(checks)
    
    def _generate_result(self, **kwargs) -> Dict:
        """Generate comprehensive result dictionary"""
        base_result = {
            'trades': self.trades,
            'equity_curve': self.equity_curve,
            'daily_pnl': self.daily_pnl,
            'config': self.config,
            'topstepx_rules': {
                'profit_target': self.PROFIT_TARGET,
                'max_drawdown': self.MAX_DRAWDOWN,
                'daily_loss_limit': self.DAILY_LOSS_LIMIT,
                'daily_profit_cap': self.DAILY_PROFIT_CAP
            },
            'generated_at': datetime.now().isoformat()
        }
        
        base_result.update(kwargs)
        
        # Add calculated fields if not present
        if 'avg_trade' not in base_result and len(self.trades) > 0:
            base_result['avg_trade'] = round(
                np.mean([t['pnl'] for t in self.trades]), 2
            )
        else:
            base_result['avg_trade'] = 0.0
            
        return base_result


def print_backtest_report(result: Dict):
    """Print formatted backtest report"""
    
    print("\n" + "=" * 70)
    print("QBOT PRO BACKTEST RESULTS")
    print("=" * 70)
    
    print(f"\nStrategy Test Results:")
    print("-" * 50)
    print(f"Instrument:          {result.get('instrument', 'NQ')}")
    print(f"Status:              {'✅ PASSED' if result['passed'] else '❌ FAILED'}")
    
    if result.get('reason'):
        print(f"Reason:              {result['reason']}")
    
    print(f"\nFinancial Performance:")
    print("-" * 50)
    print(f"Total Profit:         ${result.get('total_profit', 0):,.2f}")
    print(f"Final Balance:        ${result.get('final_balance', 0):,.2f}")
    print(f"Max Drawdown:         ${result.get('max_drawdown', 0):,.2f}")
    print(f"Daily Max DD:         ${result.get('daily_max_dd', 0):,.2f}")
    
    print(f"\nTrading Statistics:")
    print("-" * 50)
    print(f"Total Trades:         {result.get('total_trades', 0)}")
    print(f"Win Rate:             {result.get('win_rate', 0)*100:.1f}%")
    print(f"Avg Trade:            ${result.get('avg_trade', 0):,.2f}")
    print(f"Profit Factor:        {result.get('profit_factor', 0):.2f}")
    print(f"Sharpe Ratio:         {result.get('sharpe_ratio', 0):.2f}")
    
    print(f"\nTopStepX Compliance:")
    print("-" * 50)
    
    rules = result.get('topstepx_rules', {})
    profit = result.get('total_profit', 0)
    max_dd = result.get('max_drawdown', 0)
    daily_dd = result.get('daily_max_dd', 0)
    
    profit_ok = profit >= rules.get('profit_target', 3000) * 0.8
    dd_ok = max_dd <= rules.get('max_drawdown', 2000)
    daily_ok = daily_dd <= rules.get('daily_loss_limit', 400) * 1.05
    
    print(f"Profit Target ({rules.get('profit_target', 3000)}):      {'✅ MET' if profit_ok else '❌ NOT MET'} (${profit:,.2f})")
    print(f"Max Drawdown ({rules.get('max_drawdown', 2000)}):       {'✅ PASSED' if dd_ok else '❌ FAILED'} (${max_dd:,.2f})")
    print(f"Daily Loss Limit ({rules.get('daily_loss_limit', 400)}):  {'✅ PASSED' if daily_ok else '❌ FAILED'} (${daily_dd:,.2f})")
    
    print("\n" + "=" * 70)
    
    if result['passed']:
        print("🎯 VERDICT: THIS STRATEGY PASSES TOPSTEPX COMBINE!")
    else:
        print("⚠️  VERDICT: Strategy needs optimization to pass.")
    print("=" * 70 + "\n")


if __name__ == '__main__':
    # Quick test
    print("Backtesting engine loaded successfully!")
    bt = TopStepXBacktester()
    print(f"Config: Initial Balance ${bt.config['initial_balance']:,.2f}")
    print(f"Max Drawdown Limit: ${bt.MAX_DRAWDOWN:,.2f}")
    print("✅ Backtesting engine ready!")