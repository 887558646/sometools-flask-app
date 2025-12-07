"""
族群熱度分析路由
提供族群熱度分析、注意股分析等功能
"""

from flask import Blueprint, request, jsonify
import sys
import os
import pandas as pd
import re
from datetime import datetime

# 添加父目錄到路徑
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 導入模組
from modules.data_loader import (
    load_supply_chain_json, 
    load_today_topN, 
    load_attention_stocks_from_web
)
from modules.theme_engine import map_stock_to_themes, calc_theme_heat
from modules.report_builder import build_theme_report, get_theme_detail_for_display

theme_analysis_bp = Blueprint('theme_analysis', __name__)


@theme_analysis_bp.route('/analyze', methods=['POST'])
def analyze():
    """分析族群熱度"""
    try:
        data = request.get_json()
        top_n = data.get('top_n', None)
        
        if top_n:
            try:
                top_n = int(top_n)
                if top_n < 1:
                    return jsonify({'error': 'top_n 必須大於 0'}), 400
            except ValueError:
                return jsonify({'error': 'top_n 必須是有效的數字'}), 400
        
        # 載入週轉率資料
        stocks_df = load_today_topN(top_n=top_n, source="api")
        if stocks_df.empty:
            return jsonify({'error': '無法載入週轉率資料'}), 500
        
        # 載入族群定義
        themes_data = load_supply_chain_json()
        
        # 如果指定了 top_n，限制資料
        if top_n and top_n > 0:
            stocks_df = stocks_df.head(top_n).copy()
        
        # 計算族群對應與熱度
        stock_to_themes = map_stock_to_themes(stocks_df, themes_data)
        theme_heat_df = calc_theme_heat(stocks_df, stock_to_themes)
        
        # 建立報告
        turnover_report_data = build_theme_report(
            stocks_df, theme_heat_df, stock_to_themes, themes_data
        )
        
        # 嘗試載入注意股
        focus_df = pd.DataFrame()
        focus_report_data = None
        focus_merged = pd.DataFrame()
        
        try:
            focus_df = load_attention_stocks_from_web()
            if not focus_df.empty:
                # 合併注意股與週轉率資料
                focus_merged = focus_df.merge(
                    stocks_df,
                    on="code",
                    how="inner",
                    suffixes=("_focus", "")
                )
                
                if not focus_merged.empty:
                    focus_stock_to_themes = map_stock_to_themes(focus_merged, themes_data)
                    focus_theme_heat_df = calc_theme_heat(focus_merged, focus_stock_to_themes)
                    focus_report_data = build_theme_report(
                        focus_merged, focus_theme_heat_df, focus_stock_to_themes, themes_data
                    )
        except Exception as e:
            # 注意股載入失敗不影響主流程
            pass
        
        # 計算平均週轉率
        avg_turnover = float(stocks_df['turnover'].mean()) if not stocks_df.empty else 0.0
        
        # 準備週轉率前N名清單
        turnover_stocks_list = []
        for _, row in stocks_df.iterrows():
            stock_code = str(row["code"]).zfill(4)
            turnover_stocks_list.append({
                'code': stock_code,
                'name': row.get('name', ''),
                'turnover': float(row.get('turnover', 0)) if pd.notna(row.get('turnover')) else None,
                'chg_pct': float(row.get('chg_pct', 0)) if pd.notna(row.get('chg_pct')) else None,
            })
        
        # 為每個族群準備個股清單
        theme_stocks_map = {}
        for theme_name in turnover_report_data['theme_heat_ranking']['theme_name'].tolist():
            # 取得該族群在 Top N 中實際出現的股票
            from modules.theme_engine import get_stocks_in_theme
            theme_stocks = get_stocks_in_theme(stocks_df, stock_to_themes, theme_name)
            if not theme_stocks.empty:
                theme_stocks_map[theme_name] = []
                for _, stock_row in theme_stocks.iterrows():
                    stock_code = str(stock_row["code"]).zfill(4)
                    theme_stocks_map[theme_name].append({
                        'code': stock_code,
                        'name': stock_row.get('name', ''),
                        'turnover': float(stock_row.get('turnover', 0)) if pd.notna(stock_row.get('turnover')) else None,
                        'chg_pct': float(stock_row.get('chg_pct', 0)) if pd.notna(stock_row.get('chg_pct')) else None,
                    })
        
        # 準備返回資料
        result = {
            'turnover_report': {
                'summary': {
                    'total_stocks': turnover_report_data['summary']['total_stocks'],
                    'total_themes': turnover_report_data['summary']['total_themes'],
                    'avg_turnover': avg_turnover
                },
                'theme_heat_ranking': turnover_report_data['theme_heat_ranking'].to_dict('records'),
                'theme_stocks': theme_stocks_map,  # 每個族群的個股清單
                'turnover_stocks_list': turnover_stocks_list,  # 週轉率前N名清單
                'unclassified_stocks': []
            }
        }
        
        # 找出未分類股票
        for _, row in stocks_df.iterrows():
            stock_code = str(row["code"]).zfill(4)
            themes = stock_to_themes.get(stock_code, [])
            if not themes:
                result['turnover_report']['unclassified_stocks'].append({
                    'code': stock_code,
                    'name': row.get('name', ''),
                    'turnover': float(row.get('turnover', 0)) if pd.notna(row.get('turnover')) else None,
                    'chg_pct': float(row.get('chg_pct', 0)) if pd.notna(row.get('chg_pct')) else None,
                })
        
        # 如果有注意股報告
        if focus_report_data and not focus_merged.empty:
            focus_stock_to_themes = map_stock_to_themes(focus_merged, themes_data)
            focus_avg_turnover = float(focus_merged['turnover'].mean()) if not focus_merged.empty else None
            
            # 準備注意股清單
            # 使用 focus_merged 來獲取正確的股票名稱（因為 focus_df 的 name 可能包含敘述）
            focus_stocks_list = []
            # 建立一個 code -> name 的映射，優先使用 focus_merged 中的名稱（來自 stocks_df，包含正確的股票名稱）
            name_map = {}
            if not focus_merged.empty:
                for _, row in focus_merged.iterrows():
                    stock_code = str(row["code"]).zfill(4)
                    # 使用合併後的名稱（來自 stocks_df，是正確的股票名稱）
                    name_map[stock_code] = row.get('name', '')
            
            # 也從 stocks_df 建立映射，以確保所有注意股都能獲取正確的名稱
            stocks_name_map = {}
            for _, row in stocks_df.iterrows():
                stock_code = str(row["code"]).zfill(4)
                stocks_name_map[stock_code] = row.get('name', '')
            
            # 遍歷 focus_df，優先使用合併後的名稱，其次使用 stocks_df 中的名稱，最後才使用原始名稱
            for _, row in focus_df.iterrows():
                stock_code = str(row["code"]).zfill(4)
                # 優先順序：1. focus_merged 中的名稱 2. stocks_df 中的名稱 3. 原始名稱（但會過濾掉太長的名稱）
                stock_name = name_map.get(stock_code) or stocks_name_map.get(stock_code) or row.get('name', '')
                
                # 如果名稱太長（超過20個字元），可能是敘述文字，嘗試從 stocks_df 獲取
                if len(str(stock_name)) > 20:
                    stock_name = stocks_name_map.get(stock_code, '')
                    # 如果還是太長或為空，嘗試提取簡短的股票名稱
                    if len(str(stock_name)) > 20 or not stock_name:
                        name_match = re.search(r'^([\u4e00-\u9fff]{2,4})', str(row.get('name', '')))
                        if name_match:
                            stock_name = name_match.group(1)
                        else:
                            stock_name = ''  # 無法提取，使用空字串
                
                # 從 focus_merged 中獲取週轉率和漲跌幅（如果有的話）
                turnover = None
                chg_pct = None
                if not focus_merged.empty:
                    merged_row = focus_merged[focus_merged['code'] == stock_code]
                    if not merged_row.empty:
                        turnover = float(merged_row.iloc[0].get('turnover', 0)) if pd.notna(merged_row.iloc[0].get('turnover')) else None
                        chg_pct = float(merged_row.iloc[0].get('chg_pct', 0)) if pd.notna(merged_row.iloc[0].get('chg_pct')) else None
                
                # 只添加有名稱的股票
                if stock_name:
                    focus_stocks_list.append({
                        'code': stock_code,
                        'name': stock_name,
                        'turnover': turnover,
                        'chg_pct': chg_pct,
                    })
            
            # 為每個注意股族群準備個股清單
            focus_theme_stocks_map = {}
            for theme_name in focus_report_data['theme_heat_ranking']['theme_name'].tolist():
                from modules.theme_engine import get_stocks_in_theme
                theme_stocks = get_stocks_in_theme(focus_merged, focus_stock_to_themes, theme_name)
                if not theme_stocks.empty:
                    focus_theme_stocks_map[theme_name] = []
                    for _, stock_row in theme_stocks.iterrows():
                        stock_code = str(stock_row["code"]).zfill(4)
                        focus_theme_stocks_map[theme_name].append({
                            'code': stock_code,
                            'name': stock_row.get('name', ''),
                            'turnover': float(stock_row.get('turnover', 0)) if pd.notna(stock_row.get('turnover')) else None,
                            'chg_pct': float(stock_row.get('chg_pct', 0)) if pd.notna(stock_row.get('chg_pct')) else None,
                        })
            
            result['focus_report'] = {
                'summary': {
                    'total_focus_stocks': len(focus_df),
                    'merged_count': len(focus_merged),
                    'avg_turnover': focus_avg_turnover
                },
                'theme_heat_ranking': focus_report_data['theme_heat_ranking'].to_dict('records'),
                'theme_stocks': focus_theme_stocks_map,  # 每個族群的個股清單
                'focus_stocks_list': focus_stocks_list,  # 注意股清單
                'unclassified_stocks': []
            }
            
            # 找出未分類注意股
            for _, row in focus_merged.iterrows():
                stock_code = str(row["code"]).zfill(4)
                themes = focus_stock_to_themes.get(stock_code, [])
                if not themes:
                    result['focus_report']['unclassified_stocks'].append({
                        'code': stock_code,
                        'name': row.get('name', ''),
                        'turnover': float(row.get('turnover', 0)) if pd.notna(row.get('turnover')) else None,
                        'chg_pct': float(row.get('chg_pct', 0)) if pd.notna(row.get('chg_pct')) else None,
                    })
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': f'分析失敗: {str(e)}'}), 500


@theme_analysis_bp.route('/theme-detail', methods=['POST'])
def theme_detail():
    """取得族群詳細資訊"""
    try:
        data = request.get_json()
        theme_name = data.get('theme_name')
        stocks_df = data.get('stocks_df')  # 應該是 JSON 格式的 DataFrame
        
        if not theme_name:
            return jsonify({'error': '請提供 theme_name'}), 400
        
        # 載入族群定義
        themes_data = load_supply_chain_json()
        
        # 如果有 stocks_df，轉換為 DataFrame
        today_df = pd.DataFrame(stocks_df) if stocks_df else pd.DataFrame()
        
        # 計算股票對應
        stock_to_themes = {}
        if not today_df.empty:
            stock_to_themes = map_stock_to_themes(today_df, themes_data)
        
        # 取得族群詳細資訊
        theme_detail_data = get_theme_detail_for_display(
            theme_name, today_df, themes_data, stock_to_themes
        )
        
        if not theme_detail_data:
            return jsonify({'error': f'找不到族群: {theme_name}'}), 404
        
        return jsonify(theme_detail_data)
        
    except Exception as e:
        return jsonify({'error': f'取得族群詳細資訊失敗: {str(e)}'}), 500


@theme_analysis_bp.route('/theme-list', methods=['GET'])
def theme_list():
    """取得所有族群清單"""
    try:
        themes_data = load_supply_chain_json()
        
        # 判斷格式
        themes_list = []
        if isinstance(themes_data, list):
            themes_list = themes_data
        elif "themes" in themes_data:
            themes_list = themes_data.get("themes", [])
        elif "popular_sectors" in themes_data:
            themes_list = themes_data.get("popular_sectors", [])
        else:
            themes_list = themes_data.get("族群清單", [])
        
        # 格式化返回資料
        result = []
        for theme_info in themes_list:
            if isinstance(theme_info, dict):
                theme_name = theme_info.get("theme") or theme_info.get("sector_name") or theme_info.get("族群名稱", "")
                description = theme_info.get("description", "")
                stocks = theme_info.get("stocks", [])
                
                result.append({
                    'theme_name': theme_name,
                    'description': description,
                    'stock_count': len(stocks) if isinstance(stocks, list) else 0
                })
        
        return jsonify({'themes': result})
        
    except Exception as e:
        return jsonify({'error': f'取得族群清單失敗: {str(e)}'}), 500

