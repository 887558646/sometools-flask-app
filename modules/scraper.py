import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import time
from typing import Optional

def fetch_turnover_rank_data(top_n: Optional[int] = None) -> pd.DataFrame:
    """
    從玩股網抓取當日週轉率排行資料。
    URL: https://www.wantgoo.com/stock/ranking/turnover-rate
    """
    url = "https://www.wantgoo.com/stock/ranking/turnover-rate"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        from io import StringIO
        # 使用 pandas 解析表格
        dfs = pd.read_html(StringIO(response.text))
        
        target_df = None
        for df in dfs:
            # 處理 MultiIndex 欄位：將其扁平化
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
            
            # 確保欄位名稱是字串
            df.columns = [str(c) for c in df.columns]
            
            # 檢查欄位
            cols = df.columns.tolist()
            
            # 使用最簡單的字串匹配
            check_turnover = False
            check_code = False
            check_name = False
            
            for c in cols:
                if '週轉率' in c or '周轉率' in c: check_turnover = True
                if '代碼' in c: check_code = True
                if '股票' in c or '名稱' in c: check_name = True
            
            if check_turnover and check_code and check_name:
                target_df = df
                break
        
        if target_df is None:
            # 如果找不到表格，嘗試印出所有表格的欄位以便除錯
            debug_info = []
            for i, df in enumerate(dfs):
                debug_info.append(f"Table {i} cols: {df.columns.tolist()}")
            raise Exception(f"無法在頁面上找到週轉率排行表格。找到 {len(dfs)} 個表格。欄位資訊: {'; '.join(debug_info)}")
            
        # 標準化欄位名稱
        col_mapping = {}
        for col in target_df.columns:
            c_str = str(col)
            if '代碼' in c_str: col_mapping[col] = 'code'
            elif '股票' in c_str or '名稱' in c_str: col_mapping[col] = 'name'
            elif '週轉率' in c_str or '周轉率' in c_str: col_mapping[col] = 'turnover'
            elif '成交價' in c_str or '收盤' in c_str: col_mapping[col] = 'close'
            elif '漲跌%' in c_str: col_mapping[col] = 'chg_pct'
            
        df = target_df.rename(columns=col_mapping)
        
        # 確保必要欄位存在
        required = ['code', 'name', 'turnover']
        if not all(col in df.columns for col in required):
            raise Exception(f"表格缺少必要欄位: {required}")
            
        # 資料清理
        # 1. 代碼轉為 4 位字串
        df['code'] = df['code'].astype(str).str.zfill(4)
        
        # 2. 數值欄位清理 (移除 %, ,, +, ▲, ▼)
        def clean_numeric(val):
            if pd.isna(val): return val
            s = str(val)
            s = s.replace('%', '').replace(',', '').replace('+', '')
            s = s.replace('▲', '').replace('▼', '')
            try:
                return float(s)
            except:
                return 0.0

        df['turnover'] = df['turnover'].apply(clean_numeric)
        
        if 'close' in df.columns:
            df['close'] = df['close'].apply(clean_numeric)
        else:
            df['close'] = 0.0
            
        if 'chg_pct' in df.columns:
            df['chg_pct'] = df['chg_pct'].apply(clean_numeric)
        else:
            df['chg_pct'] = 0.0
            
        # 排序與篩選
        df = df.sort_values('turnover', ascending=False).reset_index(drop=True)
        
        if top_n:
            df = df.head(top_n)
            
        return df[['code', 'name', 'turnover', 'close', 'chg_pct']]

    except Exception as e:
        raise Exception(f"抓取週轉率資料失敗: {str(e)}")


def fetch_attention_stock_data() -> pd.DataFrame:
    """
    從 MoneyDJ 抓取注意股資料。
    URL: https://www.moneydj.com/Z/ZE/ZEV/ZEV.djhtm
    """
    url = "https://www.moneydj.com/Z/ZE/ZEV/ZEV.djhtm"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        stocks = []
        seen_codes = set()
        
        # 策略 1: 針對 MoneyDJ 特有的 Link2Stk 連結進行解析 (最準確)
        # 格式: <a href="javascript:Link2Stk('AS2375');">2375凱美</a>
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=re.compile(r"Link2Stk"))
        
        for link in links:
            # 解析 href 中的代碼: javascript:Link2Stk('AS2375');
            match = re.search(r"'AS(\d{4})'", link.get('href', ''))
            if match:
                code = match.group(1)
                
                if code in seen_codes:
                    continue
                    
                # 解析連結文字中的名稱
                text = link.get_text(strip=True)
                # 通常是 "2375凱美" 或 "2375 凱美"
                name = text.replace(code, '').strip()
                
                # 如果名稱為空，嘗試從 title 屬性或其他地方找，或者這只是代碼連結
                if not name:
                    continue
                
                # 如果名稱太長（超過20個字元），可能是敘述文字，嘗試提取簡短的股票名稱
                # 通常股票名稱是2-4個中文字
                if len(name) > 20:
                    # 嘗試提取前幾個中文字作為股票名稱
                    import re
                    name_match = re.search(r'^([\u4e00-\u9fff]{2,4})', name)
                    if name_match:
                        name = name_match.group(1)
                    else:
                        # 如果無法提取，跳過這筆資料（名稱可能是敘述）
                        continue
                    
                stocks.append({'code': code, 'name': name})
                seen_codes.add(code)
        
        if stocks:
            return pd.DataFrame(stocks)

        # 策略 2: 嘗試用 pandas read_html 解析表格 (備用)
        try:
            from io import StringIO
            dfs = pd.read_html(StringIO(response.text))
            for df in dfs:
                df_str = df.astype(str)
                # 尋找包含股票代碼的欄位 (可能混合了名稱)
                for col in df_str.columns:
                    # 檢查欄位值是否包含 4 位數字
                    # 我們看前幾列是否有符合格式的
                    sample = df_str[col].head(10).astype(str)
                    has_codes = sample.str.contains(r'\d{4}').sum()
                    
                    if has_codes >= 1:
                        # 嘗試從該欄位提取代碼和名稱
                        # 情況 A: 代碼和名稱在同一欄 (例如 "2375凱美")
                        extracted = df_str[col].str.extract(r'(\d{4})\s*([\u4e00-\u9fff]+.*)?')
                        extracted.columns = ['code', 'name']
                        
                        # 情況 B: 代碼在這一欄，名稱在下一欄
                        if extracted['name'].isna().all() or (extracted['name'] == '').all():
                            col_idx = df_str.columns.get_loc(col)
                            if col_idx + 1 < len(df_str.columns):
                                name_col = df_str.columns[col_idx + 1]
                                extracted['name'] = df_str[name_col]
                        
                        # 過濾有效資料
                        valid_stocks = extracted.dropna(subset=['code']).copy()
                        if not valid_stocks.empty:
                            # 清理名稱 (移除可能的代碼殘留)
                            def clean_stock_name(row):
                                name_str = str(row['name']).replace(str(row['code']), '').strip()
                                # 如果名稱太長（超過20個字元），可能是敘述文字，嘗試提取簡短的股票名稱
                                if len(name_str) > 20:
                                    name_match = re.search(r'^([\u4e00-\u9fff]{2,4})', name_str)
                                    if name_match:
                                        return name_match.group(1)
                                    else:
                                        return ''  # 無法提取，返回空字串
                                return name_str
                            
                            valid_stocks.loc[:, 'name'] = valid_stocks.apply(clean_stock_name, axis=1)
                            # 移除空名稱
                            valid_stocks = valid_stocks[valid_stocks['name'] != '']
                            
                            if not valid_stocks.empty:
                                return valid_stocks.drop_duplicates(subset=['code']).reset_index(drop=True)
        except:
            pass
            
        # 策略 3: Regex 全文搜索 (最後手段)
        text_content = soup.get_text('\n')
        # 放寬限制：允許代碼和名稱之間有少量字元
        pattern = re.compile(r"(\d{4})\s*.*?([\u4e00-\u9fff]+)")
        
        for line in text_content.split('\n'):
            line = line.strip()
            if not line: continue
            
            # 排除明顯不是股票的行 (例如日期)
            if "日期" in line or "注意" in line:
                continue
                
            matches = pattern.finditer(line)
            for match in matches:
                code = match.group(1)
                name = match.group(2)
                
                # 如果名稱太長（超過20個字元），可能是敘述文字，嘗試提取簡短的股票名稱
                if len(name) > 20:
                    name_match = re.search(r'^([\u4e00-\u9fff]{2,4})', name)
                    if name_match:
                        name = name_match.group(1)
                    else:
                        # 如果無法提取，跳過這筆資料
                        continue
                
                # 簡單驗證名稱長度，避免抓到雜訊
                # 股票名稱通常是2-4個中文字，太長或太短都可能是錯誤的
                if 2 <= len(name) <= 10 and code not in seen_codes:
                    stocks.append({'code': code, 'name': name})
                    seen_codes.add(code)
                    
        if not stocks:
            raise Exception("無法解析注意股內容 (找不到表格或符合格式的文字)")
            
        return pd.DataFrame(stocks)

    except Exception as e:
        raise Exception(f"抓取注意股資料失敗: {str(e)}")


def clean_numeric(value):
    """
    嚴格清洗數值函式，確保數值轉換正確。
    
    Args:
        value: 要清洗的值
    
    Returns:
        有效的數值或 None
    """
    if value is None or pd.isna(value):
        return None
    
    # 轉為字串並移除逗號和特殊字符
    s = str(value).replace(",", "").replace("--", "").strip()
    
    # 嘗試轉為數值
    try:
        return pd.to_numeric(s, errors="coerce")
    except:
        return None


def get_twse_df() -> pd.DataFrame:
    """
    獲取上市 (TWSE) 股票的週轉率資料。
    
    流程：
    1. 抓取股價與成交量資料
    2. 抓取股本資料
    3. 資料清洗與合併
    4. 計算週轉率
    
    Returns:
        DataFrame，包含 code, name, turnover, close, chg_pct, market 欄位
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    # 標準欄位
    STANDARD_COLUMNS = ['code', 'name', 'close', 'turnover', 'chg_pct', 'market']
    
    try:
        # 1. 抓取股價與成交量資料
        price_url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        price_response = requests.get(price_url, headers=headers, timeout=15)
        price_response.raise_for_status()
        price_data = price_response.json()
        
        if not price_data:
            raise Exception("無法從 TWSE API 取得股價資料")
        
        price_df = pd.DataFrame(price_data)
        
        # 標準化欄位名稱
        price_df = price_df.rename(columns={
            "Code": "code",
            "Name": "name",
            "TradeVolume": "TradeVolume",
            "ClosingPrice": "ClosingPrice"
        })
        
        # 資料清洗
        price_df["code"] = price_df["code"].astype(str).str.strip().str.zfill(4)
        price_df["TradeVolume"] = price_df["TradeVolume"].apply(clean_numeric)
        price_df["ClosingPrice"] = price_df["ClosingPrice"].apply(clean_numeric)
        
        # 2. 抓取股本資料
        capital_url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
        capital_response = requests.get(capital_url, headers=headers, timeout=15)
        capital_response.raise_for_status()
        capital_data = capital_response.json()
        
        if not capital_data:
            raise Exception("無法從 TWSE API 取得股本資料")
        
        capital_df = pd.DataFrame(capital_data)
        
        # 標準化欄位名稱
        capital_df = capital_df.rename(columns={
            "公司代號": "code",
            "已發行普通股數或TDR原股發行股數": "IssuedShares"
        })
        
        # 資料清洗
        capital_df["code"] = capital_df["code"].astype(str).str.strip().str.zfill(4)
        capital_df["IssuedShares"] = capital_df["IssuedShares"].apply(clean_numeric)
        
        # 去重（保留第一筆）
        capital_df = capital_df.drop_duplicates(subset=["code"], keep="first")
        
        # 3. 合併資料
        merged_df = pd.merge(
            price_df[["code", "name", "TradeVolume", "ClosingPrice"]],
            capital_df[["code", "IssuedShares"]],
            on="code",
            how="inner"
        )
        
        # 4. 過濾條件
        # 排除 ETF (00xx)
        merged_df = merged_df[~merged_df["code"].str.startswith("00")]
        # 排除 TDR (91xx)
        merged_df = merged_df[~merged_df["code"].str.startswith("91")]
        # 排除成交量 < 500,000 股（500 張）
        merged_df = merged_df[merged_df["TradeVolume"] >= 500000]
        
        # 5. 計算週轉率
        # 週轉率 (%) = (成交股數 / 發行股數) × 100
        merged_df["turnover"] = (merged_df["TradeVolume"] / merged_df["IssuedShares"]) * 100
        
        # 移除無效的週轉率
        merged_df = merged_df[
            (merged_df["turnover"].notna()) &
            (merged_df["turnover"] != float('inf')) &
            (merged_df["turnover"] >= 0)
        ]
        
        # 6. 標準化輸出
        result_df = pd.DataFrame({
            "code": merged_df["code"],
            "name": merged_df["name"],
            "close": merged_df["ClosingPrice"],
            "turnover": merged_df["turnover"],
            "chg_pct": None,  # TWSE API 沒有漲跌幅，設為 None
            "market": "上市"
        })
        
        return result_df[STANDARD_COLUMNS]
        
    except requests.RequestException as e:
        raise Exception(f"TWSE API 連線錯誤: {str(e)}")
    except Exception as e:
        raise Exception(f"處理 TWSE 資料時發生錯誤: {str(e)}")


def get_tpex_df() -> pd.DataFrame:
    """
    獲取上櫃 (TPEx) 股票的週轉率資料。
    
    流程：
    1. 抓取完整資料（包含股價、成交量、股本）
    2. 資料清洗
    3. 計算週轉率
    
    Returns:
        DataFrame，包含 code, name, turnover, close, chg_pct, market 欄位
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    # 標準欄位
    STANDARD_COLUMNS = ['code', 'name', 'close', 'turnover', 'chg_pct', 'market']
    
    try:
        # 抓取資料
        url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=json"
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            raise Exception("無法從 TPEx API 取得資料")
        
        # 解析 JSON 結構
        # 支援兩種格式：tables[0]['data'] 或 aaData
        if "tables" in data and len(data["tables"]) > 0 and "data" in data["tables"][0]:
            rows = data["tables"][0]["data"]
        elif "aaData" in data:
            rows = data["aaData"]
        else:
            raise Exception("無法解析 TPEx API 資料結構")
        
        if not rows:
            raise Exception("TPEx API 回傳的資料為空")
        
        # 解析資料
        stocks = []
        for row in rows:
            if not row or len(row) < 16:
                continue
            
            # 索引對應：
            # Index 0: 股票代號
            # Index 1: 股票名稱
            # Index 2: 收盤價
            # Index 8: 成交量（股數）
            # Index 15: 發行股數（股數）
            
            code = str(row[0]).strip()
            name = str(row[1]).strip()
            
            # 只保留 4 碼代號
            if len(code) != 4 or not code.isdigit():
                continue
            
            # 移除 HTML 標籤（如果有）
            name = re.sub(r'<[^>]+>', '', name)
            
            # 數值清洗
            close = clean_numeric(row[2])
            volume = clean_numeric(row[8])
            issued_shares = clean_numeric(row[15])
            
            # 過濾條件：成交量 >= 500,000 股（500 張）
            if volume is None or volume < 500000:
                continue
            
            # 計算週轉率
            if issued_shares and issued_shares > 0:
                turnover = (volume / issued_shares) * 100
                
                # 移除無效的週轉率
                if pd.isna(turnover) or turnover == float('inf') or turnover < 0:
                    continue
                
                stocks.append({
                    "code": code.zfill(4),
                    "name": name,
                    "close": close,
                    "turnover": turnover,
                    "chg_pct": None,  # TPEx API 沒有漲跌幅，設為 None
                    "market": "上櫃"
                })
        
        if not stocks:
            raise Exception("無法從 TPEx API 資料中提取有效股票")
        
        result_df = pd.DataFrame(stocks)
        return result_df[STANDARD_COLUMNS]
        
    except requests.RequestException as e:
        raise Exception(f"TPEx API 連線錯誤: {str(e)}")
    except Exception as e:
        raise Exception(f"處理 TPEx 資料時發生錯誤: {str(e)}")


def fetch_turnover_from_api(top_n: Optional[int] = 50) -> pd.DataFrame:
    """
    從 TWSE 和 TPEx API 抓取資料並計算週轉率，合併後返回排名前 N 名。
    
    這是主要的入口函式，會：
    1. 獲取上市資料
    2. 獲取上櫃資料
    3. 合併兩個市場的資料
    4. 排序並取前 N 名
    
    Args:
        top_n: 要返回的前 N 名（預設 50）
    
    Returns:
        DataFrame，包含 code, name, turnover, close, chg_pct, market 欄位
    """
    twse_df = None
    tpex_df = None
    
    # 獲取上市資料
    try:
        twse_df = get_twse_df()
    except Exception as e:
        print(f"⚠️ 獲取上市資料失敗: {str(e)}")
    
    # 延遲 1 秒，避免 API 請求過於頻繁
    time.sleep(1)
    
    # 獲取上櫃資料
    try:
        tpex_df = get_tpex_df()
    except Exception as e:
        print(f"⚠️ 獲取上櫃資料失敗: {str(e)}")
    
    # 合併資料
    if twse_df is not None and tpex_df is not None:
        combined_df = pd.concat([twse_df, tpex_df], ignore_index=True)
    elif twse_df is not None:
        combined_df = twse_df
    elif tpex_df is not None:
        combined_df = tpex_df
    else:
        raise Exception("無法從 TWSE 或 TPEx API 取得任何資料")
    
    # 排序與篩選
    combined_df = combined_df.sort_values("turnover", ascending=False).reset_index(drop=True)
    
    if top_n:
        combined_df = combined_df.head(top_n)
    
    # 移除 market 欄位（與現有系統格式一致）
    result_df = combined_df[["code", "name", "turnover", "close", "chg_pct"]].copy()
    
    return result_df
