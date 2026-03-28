import os
import requests
import time
from datetime import datetime, timedelta
from supabase import create_client, Client
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 🔑 GitHub Secrets から鍵を読み込むお！
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
MAIL_TO = os.environ.get("MAIL_TO")

def send_email(subject, body):
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD or not MAIL_TO:
        return
    msg = MIMEMultipart()
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = MAIL_TO
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.send_message(msg)
        server.quit()
    except Exception as e:
        print(f"❌ メール送信エラー: {e}")

def main():
    print("🚀 Polilog 完全自動クレンジング同期エンジン、起動！！")
    try:
        # 1. DBの最新日付を確認して、3日前から取得開始
        res = supabase.table("raw_documents").select("meeting_date").order("meeting_date", desc=True).limit(1).execute()
        latest_date_str = res.data[0]['meeting_date'] if res.data else "2024-01-01"
        latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d")
        start_date = (latest_date - timedelta(days=3)).strftime("%Y-%m-%d")
        # 終了日は今日にするお
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        print(f"🌐 NDL API に {start_date} 〜 {end_date} のデータを取りに行ってるお...")
        
        # ==========================================
        # 💎 2. 会議録取得ループ（100件の壁を突破するお！）
        # ==========================================
        current_start = 1
        added_count = 0
        skipped_count = 0
        
        while True:
            ndl_api_url = "https://kokkai.ndl.go.jp/api/1.0/speech"
            params = {
                "from": start_date, 
                "until": end_date,
                "recordPacking": "json", 
                "maximumRecords": 100,
                "startRecord": current_start # 👈 ここで次の100件を要求するお！
            }
            
            response = requests.get(ndl_api_url, params=params)
            response.raise_for_status()
            speeches = response.json().get("speechRecord", [])
            
            if not speeches:
                break # データが尽きたらループ終了だお！
                
            for s in speeches:
                meeting_name = s.get("meetingName", "")
                
                # 🚨 【異物フィルター】法案や趣意書などは完全スルー
                if "趣意書" in meeting_name or "法案" in meeting_name or "質問" in meeting_name:
                    # print(f"⏩ スキップ（議事録外データ）: {meeting_name}") # ログが長くなるから消してもいいかも
                    skipped_count += 1
                    continue
                    
                raw_speaker = s.get("speaker")

                # 🚨 【話者フィルター】人間じゃないデータは 'meeting_info' に隔離
                if raw_speaker in ["会議録情報", "目次"] or not raw_speaker:
                    doc_type_val = "meeting_info"
                    speaker_val = None
                else:
                    doc_type_val = "speech"
                    speaker_val = raw_speaker

                # 🚨 【ファイル名補正】既存データ（meeting_YYYY_MM.json）に合わせるお！
                m_date = s.get("date")
                dt_obj = datetime.strptime(m_date, "%Y-%m-%d")
                legacy_file_name = dt_obj.strftime("meeting_%Y_%m.json")

                # 保存用のデータを整形
                new_data = {
                    "file_name": legacy_file_name, # 👈 修正：既存ルールに合わせたお！
                    "doc_type": doc_type_val,      
                    "meeting_date": m_date,
                    "speaker": speaker_val,        
                    "content": s
                }
                
                # DBにインサート！
                try:
                    supabase.table("raw_documents").insert(new_data).execute()
                    print(f"✅ 追加: {m_date} {speaker_val} ({doc_type_val})")
                    added_count += 1
                except Exception as e:
                    # 重複エラーは無視して進むお
                    pass

            current_start += len(speeches) # 次の開始位置をズラすお
            if len(speeches) < 100:
                break # 100件未満なら最後のページだから終了！
            time.sleep(1) # NDLサーバーへの優しさ

        # 3. 大成功メールを送信！！
        if added_count > 0:
            success_msg = f"🎉 PolilogのDB同期＆クレンジングが完了したお！\n\n新たに {added_count} 件の綺麗なデータを追加したぜ！(異物スキップ: {skipped_count}件)\n今日も1日頑張るお！！🔥🚀"
            send_email("【Polilog】同期レポート: 成功！", success_msg)
        else:
            msg = f"🤷‍♂️ {start_date} 以降の新しい発言は見つからなかったお！（もしくは既存データのみ）"
            print(msg)
            send_email("【Polilog】同期レポート: 更新なし", msg)

    except Exception as e:
        error_msg = f"❌ Polilogの同期中にエラーが発生したお...\n\n詳細:\n{e}"
        print(error_msg)
        send_email("【🚨Polilog】同期エラー発生！！", error_msg)

if __name__ == "__main__":
    main()
