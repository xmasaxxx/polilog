import os
import requests
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

# 💌 メール送信用の専用関数だぜ！
def send_email(subject, body):
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD or not MAIL_TO:
        print("⚠️ メール設定が足りないから送信をスキップするお！")
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
        print("💌 報告メールの送信に成功したお！！")
    except Exception as e:
        print(f"❌ メールの送信に失敗したお...: {e}")

def main():
    print("🚀 Polilog 本番データ同期エンジン、起動！！")
    try:
        # 1. DBの最新日付を確認して、3日前から取得開始
        res = supabase.table("raw_documents").select("meeting_date").order("meeting_date", desc=True).limit(1).execute()
        latest_date_str = res.data[0]['meeting_date'] if res.data else "2024-01-01"
        latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d")
        start_date = (latest_date - timedelta(days=3)).strftime("%Y-%m-%d")
        
        # 2. NDL API にリクエスト！(まずはテストで5件限定)
        ndl_api_url = "https://kokkai.ndl.go.jp/api/1.0/speech"
        params = {"from": start_date, "recordPacking": "json", "maximumRecords": 5}
        
        print("🌐 NDL API に最新の議事録を取りに行ってるお...")
        response = requests.get(ndl_api_url, params=params)
        response.raise_for_status()
        
        speeches = response.json().get("speechRecord", [])
        
        if not speeches:
            msg = f"🤷‍♂️ {start_date} 以降の新しい発言は見つからなかったお！今日はお休みだぜ。"
            print(msg)
            send_email("【Polilog】同期レポート: 更新なし", msg)
            return

        # 3. DBにインサート！
        added_count = 0
        for speech in speeches:
            unique_name = f"{speech.get('issueID')}_{speech.get('speechOrder')}.json"
            new_data = {
                "file_name": unique_name,
                "doc_type": "minutes",
                "meeting_date": speech.get("date"),
                "speaker": speech.get("speaker"),
                "content": speech
            }
            try:
                supabase.table("raw_documents").insert(new_data).execute()
                print(f"✅ 追加: {speech.get('date')} {speech.get('speaker')}")
                added_count += 1
            except Exception as e:
                print(f"⚠️ スキップ（重複かも）: {unique_name}")

        # 4. 大成功メールを送信！！
        success_msg = f"🎉 PolilogのDB同期が完了したお！\n\n新たに {added_count} 件の議事録を追加したぜ！\n今日も1日頑張るお！！🔥🚀"
        send_email("【Polilog】同期レポート: 成功！", success_msg)

    except Exception as e:
        # 万が一エラーが起きても、ちゃんとメールで知らせてくれるプロ仕様だお！
        error_msg = f"❌ Polilogの同期中にエラーが発生したお...\n\n詳細:\n{e}"
        print(error_msg)
        send_email("【🚨Polilog】同期エラー発生！！", error_msg)

if __name__ == "__main__":
    main()
