import os
import requests
from supabase import create_client, Client

# GitHub Secrets から接続情報を読み込むお！
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def main():
    print("🚀 Polilog 自動同期エンジン、起動！！")
    try:
        # 1. DBの最新日付を確認（meeting_dateカラムを使用）
        res = supabase.table("raw_documents").select("meeting_date").order("meeting_date", desc=True).limit(1).execute()
        latest_date = res.data[0]['meeting_date'] if res.data else "2024-01-01"
        print(f"📊 現在のDB内最新レコードの日付: {latest_date}")

        # 🚩 今回は「自動同期成功の証」として1件データを追加するお！
        new_data = {
            "file_name": f"auto_sync_{latest_date}.txt",
            "doc_type": "minutes",
            "meeting_date": "2026-03-28", # 今日
            "speaker": "GitHub Actions Bot",
            "content": {"status": "success", "message": "全自動同期システムの稼働に成功したお！"}
        }

        # 2. DBにインサート！！
        supabase.table("raw_documents").insert(new_data).execute()
        print("✅ DBの更新に成功したお！！Polilogはまた一歩進化したぜ！！")

    except Exception as e:
        print(f"❌ エラー発生だお...: {e}")

if __name__ == "__main__":
    main()
