import os
import requests
import time
import hashlib
from datetime import datetime, timedelta
from supabase import create_client, Client
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 🔑 GitHub Secrets から鍵を読み込む
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


def get_valid_id(s: dict) -> int:
    """
    NDL APIの1件のspeechレコードからIDを生成する。

    優先順位:
    1. speechIDが純粋な数字 → そのままint化（NDL 8桁連番ID）
    2. speechIDが英数字混じり → SHA-256でbigintに変換
    3. speechIDが完全にない → フォールバック文字列をSHA-256でbigintに変換
    """
    speech_id = s.get("speechID", "")

    # 1. 純粋な数字ならそのままint化
    if speech_id and speech_id.isdigit():
        return int(speech_id)

    # 2. 英数字混じりのspeechIDはSHA-256でbigintに変換
    if speech_id:
        digest = hashlib.sha256(speech_id.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF

    # 3. speechIDが完全にない場合のフォールバック
    fallback_str = f"{s.get('date', 'unknown')}__{s.get('speaker', 'unknown')}__{s.get('speechOrder', '0')}__{s.get('meetingName', 'unknown')}"
    digest = hashlib.sha256(fallback_str.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


def main():
    print("🚀 Polilog 完全自動クレンジング同期エンジン、起動！！")
    try:
        # 1. DBの最新日付を確認して、3日前から取得開始
        res = supabase.table("raw_documents").select("meeting_date").order("meeting_date", desc=True).limit(1).execute()
        latest_date_str = res.data[0]['meeting_date'] if res.data else "2024-01-01"
        latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d")
        start_date = (latest_date - timedelta(days=3)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        print(f"🌐 NDL API に {start_date} 〜 {end_date} のデータを取りに行ってるお...")

        # 2. 会議録取得ループ
        current_start = 1
        upserted_count = 0
        added_count = 0
        skipped_count = 0
        total_records = None

        while True:
            ndl_api_url = "https://kokkai.ndl.go.jp/api/1.0/speech"
            params = {
                "from": start_date,
                "until": end_date,
                "recordPacking": "json",
                "maximumRecords": 100,
                "startRecord": current_start
            }

            response = requests.get(ndl_api_url, params=params)
            response.raise_for_status()
            response_json = response.json()
            speeches = response_json.get("speechRecord", [])

            if total_records is None:
                total_records = int(response_json.get("numberOfRecords", 0))
                print(f"📊 取得対象の総件数: {total_records} 件")

            if not speeches:
                break

            for s in speeches:
                meeting_name = s.get("meetingName", "")

                # 🚨 【異物フィルター】法案や趣意書などは完全スルー
                if "趣意書" in meeting_name or "法案" in meeting_name or "質問" in meeting_name:
                    skipped_count += 1
                    continue

                raw_speaker = s.get("speaker")

                # 🚨 【話者フィルター】
                if raw_speaker in ["会議録情報", "目次"] or not raw_speaker:
                    doc_type_val = "meeting_info"
                    speaker_val = None
                else:
                    doc_type_val = "speech"
                    speaker_val = raw_speaker

                # 🚨 【ファイル名補正】
                m_date = s.get("date")
                dt_obj = datetime.strptime(m_date, "%Y-%m-%d")
                legacy_file_name = dt_obj.strftime("meeting_%Y_%m.json")

                # ID生成
                deterministic_id = get_valid_id(s)

                new_data = {
                    "id": deterministic_id,
                    "file_name": legacy_file_name,
                    "doc_type": doc_type_val,
                    "meeting_date": m_date,
                    "speaker": speaker_val,
                    "content": s
                }

                try:
                    result = supabase.table("raw_documents").upsert(
                        new_data,
                        on_conflict="id",
                        ignore_duplicates=False
                    ).execute()

                    upserted_count += 1
                    if result.data:
                        added_count += 1

                except Exception as e:
                    print(f"⚠️ DB保存エラー (ID: {deterministic_id}): {e}")

            current_start += len(speeches)

            if current_start > total_records:
                break

            time.sleep(1)

        # 3. 完了メール送信
        if upserted_count > 0:
            success_msg = (
                f"🎉 PolilogのDB同期が完了したお！\n\n"
                f"処理件数（新規 + 更新）: {upserted_count} 件\n"
                f"異物スキップ: {skipped_count} 件\n"
                f"今日も1日頑張るお！！🔥🚀"
            )
            send_email("【Polilog】同期レポート: 成功！", success_msg)
        else:
            msg = f"🤷‍♂️ {start_date} 以降の新しい発言は見つからなかったお！"
            print(msg)
            send_email("【Polilog】同期レポート: 更新なし", msg)

    except Exception as e:
        error_msg = f"❌ Polilogの同期中にエラーが発生したお...\n\n詳細:\n{e}"
        print(error_msg)
        send_email("【🚨Polilog】同期エラー発生！！", error_msg)


if __name__ == "__main__":
    main()
