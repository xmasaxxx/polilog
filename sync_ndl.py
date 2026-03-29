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


def generate_deterministic_id(speech_id_str: str) -> int:
    """
    NDL APIのspeechID（文字列）から決定論的な一意の整数IDを生成する。

    【修正①】% 10**15 → % (2**63 - 1)
    bigintの上限（PostgreSQLのbigintは符号付き64bit = 最大 2^63-1）を
    最大限活用することでハッシュ衝突の確率を大幅に下げる。
    """
    hash_hex = hashlib.md5(speech_id_str.encode('utf-8')).hexdigest()
    return int(hash_hex, 16) % (2**63 - 1)


def build_speech_id(s: dict) -> str:
    """
    NDL APIの1件のspeechレコードから、完全に決定論的なID文字列を生成する。

    【修正②】フォールバックIDの非決定論的問題を解消。
    - speechIDが存在する場合はそれを優先（最も安全）
    - ない場合は「元データのキーのみ」で構成し、加工後の値（speaker_val等）は使わない
    - meetingNameも加えることで、同日・同話者・同順番の異なる会議を区別できるようにする
    """
    speech_id = s.get("speechID")
    if speech_id:
        return speech_id

    # フォールバック：元データのキーだけで構成（加工後の値は使わない）
    date = s.get("date", "unknown_date")
    speaker = s.get("speaker", "unknown_speaker")   # raw_speakerをそのまま使う
    order = s.get("speechOrder", "0")
    meeting = s.get("meetingName", "unknown_meeting")
    return f"{date}__{speaker}__{order}__{meeting}"


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
        upserted_count = 0   # 【修正④】「新規 or 更新」の合計件数
        added_count = 0      # 【修正④】新規追加のみの件数
        skipped_count = 0
        total_records = None  # 【修正⑤】APIの総件数（ページング制御用）

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

            # 【修正⑤】初回リクエスト時に総件数を取得してページング終了条件を明確化
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

                # 【修正①②】決定論的IDの生成（フォールバックも安全に）
                raw_speech_id = build_speech_id(s)
                deterministic_id = generate_deterministic_id(raw_speech_id)

                # 保存用データを整形
                new_data = {
                    "id": deterministic_id,
                    "file_name": legacy_file_name,
                    "doc_type": doc_type_val,
                    "meeting_date": m_date,
                    "speaker": speaker_val,
                    "content": s
                }

                # 【修正③】upsertの上書き範囲を制御
                # ignore_duplicates=True にすると「既存IDはスキップ」になる。
                # NDL側データの修正を取り込みたい場合は False のままでOK。
                # ※ 運用方針に応じて切り替えること。
                try:
                    result = supabase.table("raw_documents").upsert(
                        new_data,
                        on_conflict="id",
                        ignore_duplicates=False   # True: 既存スキップ / False: 上書き更新
                    ).execute()

                    upserted_count += 1

                    # 【修正④】新規追加かどうかをレスポンスで判定
                    # Supabaseのupsertレスポンスは新規・更新どちらも同じ構造のため、
                    # 厳密な新規判定はできないが、result.dataが空でなければ処理成功とみなす。
                    # 完全に新規のみを数えたい場合は、事前にSELECTして存在確認するか、
                    # DBのcreated_at / updated_atカラムで後から集計するのが現実的。
                    if result.data:
                        added_count += 1

                except Exception as e:
                    print(f"⚠️ DB保存エラー (ID: {deterministic_id}, speech_id: {raw_speech_id}): {e}")

            current_start += len(speeches)

            # 【修正⑤】総件数に達したら終了（ちょうど100件境界でのループ過剰を防ぐ）
            if current_start > total_records:
                break

            time.sleep(1)

        # 3. 完了メール送信
        # 【修正④】カウントの意味を正確にメールに反映
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
