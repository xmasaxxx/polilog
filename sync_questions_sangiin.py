# sync_questions_sangiin.py
"""
参議院 質問主意書 日次自動同期スクリプト

対象    : 現在の最新会期のみ（毎日 upsert）
テーブル : raw_documents
doc_type : written_question
chamber  : sangiin

設計方針:
  - 最新会期を current/syuisyo.htm から自動検出する
  - 質問・答弁を独立したレコードとして保存（correlation_key でペア紐付け）
  - upsert なので重複登録なし・冪等に実行可能
  - バックフィル（backfill_questions_sangiin_colab.py）と完全に同一の
    ID生成ロジック・スキーマを使用する

必須環境変数:
  SUPABASE_URL             : Supabase プロジェクト URL
  SUPABASE_SERVICE_ROLE_KEY: service_role キー（RLS バイパス用）
  GMAIL_ADDRESS            : 通知用 Gmail アドレス（任意）
  GMAIL_APP_PASSWORD       : Gmail アプリパスワード（任意）
  MAIL_TO                  : 通知先メールアドレス（任意）
"""

import os
import time
import re
import hashlib
import unicodedata
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timezone
from urllib.parse import urljoin
from typing import Any

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
TABLE = "raw_documents"
DOC_TYPE = "written_question"
CHAMBER = "sangiin"
BASE_URL = "https://www.sangiin.go.jp"
CURRENT_URL = f"{BASE_URL}/japanese/joho1/kousei/syuisyo/current/syuisyo.htm"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Polilog-Sync/1.0)"}
REQUEST_INTERVAL = 1.5
MAX_RETRIES = 3
CHUNK_SIZE = 100
FALLBACK_DATE = "1900-01-01"


# ---------------------------------------------------------------------------
# 漢数字→アラビア数字変換 & 提出日パース
# ---------------------------------------------------------------------------
def kanji_to_num(s: str) -> int:
    kanji = {"〇":0,"一":1,"二":2,"三":3,"四":4,"五":5,"六":6,"七":7,"八":8,"九":9,"十":10}
    s = s.strip()
    if s.isdigit():
        return int(s)
    if s == "十":
        return 10
    if len(s) == 2 and s[0] == "十":
        return 10 + kanji.get(s[1], 0)
    if len(s) == 2 and s[1] == "十":
        return kanji.get(s[0], 0) * 10
    if len(s) == 3 and s[1] == "十":
        return kanji.get(s[0], 0) * 10 + kanji.get(s[2], 0)
    return kanji.get(s, 0)


def parse_japanese_date(text: str) -> str:
    """
    参議院質問主意書の本文から提出日を抽出する。
    - 「元年」対応済み
    - 改行・全角スペースを\\s+で吸収
    - 先頭500文字→末尾500文字→全体の順で探す（本文中の日付誤検知防止）
    """
    gengo = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867}
    pattern = r"(?:提出(?:する。?|いたします。?)|答弁書。?)?\s*(令和|平成|昭和|大正|明治)\s*([0-9一二三四五六七八九十〇元]+)\s*年\s*([0-9一二三四五六七八九十]+)\s*月\s*([0-9一二三四五六七八九十]+)\s*日"
    for area in [text[:500], text[-500:], text]:
        m = re.search(pattern, area)
        if m:
            era   = m.group(1)
            y_str = m.group(2)
            y  = 1 if y_str == "元" else kanji_to_num(y_str)
            mo = str(kanji_to_num(m.group(3))).zfill(2)
            d  = str(kanji_to_num(m.group(4))).zfill(2)
            year = gengo[era] + y
            if 1900 < year <= 2030 and mo != "00" and d != "00":
                return f"{year}-{mo}-{d}"
    return ""


# ---------------------------------------------------------------------------
# 議員名正規化 & 答弁書署名者取得
# ---------------------------------------------------------------------------
def normalize_speaker(name: str) -> str:
    name = unicodedata.normalize("NFC", name)
    name = name.replace("\u3000", "").replace(" ", "").strip()
    name = re.sub(r"(君|さん|氏)$", "", name)
    return name


def extract_signer(body_text: str) -> str:
    m = re.search(r"内閣総理大臣[　\s]+(\S+[　\s]\S+?)(?:参議院|衆議院|\n)", body_text)
    if m:
        return m.group(1).strip().replace("\u3000", " ")
    return "内閣"


# ---------------------------------------------------------------------------
# ID生成
# ---------------------------------------------------------------------------
def generate_deterministic_id(source_str: str) -> int:
    digest = hashlib.sha256(source_str.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


def build_source_str(date: str, speaker: str, diet_num: str, q_num: str, suffix: str) -> str:
    spk = normalize_speaker(speaker)
    return f"wq_c_{date}_{spk}_{diet_num}_{q_num}{suffix}"


# ---------------------------------------------------------------------------
# バリデーション
# ---------------------------------------------------------------------------
def validate_meeting_date(date_val: Any) -> str:
    if not date_val:
        return FALLBACK_DATE
    date_str = str(date_val).strip()
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        return FALLBACK_DATE


# ---------------------------------------------------------------------------
# ネットワーク（リトライ付き）
# ---------------------------------------------------------------------------
def fetch_html(url: str, session: requests.Session) -> BeautifulSoup | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = session.get(url, headers=HEADERS, timeout=30)
            res.raise_for_status()
            return BeautifulSoup(res.content, "html.parser")
        except requests.RequestException as e:
            wait = 2 ** attempt
            if attempt == MAX_RETRIES:
                print(f"❌ 取得失敗: {url} → {e}")
                return None
            print(f"⚠️  取得エラー {wait}秒後にリトライ...: {e}")
            time.sleep(wait)
    return None


# ---------------------------------------------------------------------------
# 本文取得
# ---------------------------------------------------------------------------
def fetch_body_text(url: str, session: requests.Session) -> str:
    soup = fetch_html(url, session)
    if not soup:
        return ""
    main = soup.find("div", class_="ta_l")
    if main:
        return main.get_text(strip=True)
    return soup.get_text(strip=True)


# ---------------------------------------------------------------------------
# 最新会期の自動検出
# ---------------------------------------------------------------------------
def fetch_latest_session(session: requests.Session) -> tuple[int, str] | None:
    """
    current/syuisyo.htm のリンクから最新会期番号を取得する。
    Returns: (diet_num, session_url)
    """
    soup = fetch_html(CURRENT_URL, session)
    if not soup:
        return None

    for a in soup.find_all("a", href=True):
        m = re.search(r"/syuisyo/(\d+)/syuisyo\.htm", a["href"])
        if m:
            diet_num = int(m.group(1))
            session_url = f"{BASE_URL}/japanese/joho1/kousei/syuisyo/{diet_num:03d}/syuisyo.htm"
            print(f"🔍 最新会期: 第{diet_num}回")
            return diet_num, session_url

    return None


# ---------------------------------------------------------------------------
# 会期ページ巡回・質問+答弁ペアでレコード生成
# ---------------------------------------------------------------------------
def fetch_session_records(
    diet_num: int,
    session_url: str,
    http: requests.Session,
) -> list[dict]:
    soup = fetch_html(session_url, http)
    if not soup:
        return []

    tables = soup.find_all("table")
    if len(tables) < 2:
        return []

    rows = tables[1].find_all("tr")
    records = []

    for i in range(0, len(rows) - 2, 3):
        try:
            row0 = rows[i]
            row1 = rows[i + 1]
        except IndexError:
            continue

        cols0 = row0.find_all(["td", "th"])
        cols1 = row1.find_all(["td", "th"])

        if len(cols0) < 3 or len(cols1) < 3:
            continue

        q_title   = cols0[2].get_text(strip=True)
        q_num     = cols1[0].get_text(strip=True)
        q_speaker = cols1[2].get_text(strip=True)

        q_tag = row1.find("a", href=lambda h: h and "syuh" in h)
        a_tag = row1.find("a", href=lambda h: h and "touh" in h)

        if not q_tag and not a_tag:
            continue

        correlation_key = f"c_{diet_num}_{q_num}"

        submitted_date = ""
        q_body_text = ""

        if q_tag:
            q_url = urljoin(session_url, q_tag["href"])
            q_body_text = fetch_body_text(q_url, http)
            time.sleep(REQUEST_INTERVAL)
            submitted_date = parse_japanese_date(q_body_text)

        date         = submitted_date or FALLBACK_DATE
        meeting_date = validate_meeting_date(date)

        # ── 質問レコード ──────────────────────────────────────
        if q_tag and q_num:
            source_q = build_source_str(date, q_speaker, str(diet_num), q_num, "_q")
            records.append({
                "id":              generate_deterministic_id(source_q),
                "speech_id":       f"wq_{CHAMBER}_{correlation_key}_question",
                "file_name":       f"sangiin_{diet_num}.json",
                "doc_type":        DOC_TYPE,
                "chamber":         CHAMBER,
                "sub_type":        "question",
                "correlation_key": correlation_key,
                "meeting_date":    meeting_date,
                "speaker":         normalize_speaker(q_speaker),
                "content": {
                    "question": q_body_text,
                    "title":    q_title,
                    "polilog_meta": {
                        "sub_type":        "question",
                        "text_length":     len(q_body_text),
                        "original_source": "sangiin_auto_sync",
                    },
                },
            })

        # ── 答弁レコード ──────────────────────────────────────
        if a_tag:
            a_url         = urljoin(session_url, a_tag["href"])
            ans_body_text = fetch_body_text(a_url, http)
            time.sleep(REQUEST_INTERVAL)
            signer   = extract_signer(ans_body_text)
            source_a = build_source_str(date, signer, str(diet_num), q_num, "_a")
            records.append({
                "id":              generate_deterministic_id(source_a),
                "speech_id":       f"wq_{CHAMBER}_{correlation_key}_answer",
                "file_name":       f"sangiin_{diet_num}.json",
                "doc_type":        DOC_TYPE,
                "chamber":         CHAMBER,
                "sub_type":        "answer",
                "correlation_key": correlation_key,
                "meeting_date":    meeting_date,
                "speaker":         signer,
                "content": {
                    "answer": ans_body_text,
                    "title":  q_title,
                    "polilog_meta": {
                        "sub_type":        "answer",
                        "text_length":     len(ans_body_text),
                        "original_source": "sangiin_auto_sync",
                    },
                },
            })

    return records


# ---------------------------------------------------------------------------
# チャンク分割バルクupsert
# ---------------------------------------------------------------------------
def bulk_upsert(supabase: Client, records: list[dict]) -> tuple[int, list[str]]:
    upserted = 0
    errors: list[str] = []
    total_chunks = (len(records) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(records), CHUNK_SIZE):
        chunk     = records[i : i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        try:
            supabase.table(TABLE).upsert(chunk, on_conflict="speech_id").execute()
            upserted += len(chunk)
            print(f"   💾 chunk {chunk_num}/{total_chunks} ({len(chunk)}件) 完了")
        except Exception as e:
            msg = f"chunk {chunk_num} 失敗: {e}"
            print(f"   ⚠️  {msg}")
            errors.append(msg)

    return upserted, errors


# ---------------------------------------------------------------------------
# メール通知
# ---------------------------------------------------------------------------
def send_email(subject: str, body: str) -> None:
    gmail_address = os.environ.get("GMAIL_ADDRESS")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD")
    mail_to = os.environ.get("MAIL_TO")

    if not all([gmail_address, gmail_password, mail_to]):
        print("⚠️  メール通知の設定が不完全なためスキップします。")
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = gmail_address
    msg["To"] = mail_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(gmail_address, gmail_password)
            smtp.send_message(msg)
        print("📧 メールを送信しました。")
    except Exception as e:
        print(f"⚠️  メール送信に失敗しました: {e}")


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------
def main() -> None:
    started_at = datetime.now(timezone.utc)
    print(f"🚀 Polilog 参議院 質問主意書 日次同期エンジン起動 [{started_at.isoformat()}]")

    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )

    with requests.Session() as http:
        latest = fetch_latest_session(http)
        if not latest:
            print("❌ 最新会期の取得に失敗しました。処理を中断します。")
            raise SystemExit(1)

        diet_num, session_url = latest
        print(f"\n📂 第{diet_num}回国会の質問主意書を同期中...")

        records = fetch_session_records(diet_num, session_url, http)
        q_count = sum(1 for r in records if r["sub_type"] == "question")
        a_count = sum(1 for r in records if r["sub_type"] == "answer")
        print(f"   📊 質問:{q_count}件 / 答弁:{a_count}件 → 合計{len(records)}レコード")

        if not records:
            print("⚠️  取得レコードが0件です。処理をスキップします。")
            return

        upserted, errors = bulk_upsert(supabase, records)

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"\n🎉 同期完了！ upsert={upserted}件 / error={len(errors)}件 / 経過={elapsed:.1f}秒")

    if errors:
        send_email(
            subject="⚠️ [Polilog] 参議院質問主意書の同期中にエラーが発生しました",
            body=f"以下のエラーが発生しました（{len(errors)} 件）:\n\n" + "\n".join(errors)
        )
        raise SystemExit(1)
    else:
        send_email(
            subject="✅ [Polilog] 参議院質問主意書の同期が完了しました",
            body=(
                f"同期が正常に完了しました。\n\n"
                f"会期    : 第{diet_num}回国会\n"
                f"質問    : {q_count} 件\n"
                f"答弁    : {a_count} 件\n"
                f"upsert  : {upserted} 件\n"
                f"経過時間: {elapsed:.1f} 秒\n"
            )
        )


if __name__ == "__main__":
    main()
