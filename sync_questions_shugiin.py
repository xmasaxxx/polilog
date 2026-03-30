# src/sync_questions_shugiin.py
"""
衆議院 質問主意書 日次自動同期スクリプト

対象    : 現在の最新会期のみ（毎日 upsert）
テーブル : raw_documents
doc_type : written_question
chamber  : shugiin

設計方針:
  - 最新会期を menu_all.htm から自動検出する
  - 質問・答弁を独立したレコードとして保存（correlation_key でペア紐付け）
  - upsert なので重複登録なし・冪等に実行可能
  - バックフィル（backfill_questions_colab_genesis.py）と完全に同一の
    ID生成ロジック・スキーマを使用する

必須環境変数:
  SUPABASE_URL       : Supabase プロジェクト URL
  SUPABASE_KEY       : service_role キー（RLS バイパス用）
  GMAIL_ADDRESS      : 通知用 Gmail アドレス（任意）
  GMAIL_APP_PASSWORD : Gmail アプリパスワード（任意）
  MAIL_TO            : 通知先メールアドレス（任意）
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
CHAMBER = "shugiin"
BASE_URL = "https://www.shugiin.go.jp"
MENU_URL = f"{BASE_URL}/internet/itdb_shitsumon.nsf/html/shitsumon/menu_all.htm"
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
    """「令和六年十一月二十八日提出」→「2024-11-28」"""
    gengo = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867}
    m = re.search(
        r"(令和|平成|昭和|大正|明治)\s*([0-9一二三四五六七八九十〇]+)年"
        r"([0-9一二三四五六七八九十]+)月([0-9一二三四五六七八九十]+)日提出",
        text
    )
    if m:
        era = m.group(1)
        y  = kanji_to_num(m.group(2))
        mo = str(kanji_to_num(m.group(3))).zfill(2)
        d  = str(kanji_to_num(m.group(4))).zfill(2)
        return f"{gengo[era] + y}-{mo}-{d}"
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
    """答弁書本文から署名者氏名を取得する（例: '石破 茂'）"""
    m = re.search(r"内閣総理大臣[　\s]+(\S+[　\s]\S+?)(?:\n|　|衆議院|参議院)", body_text)
    if m:
        return m.group(1).strip().replace("\u3000", " ")
    return "内閣"


# ---------------------------------------------------------------------------
# ID生成
# ---------------------------------------------------------------------------
def generate_deterministic_id(source_str: str) -> int:
    """SHA-256の上位64bitからPostgreSQL bigint範囲の決定論的IDを生成する"""
    digest = hashlib.sha256(source_str.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


def build_source_str(date: str, speaker: str, diet_num: str, q_num: str, suffix: str) -> str:
    """
    決定論的ID用ソース文字列を生成する。
    バックフィルスクリプトと完全に同一のロジック。
    suffix: "_q"（質問）または "_a"（答弁）
    """
    spk = normalize_speaker(speaker)
    return f"wq_s_{date}_{spk}_{diet_num}_{q_num}{suffix}"


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
def fetch_html(url: str, session: requests.Session, encoding: str = "cp932") -> BeautifulSoup | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = session.get(url, headers=HEADERS, timeout=30)
            res.raise_for_status()
            res.encoding = encoding
            return BeautifulSoup(res.text, "html.parser")
        except requests.RequestException as e:
            wait = 2 ** attempt
            if attempt == MAX_RETRIES:
                print(f"❌ 取得失敗: {url} → {e}")
                return None
            print(f"⚠️  取得エラー {wait}秒後にリトライ...: {e}")
            time.sleep(wait)
    return None


# ---------------------------------------------------------------------------
# 本文取得（堅牢化版）
# ---------------------------------------------------------------------------
def fetch_body_text(url: str, session: requests.Session) -> str:
    soup = fetch_html(url, session)
    if not soup:
        return ""

    # クラス名で探す
    main = soup.select_one(".maintext")
    if main:
        return main.get_text(strip=True)

    text = soup.get_text(strip=True)

    # 答弁書: 「受領」キーワードで本文を削り出す
    if "受領" in text and "答弁" in text:
        body_match = re.search(
            r"(令和.*?受領.*?)(?:経過へ|質問本文|答弁本文|ページの先頭へ)",
            text
        )
        if body_match:
            return body_match.group(1).strip()

    # 質問本文: 「令和〇年〇月〇日提出」から本文を削り出す
    if "提出" in text and "質問" in text:
        body_match = re.search(
            r"((?:令和|平成|昭和).+?提出.+?)(?:ホームページについて|案内図|Copyright)",
            text,
            re.DOTALL
        )
        if body_match:
            return body_match.group(1).strip()

    return ""


# ---------------------------------------------------------------------------
# 最新会期の自動検出
# ---------------------------------------------------------------------------
def fetch_latest_session(session: requests.Session) -> tuple[int, str, str] | None:
    """
    menu_all.htm から最新（最大回次）の会期を取得する。
    Returns: (diet_num, session_name, session_url)
    """
    soup = fetch_html(MENU_URL, session)
    if not soup:
        return None

    sessions = []
    for a in soup.find_all("a", href=True):
        url_match = re.search(r"kaiji(\d+)_l\.htm", a["href"])
        if url_match:
            diet_num = int(url_match.group(1))
            full_url = urljoin(
                f"{BASE_URL}/internet/itdb_shitsumon.nsf/html/shitsumon/",
                a["href"]
            )
            sessions.append((diet_num, a.get_text(strip=True), full_url))

    if not sessions:
        return None

    latest = max(sessions, key=lambda x: x[0])
    print(f"🔍 最新会期: 【{latest[1]}】（第{latest[0]}回）")
    return latest


# ---------------------------------------------------------------------------
# 会期ページ巡回・質問+答弁ペアでレコード生成
# ---------------------------------------------------------------------------
def fetch_session_records(
    diet_num: int,
    session_url: str,
    http: requests.Session,
) -> list[dict]:
    """
    1会期分の全質問について、質問レコードと答弁レコードを
    独立して生成して返す。correlation_key でペアを紐付ける。
    """
    soup = fetch_html(session_url, http)
    if not soup:
        return []

    target_table = next(
        (t for t in soup.find_all("table") if "質問件名" in t.get_text()),
        None,
    )
    if not target_table:
        print("⚠️  質問一覧テーブルが見つかりません")
        return []

    rows = target_table.find_all("tr")[1:]
    records = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        q_num_text = cols[0].get_text(strip=True)
        q_title    = cols[1].get_text(strip=True)
        q_speaker  = cols[2].get_text(strip=True)

        q_tag   = next((a for a in row.find_all("a") if "質問" in a.get_text() and "HTML" in a.get_text()), None)
        ans_tag = next((a for a in row.find_all("a") if "答弁" in a.get_text() and "HTML" in a.get_text()), None)

        if not q_tag and not ans_tag:
            continue

        correlation_key = f"s_{diet_num}_{q_num_text}"

        # 質問本文ページから提出日・答弁URLを取得
        submitted_date = ""
        q_body_text = ""

        if q_tag:
            q_page_url = urljoin(session_url, q_tag.get("href", ""))
            soup_q = fetch_html(q_page_url, http)
            time.sleep(REQUEST_INTERVAL)
            if soup_q:
                submitted_date = parse_japanese_date(soup_q.get_text())
                m = soup_q.select_one(".maintext")
                if m:
                    q_body_text = m.get_text(strip=True)
                else:
                    raw = soup_q.get_text(strip=True)
                    bm = re.search(
                        r"((?:令和|平成|昭和).+?提出.+?)(?:ホームページについて|案内図|Copyright)",
                        raw, re.DOTALL
                    )
                    q_body_text = bm.group(1).strip() if bm else ""
                if not ans_tag:
                    ans_tag = next(
                        (a for a in soup_q.find_all("a", href=True) if "答弁本文(HTML)" in a.get_text()),
                        None,
                    )

        date         = submitted_date or FALLBACK_DATE
        meeting_date = validate_meeting_date(date)

        # ── 質問レコード ──────────────────────────────────────
        if q_tag and q_num_text:
            source_q = build_source_str(date, q_speaker, str(diet_num), q_num_text, "_q")
            records.append({
                "id":              generate_deterministic_id(source_q),
                "file_name":       f"shugiin_{diet_num}.json",
                "doc_type":        DOC_TYPE,
                "chamber":         CHAMBER,
                "sub_type":        "question",
                "correlation_key": correlation_key,
                "meeting_date":    meeting_date,
                "speaker":         q_speaker,
                "content": {
                    "question": q_body_text,
                    "title":    q_title,
                    "polilog_meta": {
                        "sub_type":        "question",
                        "text_length":     len(q_body_text),
                        "original_source": "shugiin_auto_sync",
                    },
                },
            })

        # ── 答弁レコード ──────────────────────────────────────
        if ans_tag:
            ans_url       = urljoin(session_url, ans_tag.get("href", ""))
            ans_body_text = fetch_body_text(ans_url, http)
            time.sleep(REQUEST_INTERVAL)
            signer   = extract_signer(ans_body_text)
            source_a = build_source_str(date, signer, str(diet_num), q_num_text, "_a")
            records.append({
                "id":              generate_deterministic_id(source_a),
                "file_name":       f"shugiin_{diet_num}.json",
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
                        "original_source": "shugiin_auto_sync",
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
            supabase.table(TABLE).upsert(chunk, on_conflict="id").execute()
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
def send_error_report(errors: list[str]) -> None:
    gmail_address = os.environ.get("GMAIL_ADDRESS")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD")
    mail_to = os.environ.get("MAIL_TO")

    if not all([gmail_address, gmail_password, mail_to]):
        print("⚠️  メール通知の設定が不完全なためスキップします。")
        return

    body = f"以下のエラーが発生しました（{len(errors)} 件）:\n\n" + "\n".join(errors)
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = "⚠️ [Polilog] 衆議院質問主意書の同期中にエラーが発生しました"
    msg["From"] = gmail_address
    msg["To"] = mail_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(gmail_address, gmail_password)
            smtp.send_message(msg)
        print("📧 エラーレポートを送信しました。")
    except Exception as e:
        print(f"⚠️  メール送信に失敗しました: {e}")


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------
def main() -> None:
    started_at = datetime.now(timezone.utc)
    print(f"🚀 Polilog 衆議院 質問主意書 日次同期エンジン起動 [{started_at.isoformat()}]")

    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_KEY"],  # service_role キー必須（RLS バイパス）
    )

    with requests.Session() as http:
        # 最新会期を自動検出
        latest = fetch_latest_session(http)
        if not latest:
            print("❌ 最新会期の取得に失敗しました。処理を中断します。")
            raise SystemExit(1)

        diet_num, session_name, session_url = latest
        print(f"\n📂 【{session_name}】の質問主意書を同期中...")

        # レコード生成
        records = fetch_session_records(diet_num, session_url, http)
        q_count = sum(1 for r in records if r["sub_type"] == "question")
        a_count = sum(1 for r in records if r["sub_type"] == "answer")
        print(f"   📊 質問:{q_count}件 / 答弁:{a_count}件 → 合計{len(records)}レコード")

        if not records:
            print("⚠️  取得レコードが0件です。処理をスキップします。")
            return

        # upsert
        upserted, errors = bulk_upsert(supabase, records)

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(f"\n🎉 同期完了！ upsert={upserted}件 / error={len(errors)}件 / 経過={elapsed:.1f}秒")

    if errors:
        send_error_report(errors)
        raise SystemExit(1)  # GitHub Actions にエラーを通知するため非0終了


if __name__ == "__main__":
    main()
