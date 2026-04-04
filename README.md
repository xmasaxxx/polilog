# polilog

Polilog: The Political Debugger
「1,140万件のログが、日本の政治をデバッグする」

1947年から2026年まで、1,113万件超の国会会議録および3.6万件の質問主意書を完全構造化・同期。独自の数理モデルで政治家の実務能力を評価する、次世代の政治データ解析基盤。

Logic & Protocol
表面的なキーワード検索を超え、7つの独自Protocolで「発言の質」を解体します。

まずはProtocol 1 (MSS): Mass Speech Score. 発言内のファクト（数値・固有名詞・法案）の密度を算出。

Cross-Analysis: 質問主意書と答弁書をペアリングし、はぐらかしや具体性の欠如を検知。

High-Purity Data: 1947年以降の和暦正規化、およびWebナビゲーションノイズを徹底除去。

🛠️ Architecture
Backend: Supabase Pro (PostgreSQL) - 11.4M+ Rows Optimized.

Pipeline: NDL API + Written Questions Scraper (GitHub Actions).

NLP: Python (spaCy / GiNZA) による高度な自然言語処理。
