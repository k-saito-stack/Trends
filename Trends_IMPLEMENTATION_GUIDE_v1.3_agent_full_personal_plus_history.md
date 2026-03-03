# Trends — 実装指示書（AIエージェントチーム向け / 完全版・個人Firebase前提）
版数: v1.3-agent-full-personal
日付: 2026-03-03  
作成: 要件定義書作成AI  
対象: 個人Gmailで作成したFirebaseプロジェクトを基盤に、社内の少人数（約5名）が @kodansha.co.jp で利用する「トレンド兆し検知」プラットフォーム（MVP→拡張）

---

## 目次
1. [この文書の目的（最重要）](#この文書の目的最重要)  
2. [前提・制約・成功条件（絶対に守る）](#前提制約成功条件絶対に守る)  
3. [コンテキストと設計判断の理由（AIエージェント必読）](#コンテキストと設計判断の理由aiエージェント必読)  
4. [俯瞰レビュー：全体設計の脆弱性と修正（重要）](#俯瞰レビュー全体設計の脆弱性と修正重要)  
5. [推奨アーキテクチャ（現実制約込みの最善）](#推奨アーキテクチャ現実制約込みの最善)  
6. [セキュリティ設計（MVPでも必須）](#セキュリティ設計mvpでも必須)  
7. [データソース設計（無料/安価×信頼性×多様性）](#データソース設計無料安価信頼性多様性)  
8. [Discover（候補発掘）ルール仕様（MVP v1）](#discover候補発掘ルール仕様mvp-v1)  
9. [候補（Candidate）モデルと安全側統合（増殖防止の核）](#候補candidateモデルと安全側統合増殖防止の核)  
10. [スコアリング仕様（数学/実学ベストプラクティス）](#スコアリング仕様数学実学ベストプラクティス)  
11. [根拠（Evidence）と要約（Summary）仕様（幻覚/コスト対策）](#根拠evidenceと要約summary仕様幻覚コスト対策)  
12. [データモデル（Firestore）とインデックス設計](#データモデルfirestoreとインデックス設計)  
13. [日次バッチ実装手順（Runbook）— 完成品カード生成](#日次バッチ実装手順runbook--完成品カード生成)  
14. [スケジューリング（Cloud不可でもベストプラクティス寄り）](#スケジューリングcloud不可でもベストプラクティス寄り)  
15. [リポジトリ構成 / コーディング規約 / CI](#リポジトリ構成--コーディング規約--ci)  
16. [テスト計画（受入/回帰/バックテスト）](#テスト計画受入回帰バックテスト)  
17. [運用（変更履歴・復元・監査・バックアップ）](#運用変更履歴復元監査バックアップ)  
18. [タスク分割（AIエージェントの役割定義）](#タスク分割aiエージェントの役割定義)  
19. [未確定事項（TBD）とデフォルト](#未確定事項tbdとデフォルト)  
20. [付録A: Firestore Security Rules テンプレ](#付録a-firestore-security-rules-テンプレ)  
21. [付録B: GitHub Actions（cron）ワークフロー雛形](#付録b-github-actionscronワークフロー雛形)  
22. [付録C: 設定ドキュメント雛形（/config）](#付録c-設定ドキュメント雛形config)  
23. [付録D: スコアリング擬似コード（Python）](#付録d-スコアリング擬似コードpython)  
24. [付録E: データソース別「規約・安定性・停止スイッチ」表](#付録e-データソース別規約安定性停止スイッチ表)  
25. [参考文献・参照URL](#参考文献参照url)  
26. [付録：過去に検討したが採用しなかった案（車輪の再発明防止）](#付録過去に検討したが採用しなかった案車輪の再発明防止)  

---

## この文書の目的（最重要）
この文書は「作業用AIエージェントチーム」が **一切の前提取り違えなく**実装できるようにするための「実装仕様書」です。

**実装で迷ったら必ずここへ戻ること：**
- 本プロジェクトは「SNS運用自動化」ではなく、**兆し検知と共有**が核。
- UIは **単一ダッシュボード**、カード構造固定。  
  似た機能を別画面に増やして「違う感じに見える」状態を禁止。
- 予算は **月5,000円**（変動費）。超える前に縮退する。
- **無料/安価で信頼性が高い**情報源を優先。  
  有料・規約が厳しい・壊れやすいものは「補助」か「停止スイッチ付き」で扱う。
- スコアは「雰囲気」ではなく、**統計的に筋の良い上振れ検知（baseline→significance）**を核にする。  
  参照：SigniTrend（KDD’14）、Kleinberg burst 等。

---

## 前提・制約・成功条件（絶対に守る）
### 背景・目的（合意）
- 月刊サイクルにより、**話題の人/作品の初動を1ヶ月以内に掴みたい**
- 微弱なトレンド（今後伸びそう）も拾いたい  
- 編集の属人性を減らし、チーム全員が同じ観測結果を共有したい

### 制約（合意）
- 実行: **毎日1回**
- 出力: **上位15件**
- 認証: Firebase Googleログイン / `@kodansha.co.jp` のみ  
- 権限: **全員同一権限（管理者相当）**（運用でカバー）
- 変動費: **〜5,000円/月**（かなり節約設計）
- GitHubにコードがパブリックになる可能性あり  
  → **監視クエリや運用設定はコードに埋めない**（DBで管理）

### 当面のスコープ（合意）
- Instagram: **補強専用**（DiscoverはPCA通過後に機能フラグON）
- 番組表: Phase2（研究要素）
- 通知/自動投稿: Out

### 成功条件（定性中心）
- 日次で「会議に持ち込める候補」が提示される
- 根拠リンクにより「なぜ出たか」が説明できる
- UIが増殖せず見やすい（単一ダッシュボード）
- 予算内に収まる（縮退含めて）

---


## コンテキストと設計判断の理由（AIエージェント必読）
AIエージェントは事前コンテキスト（ユーザーとの会話ログ）を持たない前提で、本節に**合意事項と理由**を一括で書く。

### 1) このプロダクトが解く課題（Why）
- **話題の人/作品の“初動”を取り逃す**（月刊サイクルにより、1ヶ月以内に兆しを掴む必要がある）
- **観測が属人化**しやすい（編集者ごとに見ているSNS/メディアが違う）
- “弱い兆し”は単一ソースだとノイズに埋もれるため、**複数ソース統合**が必要

### 2) 成功の定義（What）
- 毎日1回、上位15件の「兆し候補」が出る
- 候補には「なぜ出たか」を説明する根拠がある（根拠3件＋内訳）
- チーム全員が同じ画面を見て議論できる（単一ダッシュボード）
- 変動費が月5,000円以内で回る（縮退で保証）

### 3) 設計原則（How）
#### 3.1 UI増殖禁止（単一ダッシュボード）
- “同じ機能なのに違う感じに見える”ダッシュボード/タブ/ページを増やすと運用が破綻する  
→ **単一ダッシュボード + カード内展開**で全て表現する

#### 3.2 Discover中心（登録クエリは補助）
- 登録クエリ中心だと「知っているものしか拾えない」  
→ ランキング/トレンド/新着から自動発掘（Discover）を主とし、登録クエリは補助

#### 3.3 設定はコードに埋めない（戦略・公開リポジトリ対策）
- 監視クエリやソースON/OFFがコード内にあると、公開時に戦略が漏れる  
→ `/config` `/queries` をFirestoreに置き、UIで運用変更（履歴/復元付き）

#### 3.4 無料/安価×信頼性の高いソースを核にする
- 「毎日動く」ことが最重要。壊れやすいスクレイピングは核にしない  
→ 公式API/公式フィード/RSS/公的API（Wikimedia等）を核にし、Webページ解析は補助扱い＋kill switch

#### 3.5 スコアは“生の人気”ではなく“上振れ（兆し）”を測る
- 単純な合計（views合計等）は「常に強い人」に引っ張られる  
→ 各ソースで **baseline（通常）→上振れ有意度**へ正規化し統合する（SigniTrend系）

#### 3.6 誤統合より取り逃しを優先（安全側統合）
- 同名別人の誤統合は致命的（誤った意思決定につながる）  
→ 自動統合は高確度のみ。曖昧は分けたまま。運用でalias統合できるようにする

#### 3.7 個人Firebase前提だが“詰み事故”だけは避ける
- 個人プロジェクトでもOK。ただし「アカ停止＝全停止」を避けるため、可能ならオーナーを複数名にする  
- 会社レベルの重装備（RBAC/重監視/大規模基盤）は不要（本指示書も除外）


## 俯瞰レビュー：全体設計の脆弱性と修正（重要）
ここは「瑣末ではない全体不合理」を潰すセクション。**必ず反映**すること。

### 重要な現実制約
> 会社アカウントでは Google Cloud / Firebase のプロジェクトが作れない。  
> 個人Gmailなら可能。

#### これが生む最大リスク（脆弱性）
1) **運用・権限・請求の“個人依存”**  
   - 個人が所有者だと、退職/異動/アカ停止で止まる  
2) **ガバナンス（監査・法務・セキュリティ）**  
   - 社内向けでも、基盤が個人所有だと説明が難しい  
3) **予算管理**  
   - 個人課金・会社精算など運用がブレる

#### 修正方針（現実解）
- **結論**：個人Gmail基盤は「PoC/パイロット」扱いで開始し、設計を **移行前提**にする。  
  「最終的に会社管理へ移す」ことを、最初から仕様として組み込む（後述の移行計画）。
- ただし “今すぐ動く” ことも重要なので、MVPは止めない。

#### 最低限のガバナンス対策（必須）
- 個人GCP/Firebaseプロジェクトに、**会社アカウント（@kodansha.co.jp）を複数名 Owner/IAM Admin として追加**（少なくとも2名）  
- 可能なら **請求アカウント/予算アラート** を設定  
- データは **公開情報の集計・リンク中心**に限定（個人情報や非公開データを入れない）
- 移行しやすいように **設定はDB/JSON、コードはステートレス**にする（後述）

### もう1つの大きな脆弱性：スクレイピング依存
Netflix/ABEMA/TVerなど「公開ページ解析」は、以下のリスクがある：
- DOM変更で壊れる（運用コスト増）
- 規約/ロボット/著作権の論点が出やすい
- 兆し検知の核（毎日安定）を壊し得る

#### 修正方針
- ランキング系Webページは **B（補助）**扱い + **停止スイッチ** + **失敗しても全体を止めない**  
- 「無料で信頼性の高い」優先なら、核は **公式API/公式フィード**へ寄せる（YouTube/Apple RSS/Wiki/RSS/楽天API等）
- 「法務/規約が不安」なら最初は **リンクのみ**（スコアへ入れない）でも良い  
  → その場合もUIは変えない（内訳に入らないだけ）

### 脆弱性：候補抽出のノイズ爆発（日本語固有名詞）
- タイトル・見出しからの抽出はノイズが出やすい  
- LLMで抽出すると精度は上がるがコスト増＆外部送信リスク

#### 修正方針（MVPの最善）
- **高精度（precision）優先**のルールベース抽出 + **マルチソース一致**で拾う  
- 取りこぼしは「登録クエリ（補助）」と「運用でalias追加」で補う  
- LLMの利用は要約のみ（抽出には使わない）を原則にし、予算が余れば抽出を検討

---

## 推奨アーキテクチャ（現実制約込みの最善）
### 結論（推奨）
**GitHub Pages + Firebase Auth + Firestore + GitHub Actions（cron）日次バッチ**  
（GCP側のCloud Scheduler/Runに依存しない）

理由：
- 会社アカウントでGCPプロジェクトが作れない制約下でも成立
- GitHub Actionsはスケジュール実行が可能（多少の遅延は許容）
- Firestoreに「完成品カード」を保存すれば、フロントは読むだけで済む（UI増殖しない）
- 移行（会社管理プロジェクトへ）も比較的容易

> 将来、会社管理のGCPプロジェクトが作れるようになったら  
> Cloud Run Job + Cloud Scheduler に移行する（移行計画参照）。

### 構成図（概念）
1. **Frontend（Public）**  
   - GitHub Pagesで静的配信  
   - Firebase Auth（Google）でログイン  
   - Firestoreから `daily_rankings/{date}/items` を読む  
2. **Data Plane（Private）**  
   - Firestore（Rulesで @kodansha.co.jp のみアクセス）  
3. **Batch（Private）**  
   - GitHub Actions（cron）で日次実行  
   - 収集→候補生成→スコア→Top15→Firestoreへ書き込み  
   - APIキーはGitHub SecretsまたはOIDC（推奨）

---

## セキュリティ設計（MVPでも必須）
### 1) 認証・認可（Firebase Auth + Firestore Rules）
- UIはPublicでもよい（GitHub Pages）  
  **データはRulesで守る**  
- ルールは「認証済み」「メール検証済み」「ドメイン一致」を必須条件にする  
  参照：Firebase RulesとAuthの関係（公式）  
  https://firebase.google.com/docs/rules/rules-and-auth

> 注意: Google OAuthの `hd` はUI最適化であり、セキュリティ制約の中核にしない。  
> 中核は必ず Firestore Rules。

### 2) 変更履歴（全員同権限の安全策）
- 全員が書き込めるなら、**「間違えても戻せる」**が必須  
- config/queries/aliases の変更は必ず `change_logs` に before/after を残す  
- UIに「復元」ボタン（逆適用）を用意

### 3) シークレット管理
#### 必須原則
- 外部APIキーをフロントへ渡さない（絶対）
- 公開repoのコードに埋めない

#### GitHub Actionsの認証（推奨順）
1. **推奨（ベストプラクティス）**: GitHub Actions OIDC → GCP Workload Identity Federation（キー不要）  
   - Google Cloud公式ブログ（キー無し認証）  
     https://cloud.google.com/blog/products/identity-security/enabling-keyless-authentication-from-github-actions
2. **次善（簡便）**: FirebaseサービスアカウントJSONキーをGitHub Secretsに格納  
   - リスク：長期キー漏洩の可能性  
   - 対策：ローテーション・権限最小化

> MVPは2でも動くが、可能なら1を採用する。  
> この文書では **1を標準**として手順を記載し、2は補助にする。

### 4) データの安全性（社内利用でも必須）
- 保存するのは原則として  
  **(a) URL/タイトル/順位/集計値**  
  **(b) 計算済みスコア**  
  **(c) 7日推移（数値のみ）**  
- SNS投稿の本文など「コンテンツ全文」は保存しない（規約・PII・炎上対策）

---

## データソース設計（無料/安価×信頼性×多様性）
### 基本方針（優先順位）
1) **公式API / 公式フィード**（無料or低コスト）を最優先  
2) 準公式（一般公開だが構造変更しやすい）ものはB（補助）  
3) 有料/規約厳しめ/不安定はC（必要時のみ）  
4) どのソースも **ON/OFFをUI設定で可能**にする（停止スイッチ）

### MVP採用ソース（推奨）
| カテゴリ | ソース | 目的 | 方式 | コスト | 信頼性 |
|---|---|---|---|---|---|
| 動画トレンド | YouTube mostPopular JP | Discover核 | YouTube Data API | 低 | 高 |
| 検索トレンド | Google Trends | Discover核 | Trends API alpha優先 / fallback | 無料 | 中〜高 |
| 音楽トレンド | Apple RSS (JP/Global) | 音楽兆し | Apple RSS Generator | 無料 | 高 |
| ニュース/プレス | RSS/公式フィード | 補強 | RSS取得 | 無料 | 中〜高 |
| 雑誌企画 | 楽天Books 雑誌API | 版元横断シグナル | Rakuten API | 低 | 中〜高 |
| Power指標 | Wikipedia Pageviews | 地力補助 | Wikimedia API | 無料 | 高 |
| SNS裏取り | X（xAI x_search） | 根拠補強 | x_search | 変動 | 中 |
| IG補強 | Instagram Business Discovery | 補助 | IG API | 変動 | 中 |

#### 参照URL（実装時に必読）
- YouTube videos.list: https://developers.google.com/youtube/v3/docs/videos/list  
- YouTube 変更履歴: https://developers.google.com/youtube/v3/revision_history  
  （mostPopularがTrending Music/Movies/Gamingを反映する変更がある）  
- Apple RSS Builder: https://rss.marketingtools.apple.com/  
- Google Trends API alpha: https://developers.google.com/search/apis/trends  
- Rakuten Magazine Search: https://webservice.rakuten.co.jp/documentation/books-magazine-search  
- Wikimedia Pageviews: https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html  
- xAI X Search: https://docs.x.ai/developers/tools/x-search  
- IG Business Discovery: https://developers.facebook.com/docs/instagram-platform/instagram-api-with-facebook-login/business-discovery/

---

## Discover（候補発掘）ルール仕様（MVP v1）
### 目標
- 登録クエリなしでも候補が出る（Discover中心）
- ただしノイズを抑え「兆し」を出す（precision優先）
- 同一候補の増殖を防ぐ（安全側統合）

### ルールの基本構造
ソースごとに以下を定義し、**設定としてDBに保存**する（コード埋め込み禁止）：
- `sourceId`
- `enabled`
- `fetchLimit`
- `extractRules`（候補抽出）
- `xDefinition`（日次信号）
- `evidenceRule`
- `stability`（A/B/C）
- `killSwitch`（自動OFF条件）

### ルール一覧（MVP）
#### 1) YouTube mostPopular（JP）
- **取得**: videos.list `chart=mostPopular` `regionCode=JP` `maxResults=N`
- **候補抽出（MVPの最善）**:  
  - `snippet.title` から固有名詞候補を抽出  
  - `snippet.channelTitle` も候補にする（人物/団体の場合が多い）
  - ノイズ除去（後述）
- **日次信号 x(YT,q,t)**:  
  - `rank->E(rank)` で露出スコア化し、候補に紐づく合計  
- **Evidence**: 上位寄与の動画URLを候補ごとに最大3件

注: YouTube docs上、`videoCategoryId` は `chart` と併用でのみ利用可。  
https://developers.google.com/youtube/v3/docs/videos/list

#### 2) Apple RSS（音楽 / JP & Global）
- **取得**: Apple RSS Generatorで生成したJSON URLをフェッチ
- **候補抽出**:
  - Track（曲名）→ MUSIC_TRACK  
  - Artist → MUSIC_ARTIST  
- **日次信号**: ランキング露出（rank→E(rank)）
- **地域係数（合意）**:
  - 日本: w_JP=1.0  
  - Global: w_Global=0.25  
  ※係数は「sig化後」に掛ける（統計整合性）

#### 3) Google Trends（alpha優先）
- **最優先**: Trends API alphaが使えるならそれを使う  
  https://developers.google.com/search/apis/trends  
- **fallback**: 公開トレンドのページ取得（壊れても全体停止しない）
- **候補**: Keyword（固有名詞らしさ判定に通ったもののみ）
- **日次信号**:
  - APIで数値が取れるなら `log1p(value)`  
  - fallbackならランキング露出方式（rank→E(rank)）

#### 4) RSS/公式フィード
- **候補**: 見出しから固有名詞候補を抽出  
- **日次信号**: `log1p(mentions)`  
- **Evidence**: 記事URLを根拠候補に

#### 5) Rakuten Books 雑誌
- **候補**: 雑誌タイトル/説明から候補抽出（WORK/KEYWORD）  
- **日次信号**: `log1p(matchCount)`  
- **Evidence**: 楽天の該当ページURL

#### 6) Wikipedia Pageviews（Power）
- 候補とWikipediaページ（title）が紐づく場合のみPVを取得  
- **PowerScoreとして表示**（TrendScoreへの加点は当面しないのが安全）

#### 7) X Search（xAI x_search）
- Discoverの主入口にするとコストが読めない  
  → **Top候補の裏取り・根拠補強**に寄せる  
- **対象**: 暫定Top30程度に限定（縮退②で減らす）  
- **日次信号**:  
  - MVPは「関連ポストサンプル数（上限付き）」  
  - 反応量の抽出加点はPhase1.5（TBD）

---

## 候補（Candidate）モデルと安全側統合（増殖防止の核）
### 目的
- 「同じ候補が増えすぎて見辛い」を防ぐ（最重要）
- 誤統合（同名別人）を避ける（安全側）

### Candidateタイプ（固定）
- PERSON / GROUP
- WORK
- MUSIC_TRACK / MUSIC_ARTIST
- KEYWORD

### 正規化（Normalize）— 低リスク操作のみ
- Unicode NFKC
- 空白トリム、連続空白圧縮
- 記号の軽微除去（末尾の!等）
- 括弧別名抽出  
  例: `timelesz（タイムレス）` → canonical `timelesz` / alias `タイムレス`

### Resolve（候補ID解決）ルール
**優先順位（固定）**
1) alias辞書: `candidate_aliases[normName]`
2) key辞書: `candidate_keys[type:normName]`
3) 新規作成（ただし固有名詞らしさ判定を通過したもののみ）

#### 固有名詞らしさ判定（MVP必須）
目的：一般語を候補として大量生成しない。

ルール例（設定化）：
- 2文字以下は原則除外（例外: “IU”などはホワイトリスト）
- 数字のみ、記号のみは除外
- “公式/新作/予告/人気/発表”など汎用語のみは除外
- 日本語の助詞・助動詞のみは除外
- ひらがなだけで3文字以下は除外（例外: 固有名詞ホワイトリスト）

**ホワイトリスト/ブラックリストはUIで編集可能**にする（変更履歴付き）

### 自動統合（Safe Merge）の厳格条件
自動統合は最小限にする。**alias追加で吸収**が基本。

自動統合OK（高確度のみ）：
- 同一ソースID一致（例: videoId等）
- 正規化後完全一致 + 同日2ソース以上で同時上振れ + 固有名詞らしさ判定OK
- 括弧別名（alias登録）

それ以外は統合しない（誤統合リスクが高い）

### 手動統合（運用）
UIで alias追加により実現。
- 統合は「片方向」  
  - BをAのaliasにする（Aがcanonical）
- 取り消しは alias削除（change_logsで復元可能）

---

## スコアリング仕様（数学/実学ベストプラクティス）
### 基本思想
- 兆し = 「通常状態（ベースライン）からの上振れ」  
- 各ソースの生値を足すのではなく、**有意度（significance）に正規化して統合**  
- 弱い兆しは単一ソースだとノイズになりやすい  
  → **マルチソース一致ボーナス**で強化

### 参照（実装者は読む）
- SigniTrend（KDD’14）  
  https://dl.acm.org/doi/pdf/10.1145/2623330.2623740  
- Kleinberg burst detection（古典）  
  https://www.cs.cornell.edu/home/kleinber/bhs.pdf  
- DCG（ランキング上位重視の標準概念）  
  https://en.wikipedia.org/wiki/Discounted_cumulative_gain  
- Twitter S-H-ESD（季節性が強い場合の代替案。将来）  
  https://blog.x.com/engineering/en_us/a/2015/introducing-practical-and-robust-anomaly-detection-in-a-time-series

---

### 1) 日次信号 x(s,q,t) の定義（統一）
#### ランキング系（YouTube mostPopular / Apple music / Netflix等）
- 露出スコア:
\[
E(rank)=\frac{1}{\log_2(rank+1)}
\]
- 日次信号（候補qが複数エントリに出る場合は合計）:
\[
x_{rank}(s,q,t)=\sum E(rank_i)
\]

> これにより「順位が上位ほど価値が高い」が自然に表現できる。

#### カウント系（RSS見出し、楽天雑誌マッチ件数）
\[
x_{count}(s,q,t)=\log(1+\mathrm{count})
\]

#### Power系（Wikipedia PV）
\[
power(q,t)=\log(1+\mathrm{pageviews})
\]
※ TrendScoreへの加点は当面しない（表示のみが安全）

---

### 2) ベースラインと有意度（sigβ）
#### 状態として保持するもの（候補×ソース）
- EWMA平均 `m_t`
- 指数移動分散 `v_t`（EWMVar）
- last更新日、直近sig

#### EWMA更新
\[
m_t=(1-\alpha)m_{t-1}+\alpha x_t
\]

#### EWMVar更新（安定版）
\[
v_t=(1-\alpha)\left(v_{t-1}+\alpha(x_t-m_{t-1})^2\right)
\]

#### 有意度（sigβ）
\[
sig_\beta(x_t)=\frac{x_t-\max(m_t,\beta)}{\sqrt{v_t}+\beta}
\]
- βはゼロ割防止 + 雑音カット（設定化、例: 0.1）

> SigniTrend等では「閾値超え」をアラートにするが、本プロジェクトでは
> “スコアリング”に使うため、sig値を連続値として扱う。

---

### 3) 半減期（half-life）からαを作る（設定化）
\[
\alpha = 1-\exp\left(\frac{\log(1/2)}{t_{1/2}}\right)
\]
- 例: half-life=7（日） → 1週間で過去影響が半分になる

推奨初期値（MVP）：
- dailyバッチなので、half-lifeは **7日** or **10日**が妥当  
  （月刊の“初動”を取るため短期寄り）

---

### 4) Momentum（短期連続上昇の強化）
\[
momentum_t = \max(0,sig_t)+\lambda \max(0,sig_{t-1})+\lambda^2 \max(0,sig_{t-2})
\]
- λは減衰（例: 0.7、設定化）

---

### 5) マルチソース一致（multiBonus）
- `activeSources = count(sig_t >= minSig)`  
- `multiBonus = multiWeight * clamp(activeSources-1, 0, 3)`

推奨初期値：
- `minSig = 2.0`
- `multiWeight = 1.0`

理由：
- sig>=2は「通常より明確に上」程度の実務閾値として扱いやすい
- ただし初期期間はvが小さく暴れるので、warmup期間の扱いが必要（後述）

---

### 6) 音楽の地域係数（合意・必須）
- 音楽はJP優先、Global低め  
- 手順:
  1. JPとGlobalで別々にx→sig→momentumを計算
  2. 係数を掛けて合成

\[
musicScore = 1.0 \cdot momentum_{JP} + 0.25 \cdot momentum_{Global}
\]

---

### 7) 統合TrendScore（候補ごと）
**表示バケット（固定・UI増殖防止）**
- TRENDS
- YOUTUBE
- X
- NEWS_RSS
- RANKINGS_STREAM（Netflix/ABEMA/TVerまとめ）
- MUSIC（Apple+YouTube音楽まとめ、展開でJP/Global）
- MAGAZINES（楽天）
- INSTAGRAM_BOOST（当面補強）

TrendScoreは基本：
\[
TrendScore(q,t)=\sum_b w_b \cdot Score_b(q,t) + multiBonus(q,t)
\]

MVPでは `w_b=1` を基本とし、音楽は内部で地域係数を適用。

> 重要：wを増やして調整を始めると、説明性が落ちる。  
> MVPは「統計正規化 + multi」で十分強い。  
> 重み調整はバックテストをしてから。

---

### 8) Warm-up / 欠損 / 外れ値（実装必須）
#### Warm-up（初回の数日）
- vが小さく sigが過大になりがち  
- 対策（どちらか採用、設定化）
  - (A) 最初の`warmupDays`は `sig=0` で学習だけ行う（推奨: 3日）
  - (B) βを大きめにして最初の暴れを抑える

推奨: A（シンプルで事故が少ない）

#### 欠損
- ソース取得失敗は `x=None` とし  
  - 状態更新はスキップ（m,vを更新しない）  
  - その日は寄与0として扱う
- 「欠損で0にする」と誤って下降扱いになるので避ける

#### 外れ値
- ランキングやカウントは `log` や `E(rank)` である程度安定  
- それでも暴れる場合は `x` の上限クリップ（設定化）

---

## 根拠（Evidence）と要約（Summary）仕様（幻覚/コスト対策）
### Evidence（根拠3件）仕様
- 目的：編集者が “なぜ上がっているか” をすぐ確認できる
- 候補カードに最大3件を表示（固定）

Evidence最小項目：
- `sourceId`
- `title`
- `url`
- `publishedAt`（取れるなら）
- `metric`（rank/viewCount等、取れる範囲で）
- `snippet`（任意、短く、PIIマスク）

**選定ルール（固定）**
1) TrendScore内訳で寄与が大きいソースを優先  
2) 同一ドメイン3件独占を避ける（可能な範囲で分散）  
3) 新しさ・説明力のあるタイトルを優先  
4) リンク切れは繰り上げ

---

### 要約（Summary）仕様
要約は「便利」だが、コスト・幻覚・外部送信リスクの源泉。  
MVPは以下で守る。

**強制ルール**
- 要約は「根拠にない断定」を禁止  
- 入力は evidenceTop3 の `title + metric + source` のみ（本文を渡さない）
- 出力はJSONで受け、UIに安全に表示する

**モード（縮退と連動）**
- `LLM`: 通常（1〜2行）
- `TEMPLATE`: テンプレ（例: “YouTubeとTrendsで同時上昇…”）
- `OFF`: “予算節約のため要約停止中”

**キャッシュ**
- `summaryHash = sha256(candidateId + date + evidenceTop3(url+title+metric))`
- 同hashなら再生成しない

---

## データモデル（Firestore）とインデックス設計
### コレクション一覧（MVP）
- `/config/app`
- `/config/sources/{sourceId}`
- `/config/algorithm`
- `/config/music`
- `/queries/{queryId}`
- `/candidates/{candidateId}`
- `/daily_rankings/{date}`
- `/daily_rankings/{date}/items/{candidateId}`
- `/runs/{runId}`
- `/cost_logs/{yyyymm}/days/{date}`
- `/change_logs/{logId}`
- （任意）`/snapshots/config/{timestamp}`（設定バックアップ）

### daily_rankings（完成品カード）設計
#### `/daily_rankings/{date}`（メタ）
必須：
- `date`
- `generatedAt`
- `runId`
- `topK=15`
- `degradeState`（summary/xSearch/paidNews/igBoost）
- `algorithmVersion`
- `musicWeights`（JP=1.0/Global=0.25）

#### `/daily_rankings/{date}/items/{candidateId}`（カード）
必須（固定）：
- `rank`
- `candidateId`
- `candidateType`
- `displayName`
- `trendScore`
- `breakdownBuckets`（固定バケット）
- `breakdownDetails`（展開用）
- `sparkline7d`（数値7点 or null）
- `evidenceTop3`
- `summary`
- `power`（任意）

### candidates（候補マスター）
必須（推奨）：
- `candidateId`
- `type`
- `canonicalName`
- `displayName`
- `aliases[]`
- `createdAt`
- `lastSeenAt`
- `status`（ACTIVE/MERGED/BLOCKED）
- `sourceState{sourceId:{m,v,lastSig,lastUpdated}}`
- `trendHistory7d`（リングバッファ）

### インデックス（最小）
- daily_rankings/items は基本 `rank` 昇順で読みたい  
  → クエリでorderBy(rank)を使う場合、単純でOK
- candidates検索はUIでalias編集に使う  
  → `canonicalName` の前方一致などは難しいので、UIは “ID検索 or 最近出た候補一覧” 中心で良い  
  → 本格検索はPhase2

---

## 日次バッチ実装手順（Runbook）— 完成品カード生成
> 重要: バッチは「失敗してもUIを壊さない」。  
> 失敗時は前日結果を表示できるようにする。

### 0) 入力
- config一式
- 当月 cost_logs
- candidates（状態）

### 1) Run開始
- `runId=ULID()`
- `targetDate=JSTの日付`
- degradeState決定（当月累計に応じて）
- runs/{runId}へ開始ログ

### 2) 収集（Ingest）
- ソースごとに fetch  
- 失敗しても継続（欠損扱い）
- runsに件数/時間/エラー記録

### 3) RawCandidate生成（Extract）
- ソースごとに候補抽出
- ノイズ抑制（固有名詞らしさ判定、上限、ストップワード）

### 4) Normalize → Resolve（候補ID付与）
- alias→key→新規の順  
- 新規作成は固有名詞判定OKのみ

### 5) 日次信号 x(s,q,t)
- ランキング: rank→E(rank)
- カウント: log1p(count)
- X/IG: 安定値のみ（件数/増分）

### 6) EWMA/EWMVar更新 → sig → momentum
- warmupDays中は sig=0（学習のみ）

### 7) バケットへ集約 + multiBonus
- breakdownDetailsを作る
- breakdownBuckets（上位3〜4 + Other）を作る
- trendScoreの整合性チェック

### 8) Top15選抜
- trendScore降順、tieは `multiBonus`、次に `power` など
- 15件未満なら前日を表示（当日書き込みを止める）

### 9) EvidenceTop3選定
- ソース寄与と新しさで選ぶ
- ドメイン分散を試みる

### 10) 要約生成（summary）
- degradeStateに応じてLLM/Template/OFF
- hashキャッシュ

### 11) Write（Firestore）
- daily_rankings/{date}メタ
- itemsを15件書き込み

### 12) 終了ログ + cost_logs
- 主要API呼び出し回数と概算を記録
- run終了ステータス

---

## スケジューリング（Cloud不可でもベストプラクティス寄り）
### 推奨（現実制約込み）
- **GitHub Actions cron** を主にする  
  - 例：JST 07:00 実行（GitHubはUTC基準なので換算）
  - 遅延があっても「毎日1回」なら許容

### 代替/補助
- **Google Apps Script（GAS）でキック**（必要なら）
  - GitHub Actions workflow_dispatchを叩く
  - ただしGASはクォータ/不安定要素があるため、主役にしない

### 将来（会社プロジェクトが可能になったら）
- Cloud Scheduler → Cloud Run Job（最も標準的）
- GitHubはCI/CDのみへ

---

## リポジトリ構成 / コーディング規約 / CI
### リポジトリ構成（推奨）
```
/
  apps/
    web/                 # GitHub Pages (React/Vite)
  packages/
    core/                # scoring, normalization, models (pure python or TS)
    connectors/          # source connectors (python)
  infra/
    firebase/            # rules, indexes
    gcp/                 # (optional) WIF config notes
  scripts/
    backtest/            # offline eval
  docs/
    IMPLEMENTATION_GUIDE.md
  .github/
    workflows/
      daily_batch.yml
      deploy_web.yml
```

### 言語
- Batch: Python 3.11（推奨）
- Web: TypeScript + React
- 共通: JSON Schema（データ契約）

### 規約（必須）
- Lint: ruff / eslint
- 型: mypy / tsconfig strict
- フォーマット: black / prettier
- テスト: pytest / vitest
- secretsをログに出さない（絶対）

---

## テスト計画（受入/回帰/バックテスト）
### 受入テスト（MVP必須）
- ログイン（@kodansha.co.jpのみ通る）
- ダッシュボードが15件表示される
- 各カードに①〜⑤が存在する
- 設定変更→change_logsに残る
- 復元ができる
- バッチ失敗時に前日結果が表示される

### 回帰テスト（自動）
- 各コネクタのパース（スナップショット）
- スコアリング整合性（trendScore == sum(buckets)+multi）
- warmup/欠損の扱い
- 音楽地域係数が正しい（JP>Global）

### バックテスト（推奨）
- 過去データを1〜3ヶ月分収集し、  
  - 伸びた対象（後から大きくなった話題）に対して  
  - 何日前からTop15に入ったか（リードタイム）  
  - ノイズ率  
  を測定し、minSig/halfLife/multiWeightを調整

---

## 運用（変更履歴・復元・監査・バックアップ）
### 変更履歴（必須）
- `/config/*` `/queries/*` `/candidates alias` の変更は必ず change_logs に保存
- change_logsからワンクリック復元（逆適用）

### 設定バックアップ（推奨）
- 日次バッチの最後に `/snapshots/config/{timestamp}` にconfig一式を保存  
  （万一の破壊に備える）

### 監査ログ（最低限）
- runログ（成功/失敗/件数/時間）
- costログ（APIごとの概算）

---

## タスク分割（AIエージェントの役割定義）
### Agent 1: Security/Infra
- Firebase Auth, Firestore Rules（ドメイン縛り）
- WIF（GitHub Actions OIDC）設定
- GitHub Secrets整理（API key類）
- 最低限の監査ログ（runs/cost）

### Agent 2: Connectors
- YouTube / Apple RSS / Trends / RSS / Rakuten / Wiki
- B系（Netflix/ABEMA/TVer）は killSwitch付き（任意）

### Agent 3: Candidate Engine
- normalize / resolve / alias辞書
- ノイズ抑制（固有名詞判定・stopwords・ホワイトリスト）

### Agent 4: Scoring
- EWMA/EWMVar/sigβ/momentum/multiBonus
- 音楽地域係数
- breakdown生成（バケット固定）
- 整合性チェック

### Agent 5: Frontend
- 単一ダッシュボード（カード固定）
- 設定ドロワー（source/algorithm/music/queries/aliases）
- change_logs復元UI

### Agent 6: QA/Eval
- E2E（ログイン→表示→設定変更→復元）
- バックテスト harness
- コスト検証（縮退シミュレーション）

---

## 未確定事項（TBD）とデフォルト
未回答でも進めるためデフォルトを置く。

1) バッチ実行時刻  
- デフォルト: JST 07:00

2) Trends API alphaが取れない場合のfallback  
- デフォルト: 公開トレンドページ（壊れても止めない）

3) PowerScoreのTrendScore加点  
- デフォルト: しない（表示のみ）

4) LLM要約の初期モード  
- デフォルト: LLM（Top15のみ）だが、月予算の60%でTemplateへ縮退開始

5) データ保持期間  
- デフォルト: 12ヶ月（configで変更可）

---

## 付録A: Firestore Security Rules テンプレ
> 実装前に必ずレビュー。ドメイン縛りの正規表現を間違えると全漏洩になる。

```
// firestore.rules（擬似例。実際はFirebase Rules構文に合わせる）
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {

    function isSignedIn() {
      return request.auth != null;
    }

    function isAllowedDomain() {
      return isSignedIn()
        && request.auth.token.email_verified == true
        && request.auth.token.email.matches(".*@kodansha\\.co\\.jp$");
    }

    match /daily_rankings/{date} {
      allow read: if isAllowedDomain();
      allow write: if false; // メタの書き込みはサーバ（Admin SDK）だけにするのが理想
      match /items/{candidateId} {
        allow read: if isAllowedDomain();
        allow write: if false; // 同上
      }
    }

    match /config/{doc} {
      allow read, write: if isAllowedDomain();
    }
    match /config/sources/{sourceId} {
      allow read, write: if isAllowedDomain();
    }
    match /queries/{queryId} {
      allow read, write: if isAllowedDomain();
    }
    match /candidates/{candidateId} {
      allow read, write: if isAllowedDomain();
    }
    match /change_logs/{logId} {
      allow read, write: if isAllowedDomain();
    }
    match /runs/{runId} {
      allow read: if isAllowedDomain();
      allow write: if false;
    }
    match /cost_logs/{yyyymm}/days/{date} {
      allow read: if isAllowedDomain();
      allow write: if false;
    }
  }
}
```

> 注意: Firestore Rulesでは「フィールド単位で読み取り制限」はできない。  
> 設定に機密があるならコレクションを分ける設計にする。  
> 参照: https://firebase.google.com/docs/firestore/security/rules-fields

---

## 付録B: GitHub Actions（cron）ワークフロー雛形
> これは雛形。実装時にPython環境、OIDC、Secretsを正しく設定する。

```yaml
name: Daily Batch

on:
  schedule:
    - cron: "0 22 * * *" # UTC 22:00 = JST 07:00
  workflow_dispatch: {}

permissions:
  id-token: write   # OIDC用（WIF）
  contents: read

jobs:
  run-batch:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          python -m pip install -U pip
          pip install -r requirements.txt

      # ここでGCP WIFを設定（推奨）
      # - uses: google-github-actions/auth@v2
      #   with:
      #     workload_identity_provider: "projects/..../locations/global/workloadIdentityPools/.../providers/..."
      #     service_account: "batch-writer@PROJECT_ID.iam.gserviceaccount.com"

      - name: Run batch
        env:
          YOUTUBE_API_KEY: ${{ secrets.YOUTUBE_API_KEY }}
          RAKUTEN_APP_ID: ${{ secrets.RAKUTEN_APP_ID }}
          XAI_API_KEY: ${{ secrets.XAI_API_KEY }}
          FIREBASE_PROJECT_ID: ${{ secrets.FIREBASE_PROJECT_ID }}
          # もしサービスアカウントJSON方式なら:
          # FIREBASE_SA_JSON: ${{ secrets.FIREBASE_SA_JSON }}
        run: |
          python -m batch.run --date "today"
```

---

## 付録C: 設定ドキュメント雛形（/config）
### /config/app
```json
{
  "topK": 15,
  "timezone": "Asia/Tokyo",
  "runTimeJST": "07:00",
  "retentionMonths": 12,
  "environment": "poc-personal-gcp",
  "degrade": {
    "monthlyBudgetJPY": 5000,
    "thresholds": {
      "templateAtRatio": 0.6,
      "xSearchReduceAtRatio": 0.8
    }
  }
}
```

### /config/music
```json
{
  "weights": { "JP": 1.0, "GLOBAL": 0.25 },
  "sources": ["APPLE_MUSIC_JP", "APPLE_MUSIC_GLOBAL"]
}
```

### /config/algorithm
```json
{
  "halfLifeDays": 7,
  "beta": 0.1,
  "warmupDays": 3,
  "minSig": 2.0,
  "multiWeight": 1.0,
  "momentumLambda": 0.7,
  "maxXClip": 50
}
```

---

## 付録D: スコアリング擬似コード（Python）
```python
def rank_exposure(rank: int) -> float:
    return 1.0 / math.log2(rank + 1)

def alpha_from_half_life(half_life_days: float) -> float:
    return 1.0 - math.exp(math.log(0.5) / half_life_days)

def ewma_update(m_prev, x, alpha):
    return (1 - alpha) * m_prev + alpha * x

def ewmvar_update(v_prev, x, m_prev, alpha):
    # stable-ish update
    return (1 - alpha) * (v_prev + alpha * (x - m_prev) ** 2)

def sig_beta(x, m, v, beta):
    denom = math.sqrt(max(v, 0.0)) + beta
    return (x - max(m, beta)) / denom

def momentum(sig_hist, lam=0.7):
    # sig_hist: [sig_t, sig_t-1, sig_t-2]
    s0 = max(0.0, sig_hist[0])
    s1 = max(0.0, sig_hist[1])
    s2 = max(0.0, sig_hist[2])
    return s0 + lam * s1 + (lam ** 2) * s2
```

---

## 付録E: データソース別「規約・安定性・停止スイッチ」表
| sourceId | 種別 | 安定性 | 規約リスク | killSwitch条件例 | 初期 |
|---|---|---:|---:|---|---|
| YOUTUBE_TREND_JP | 公式API | 高 | 低 | 連続失敗3回→OFF | ON |
| APPLE_MUSIC_JP | 公式RSS | 高 | 低 | 連続失敗3回→OFF | ON |
| TRENDS | alpha/公開 | 中 | 中 | 失敗率高→fallbackへ | ON |
| NEWS_RSS | RSS | 中 | 低〜中 | フィード404→OFF | ON |
| RAKUTEN_MAG | 公式API | 中 | 低 | quota超→間引き | ON |
| WIKI_PAGEVIEWS | 公式API | 高 | 低 | 連続失敗3回→OFF | ON |
| NETFLIX_TOP10 | 公開ページ | 中 | 中 | DOM変更で失敗→OFF | OFF(任意) |
| ABEMA_RANKING | 公開ページ | 低〜中 | 中 | DOM変更で失敗→OFF | OFF(任意) |
| TVER_EP | 公開ページ | 低〜中 | 中 | DOM変更で失敗→OFF | OFF(任意) |
| X_SEARCH | 有料/変動 | 中 | 中 | 予算比率で削減 | ON(縮退対象) |
| IG_BOOST | API/審査 | 中 | 中 | 取得不能→OFF | OFF(任意) |

---

## 参考文献・参照URL
- SigniTrend (KDD 2014): https://dl.acm.org/doi/pdf/10.1145/2623330.2623740  
- Kleinberg burst detection: https://www.cs.cornell.edu/home/kleinber/bhs.pdf  
- DCG: https://en.wikipedia.org/wiki/Discounted_cumulative_gain  
- YouTube videos.list: https://developers.google.com/youtube/v3/docs/videos/list  
- YouTube revision history: https://developers.google.com/youtube/v3/revision_history  
- Apple RSS builder: https://rss.marketingtools.apple.com/  
- Google Trends API alpha: https://developers.google.com/search/apis/trends  
- Firebase Rules & Auth: https://firebase.google.com/docs/rules/rules-and-auth  
- Firestore field rules limitation: https://firebase.google.com/docs/firestore/security/rules-fields  
- GitHub Actions keyless auth to GCP: https://cloud.google.com/blog/products/identity-security/enabling-keyless-authentication-from-github-actions  
- Twitter S-H-ESD blog: https://blog.x.com/engineering/en_us/a/2015/introducing-practical-and-robust-anomaly-detection-in-a-time-series  
- Rakuten magazine search: https://webservice.rakuten.co.jp/documentation/books-magazine-search  
- Wikimedia pageviews: https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html  
- xAI X Search: https://docs.x.ai/developers/tools/x-search  
- IG Business Discovery: https://developers.facebook.com/docs/instagram-platform/instagram-api-with-facebook-login/business-discovery/

---

# 重要メモ（この文書の運用）
- この文書は「仕様の単一の真実」。  
  実装がズレたら、実装を直す（文書に合わせる）か、合意の上で文書を更新する。
- 追加のデータソースや係数調整は **設定（Firestore）**で行い、コードの公開リスクを減らす。

---

## 付録：過去に検討したが採用しなかった案（車輪の再発明防止）
このプロジェクトでは「早く動く」「毎日安定」「5,000円/月」「UI増殖禁止」を最優先にしています。  
そのため、過去に検討して**意図的にやめた案**をここに残します（後から別の人が同じ議論を繰り返さないため）。

> ここでいう「やめた」は **永久に禁止**ではなく、条件が変われば再検討します。  
> ただし“いまの前提（個人開発＋少人数＋節約）”では採用しない、という意味です。

### 採用しなかった案一覧
| 検討案 | 期待した効果（なぜ検討したか） | 採用しなかった理由（やめた理由） | 再検討条件（いつ戻す？） |
|---|---|---|---|
| Cloud Run + Cloud Scheduler で日次バッチ | 業界標準で堅牢、鍵管理や実行が安定 | 会社アカウントでGCPプロジェクト作成不可 / 個人GCPでやるにしても設定が重い。MVPは「動くまで」を優先 | 会社管理GCPが使える、または運用人数が増え“実行の確実性”が最重要になったら |
| Workload Identity Federation（GitHub OIDCで鍵無し） | 長期キー不要でセキュア（ベストプラクティス） | 初期設定がやや難しい。個人開発ではまずGitHub Secretsで十分。大事故リスクが上がったら導入 | 公開リポジトリで運用が長期化 / セキュリティレビュー対象になったら |
| Spotify API をMVPの核にする | 音楽トレンドが強く拾える（K-POP等） | Spotify側の開発者制限・仕様変更が続いており、安定運用が難しい可能性。無料・信頼性の条件から外れやすい | Spotify側の安定した取得手段が確立 / 公式に持続可能な方法が確認できたら |
| オリコン／Billboard等を自動収集して数値化 | 日本の権威あるチャートで強い指標 | 「無料で自動収集して保存・再利用」が利用条件/権利の論点になりやすい（許諾が必要になりがち） | 公式ライセンス契約が取れた、または公式API提供が確認できたら |
| Instagram Discover（ハッシュタグ探索）を最初からON | IG起点で新規候補発掘ができる | PCA（Public Content Access）審査が不確実で、手続きが重い。まずは補強専用にしてMVPを止めない | PCA承認が取れたら機能フラグでON |
| X（xAI x_search）をDiscoverの主入口にする | Xは早いので“初動”に強そう | 取得コストが変動しやすく、5,000円/月制約に直撃。検索結果の再現性も揺れやすい | 予算が増える / 取得量を固定できる / 無料枠が十分なら |
| LLMで固有名詞抽出（NER）を全面採用 | 表記ゆれや抽出精度を上げられる可能性 | コスト・再現性・誤抽出（幻覚）リスク。さらに外部送信範囲が増える。MVPはルール＋マルチソース一致＋運用aliasで十分 | 予算拡大 / 精度課題が顕在化 / オンプレや低コスト推論で安定化できたら |
| 取得したSNS本文・記事本文を全文保存 | 後から分析・学習に使える | 規約・著作権・個人情報のリスクが大きい。保管責任が増える。MVPはURL＋最小メタのみで目的達成 | 会社の法務/セキュリティ体制で保管が許容され、目的が「分析」中心になったら |
| Slack/メール通知 | 見逃しを減らせる | 重要度の調整が難しく、通知疲れのリスク。実装・運用コストが増える。今回は「全員が同じものを見る」が目的 | “見に行かない”問題が顕在化し、通知が本当に必要になったら |
| 投稿案生成〜自動投稿まで（SNS運用自動化） | 工数削減に直結 | そもそもスコープ外（合意）。責任範囲が増える。まずは兆し検知と共有が核 | 兆し検知が安定し、次の課題としてSNS運用自動化が正式に入ったら |
| 複数ダッシュボード（ソース別タブ多数） | 情報源ごとに見やすい | 「同じ機能なのに違う感じに見える」状態になり、見辛いという明確なNG要望に反する。単一画面に統合する方針 | 利用者が増え、役割別にビューが必要になったら（ただし基本はカード内展開で対応） |
| 役割別権限（管理者/閲覧者などのRBAC） | 誤操作を減らせる | “全員同権限で良い、運用でカバー”という合意。MVPの複雑度を上げない | 利用者数が増えて誤操作が問題化したら |
| BigQuery/Dataflow等のデータ基盤 | 大規模分析・拡張に強い | 個人開発＋少人数には過剰。コスト/運用が増える。まずはFirestoreに完成品保存で十分 | 利用が拡大し、履歴・分析が主目的になったら |
| GAS（Google Apps Script）を主スケジューラにする | 個人でも作れて便利 | クォータや不安定要素があり、運用中に止まる可能性。GitHub Actionsの方が一般に安定しやすい | GitHub Actionsのcronが不安定・制約に当たる場合の補助として |

### この付録の使い方（重要）
- 新しい提案が出たら、まず上の表に同じ案が無いか確認する  
- 「再検討条件」を満たしているなら復活させてよい  
- 満たしていないなら、**MVPの核（毎日・上位15件・単一画面・予算）を壊さない範囲**でのみ検討する
