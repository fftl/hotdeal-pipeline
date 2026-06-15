import feedparser
from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import datetime, timezone

#GCP 서비스 연결 확인
load_dotenv()
client = bigquery.Client()
print("연결 성공! 프로젝트:", client.project)

table_id = "hotdeal-pipeline.raw.hotdeals_raw"

# 기존에 쓰던 실제 RSS URL로 교체
feeds = {
    "ppomppu": "https://www.ppomppu.co.kr/rss.php?id=ppomppu",
}

rows = []
for source, url in feeds.items():
    feed = feedparser.parse(url)
    print(f"[{source}] {len(feed.entries)}건 파싱")
    for entry in feed.entries:
        rows.append({
            "deal_id": entry.get("id", entry.link),
            "title": entry.title,
            "link": entry.link,
            "source": source,
            "published_at": entry.get("published", None),
            "collected_at": datetime.now(timezone.utc).isoformat(),
        })

if not rows:
    print("파싱된 데이터가 없습니다. RSS URL을 확인하세요.")
else:
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print("적재 실패:", errors)
    else:
        print(f"{len(rows)}건 적재 완료")