from __future__ import annotations
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="hotdeal_pipeline",
    schedule="0 * * * *",  # 매시 정각 (1시간마다)
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["hotdeal", "portfolio"],
)

def hotdeal_pipeline():
    @task
    def fetch_and_load():
        import os
        from datetime import datetime, timezone

        import feedparser
        from google.cloud import bigquery

        table_id = "hotdeal-pipeline.raw.hotdeals_raw"

        feeds = {
            "ppomppu": "https://www.ppomppu.co.kr/rss.php?id=ppomppu",
            "ruliweb": "https://bbs.ruliweb.com/market/board/1020/rss",
            "eomisae": "https://eomisae.co.kr/fs/rss",
        }

        rows = []
        for source, url in feeds.items():
            feed = feedparser.parse(url)
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
            raise ValueError("수집된 항목이 0건입니다. RSS 응답을 확인하세요.")

        client = bigquery.Client()
        errors = client.insert_rows_json(table_id, rows)

        if errors:
            raise RuntimeError(f"BigQuery 적재 실패: {errors}")

        print(f"{len(rows)}건 적재 완료")
        return len(rows)

    fetch_and_load()


hotdeal_pipeline()