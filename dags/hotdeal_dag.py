"""
핫딜 파이프라인 DAG
RSS(뽐뿌, 어미새) -> BigQuery raw 적재

변경 이력:
- v2: UA 헤더 추가(403 대응), 타임아웃 설정, 피드별 수집 결과 로깅 및 실패 감지
      루리웹 제외 (클라우드 IP 차단으로 서버에서 접근 불가)
"""

from __future__ import annotations

import pendulum

from airflow.sdk import dag, task

FEEDS = {
    "ppomppu": "https://www.ppomppu.co.kr/rss.php?id=ppomppu",
    "eomisae": "https://eomisae.co.kr/fs/rss",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

REQUEST_TIMEOUT = 15


@dag(
    dag_id="hotdeal_pipeline",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["hotdeal", "portfolio"],
)
def hotdeal_pipeline():
    @task
    def fetch_and_load():
        import logging
        import urllib.request
        from datetime import datetime, timezone

        import feedparser
        from google.cloud import bigquery

        log = logging.getLogger(__name__)
        table_id = "hotdeal-pipeline.raw.hotdeals_raw"

        rows = []
        feed_stats = {}
        failed_feeds = []

        for source, url in FEEDS.items():
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": USER_AGENT,
                        "Accept": "application/rss+xml,application/xml,text/xml,*/*",
                    },
                )
                raw = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT).read()
                feed = feedparser.parse(raw)

                count = len(feed.entries)
                feed_stats[source] = count

                if count == 0:
                    failed_feeds.append(f"{source}(0건 수집)")
                    log.warning("[%s] 수집 0건 - 피드 응답은 받았으나 항목 없음", source)
                    continue

                for entry in feed.entries:
                    rows.append({
                        "deal_id": entry.get("id", entry.link),
                        "title": entry.title,
                        "link": entry.link,
                        "source": source,
                        "published_at": entry.get("published", None),
                        "collected_at": datetime.now(timezone.utc).isoformat(),
                    })

                log.info("[%s] %d건 수집 성공", source, count)

            except Exception as e:
                feed_stats[source] = 0
                failed_feeds.append(f"{source}({type(e).__name__}: {e})")
                log.error("[%s] 수집 실패 - %s: %s", source, type(e).__name__, e)

        log.info("피드별 수집 결과: %s", feed_stats)

        if not rows:
            raise ValueError(f"전체 피드 수집 실패. 상세: {failed_feeds}")

        client = bigquery.Client()
        errors = client.insert_rows_json(table_id, rows)

        if errors:
            raise RuntimeError(f"BigQuery 적재 실패: {errors}")

        log.info("총 %d건 적재 완료 (피드별: %s)", len(rows), feed_stats)

        # 일부 피드만 실패한 경우: 적재는 이미 끝났으므로 데이터는 보존되지만,
        # 조용한 부분 실패를 막기 위해 예외를 던져 UI/알림에 명확히 드러냄
        if failed_feeds:
            raise RuntimeError(
                f"일부 피드 수집 실패 ({len(failed_feeds)}/{len(FEEDS)}): {failed_feeds} "
                f"| 적재는 완료됨: {len(rows)}건, 피드별: {feed_stats}"
            )

        return {"total": len(rows), "by_source": feed_stats, "failed": failed_feeds}

    fetch_and_load()


hotdeal_pipeline()