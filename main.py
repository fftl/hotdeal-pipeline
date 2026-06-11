import feedparser
from dotenv import load_dotenv
from google.cloud import bigquery

#GCP 서비스 연결 확인
load_dotenv()
client = bigquery.Client()
print("연결 성공! 프로젝트:", client.project)

# 뽐뿌 핫딜 RSS (기존에 쓰던 실제 URL로 교체)
url = "https://www.ppomppu.co.kr/rss.php?id=ppomppu"
feed = feedparser.parse(url)

print(f"가져온 항목 수: {len(feed.entries)}\n")

for entry in feed.entries[:5]:   # 처음 5개만 확인
    print("제목:", entry.title)
    print("링크:", entry.link)
    print("시각:", entry.get("published", "없음"))
    print("-" * 40)