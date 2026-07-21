-- staging.stg_hotdeals 를 소비하는 mart 모델
-- 딜 1건 = 1행 을 유지하되, Looker 대시보드용 분석 컬럼(날짜/시/요일)을 추출.
-- materialized='table' : Looker 가 반복 조회하므로 뷰 대신 테이블로 구움.
 
{{ config(materialized='table') }}
 
with staged as (
 
    select * from {{ ref('stg_hotdeals') }}
 
),
 
final as (
 
    select
        deal_id,
        title,
        link,
        source,
        published_at,
        collected_at,
 
        -- 서울 시간대 기준 분석 컬럼 (published_at 은 UTC 기준 TIMESTAMP)
        date(published_at, 'Asia/Seoul')                     as deal_date,
        extract(hour from datetime(published_at, 'Asia/Seoul'))   as deal_hour,
        format_datetime('%A', datetime(published_at, 'Asia/Seoul')) as deal_weekday
 
    from staged
 
)
 
select * from final