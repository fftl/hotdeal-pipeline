-- raw.hotdeals_raw 를 정제한 staging 모델
-- 정제 내용:
--   1) deal_id: 링크에서 소스별 고유 숫자ID 추출 -> {source}_{id} 형태로 표준화
--   2) published_at: GMT / +0900 혼재된 문자열을 TIMESTAMP 로 파싱
--   3) 중복 제거: 동일 deal_id 는 가장 최근 수집분(collected_at 기준) 1건만 유지

with source_data as (

    select
        deal_id       as raw_deal_id,
        title,
        link,
        source,
        published_at  as raw_published_at,
        collected_at
    from {{ source('raw', 'hotdeals_raw') }}

),

cleaned as (

    select
        -- 소스별로 링크에서 숫자 ID 추출 후 표준 deal_id 생성
        case
            when source = 'ppomppu'
                then concat('ppomppu_', regexp_extract(link, r'no=(\d+)'))
            when source = 'eomisae'
                then concat('eomisae_', regexp_extract(link, r'/fs/(\d+)'))
            else concat(source, '_', raw_deal_id)
        end as deal_id,

        title,
        link,
        source,

        -- RFC822 형식 문자열(GMT / +0900 혼재)을 TIMESTAMP 로 파싱
        -- 예: 'Fri, 17 Jul 2026 17:00:55 GMT', 'Fri, 17 Jul 2026 11:54:14 +0900'
        coalesce(
            safe.parse_timestamp('%a, %d %b %Y %H:%M:%S %Z', raw_published_at),
            safe.parse_timestamp('%a, %d %b %Y %H:%M:%S %z', raw_published_at)
        ) as published_at,

        collected_at

    from source_data

),

final as (

    select
        deal_id,
        title,
        link,
        source,
        published_at,
        collected_at
    from cleaned
    -- deal_id 별 최신 수집분 1건만 유지 (BigQuery QUALIFY 로 윈도우 함수 결과 직접 필터)
    qualify row_number() over (
        partition by deal_id
        order by collected_at desc
    ) = 1

)

select * from final
