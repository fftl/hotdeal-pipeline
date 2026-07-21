{#
    dbt 기본 동작은 대상 스키마명을 "profile_schema + custom_schema" 로 합쳐
    staging_staging, staging_mart 처럼 접미사가 붙는다.
    이 매크로는 그 동작을 덮어써서, 모델에 +schema 가 지정되면
    접미사 없이 그 값을 그대로 데이터셋 이름으로 사용한다.
    (+schema 미지정 시에는 기존처럼 profile 기본 스키마 사용)
#}
 
{% macro generate_schema_name(custom_schema_name, node) -%}
 
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
 
{%- endmacro %}