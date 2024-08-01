{{ config(materialized='table') }}

SELECT
  title
  ,  FORMAT_TIMESTAMP(
      '%Y-%m-%d',
      CASE
        WHEN REGEXP_CONTAINS(published, r'GMT$')
        THEN PARSE_TIMESTAMP('%a, %d %b %Y %H:%M:%S GMT', published)
        ELSE PARSE_TIMESTAMP('%a, %d %b %Y %H:%M:%S %z', published)
      END
  ) AS date
  , SPLIT(file_name, '/')[2] AS source
  , link
FROM {{ref('silver')}}
