WITH code_channels AS(
    SELECT
        code AS cc_code,
        code_type AS code_channel,
    FROM
        {{ ref('stg_coupon_types') }}
)

SELECT 
    * EXCEPT(cc_code)
FROM {{ ref('stg_orderitems_combined') }} OIC
LEFT JOIN code_channels cc ON cc.cc_code = OIC.code
