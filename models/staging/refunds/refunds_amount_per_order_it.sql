SELECT
  * EXCEPT(rn)
FROM (
  SELECT
    order_id As transaction_id,
    amount,
    created_at,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sdc_batched_at DESC) AS rn
  FROM
     {{ source('refunds_it', 'transactions') }}
  WHERE
    kind='refund'
    AND status='success'
    AND CONCAT('IT',id) NOT IN (SELECT CONCAT(source, id) FROM `leslunes-rep.bi.transactions_to_exclude`)) AS refunds
WHERE
  rn=1