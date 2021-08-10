/*
TODO: 
Which status actually leads to costs?
confirmed, confirmed_plus,not_responsive,waiting_to_confirm,
package_not_received,no,planned,posted,benched

*/

SELECT
  source,
  started_at,
  ROUND(SUM(cost),2) AS influencer_marketing_spent,
FROM
  `leslunes-rep.marketing.deals_data_live`
WHERE
  status='posted'
GROUP BY
  started_at,
  source
ORDER BY 
  started_at DESC