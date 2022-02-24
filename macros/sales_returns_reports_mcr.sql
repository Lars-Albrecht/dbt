{%- macro sales_reports_line_items__sku(report) %}
    {%- if report=='sales_report' %}
        WHEN line_items__sku LIKE '%4007%' OR UPPER(line_items__sku) LIKE '%HWLEG%' THEN 'High Waisted Leggings'
        WHEN line_items__sku LIKE '%WPAUL125%' OR line_items__sku LIKE '%BA-PAUL%' THEN 'Paul'
        WHEN line_items__sku LIKE '%WSTELL125%' OR line_items__sku LIKE '%WSTELLA125%' OR line_items__sku LIKE '%STELLA%' THEN 'Stella'
        WHEN line_items__sku LIKE '%ALEN%' THEN 'Alena'
        WHEN line_items__sku LIKE '%STEV%' THEN 'Steve'
        WHEN line_items__sku LIKE '%MICHELLE-ECR%' OR line_items__sku LIKE '%ECR-MICHE%' THEN 'Michelle Rib'
        WHEN line_items__sku LIKE '%MICHELLE%' OR line_items__sku LIKE '%EC-MICHEL%' THEN 'Michelle' 
        WHEN line_items__sku LIKE '%OLIVIA%' THEN 'Olivia'
        WHEN line_items__sku LIKE '%CHARLOTTE%' OR line_items__sku LIKE '%CHARLT%' THEN 'Charlotte'
        WHEN line_items__sku LIKE '%ROBIN%' THEN 'Robin'
        WHEN line_items__sku LIKE '%GRACE%' THEN 'Grace'
        WHEN line_items__sku LIKE '%SOPHIA%' THEN 'Sophia'
        WHEN line_items__sku LIKE '%EMMA%' THEN 'Emma'
        WHEN line_items__sku LIKE '%LANA%' THEN 'Lana'
        WHEN line_items__sku LIKE '%AVA%' THEN 'Ava'
        WHEN line_items__sku LIKE '%MIA%' THEN 'Mia'
        WHEN line_items__sku LIKE '%JADE%' THEN 'Jade'
        WHEN line_items__sku LIKE '%MILEY%' THEN 'Miley Mask'
        WHEN line_items__sku LIKE '%RUBY%' THEN 'Ruby Scrunchie'
        WHEN line_items__sku LIKE '%VALERY%' THEN 'The Valery Scarf'
        WHEN line_items__sku LIKE '%LUNA%' THEN 'Luna Leggings'
        WHEN line_items__sku LIKE '%ABBY%' THEN 'Abby'
        WHEN line_items__sku LIKE '%CARA2% 'THEN 'Cara_2'
        WHEN line_items__sku LIKE '%CARA%' THEN 'Cara'
        WHEN line_items__sku LIKE '%LOU%' THEN 'Lou'
        WHEN line_items__sku LIKE '%BELLE%' THEN 'Belle'
        WHEN line_items__sku LIKE '%DAISY%' THEN 'Daisy'
        WHEN line_items__sku LIKE '%LILY%' THEN 'Lily'
        WHEN line_items__sku LIKE '%RUBY%' THEN 'Ruby Scrunchie'
        WHEN line_items__sku LIKE '%HALEY%' THEN 'Haley'
        WHEN line_items__sku LIKE '%HARPER%' THEN 'Harper'
        WHEN line_items__sku LIKE '%ROB%' THEN 'Rob'
        WHEN line_items__sku LIKE '%APRIL%' THEN 'April'
        WHEN line_items__sku LIKE '%IVY%' THEN 'Ivy'
        WHEN line_items__sku LIKE '%ZOE%' THEN 'Zoey'
        WHEN line_items__sku LIKE '%LEO%' THEN 'Leo'
        WHEN line_items__sku LIKE '%CHARLIE%' THEN 'Charlie'
        WHEN line_items__sku LIKE '%NOLA%' THEN 'Nola'
        WHEN line_items__sku LIKE '%ROSIE%' THEN 'Rosie'
        WHEN line_items__sku LIKE '%MARGOT%' THEN 'Margot'
        WHEN line_items__sku LIKE '%ELLA%' THEN 'Ella'
        WHEN line_items__sku LIKE '%LUCA%' THEN 'Luca'
        WHEN line_items__sku LIKE '%LAYLA%' THEN 'Layla'
        WHEN line_items__sku LIKE '%LAUREN%' THEN 'Lauren'
        WHEN line_items__sku LIKE '%LOLA%' THEN 'Lola'
        WHEN line_items__sku LIKE '%NELLY%' THEN 'Nelly'
        WHEN line_items__sku LIKE '%TONI%' THEN 'Toni'
        WHEN line_items__sku LIKE '%JOLIE%' THEN 'Jolie'
        WHEN line_items__sku LIKE '%JOSY%' THEN 'Josy'
        WHEN line_items__sku LIKE '%ESTEL%' THEN 'Estelle'
        WHEN line_items__sku LIKE '%JOY%' THEN 'Joy'
        WHEN line_items__sku LIKE '%JOSY%' THEN 'Josy'
        WHEN line_items__sku LIKE '%ALEXIS%' THEN 'Alexis'

    {%- endif -%}
    {%- if report=='returns_report' %}
    WHEN regexp_contains(product_sku, r'(set|SET)') THEN product_sku
    WHEN regexp_contains(product_sku, r'4007') THEN 'High waisted Leggings'
    WHEN regexp_contains(product_sku, r'07-BA-(LUNA|HWLEG)-(FG|VV|TM|MR)') THEN 'Luna Leggings' /*'Luna Leggings KN'*/
    WHEN regexp_contains(product_sku, r'07-BA-(LUNA|HWLEG)-(DT|CH|RT|HG|DN|BK|LL|EB|LG|WL)') THEN 'Luna Leggings'/*'Luna Leggings SA'*/
    WHEN regexp_contains(product_sku, r'(04-BA-STELLA|WSTELL125|WSTELLA125).*(XSP|XS\/S|S\/M|M\/L|L\/XL|M\/L)') THEN 'Stella Jumpsuit SA DS'
    WHEN regexp_contains(product_sku, r'04-BA-STELLA-(MR|VV|DQ|CH|TM|DN|FG|BK)-(XS|S|M|L|XL)$') THEN 'Stella Jumpsuit SA SS'
    WHEN regexp_contains(product_sku, r'STEV') THEN 'Steve Turtleneck SA'
    WHEN regexp_contains(product_sku, r'03-ALENA-(BK|WH|SGR|FG)') THEN 'Alena UTG DS'
    WHEN regexp_contains(product_sku, r'ALEN') AND NOT regexp_contains(product_sku, r'\/') THEN 'Alena SS'
    WHEN regexp_contains(product_sku, r'ALEN') THEN 'Alena SA DS'
    WHEN regexp_contains(product_sku, r'GRACE') THEN 'Grace Jumpsuit'
    WHEN regexp_contains(product_sku, r'LANA') THEN 'Lana Jumpsuit'
    WHEN regexp_contains(product_sku, r'SOPHIA') THEN 'Sophia Jumpsuit AB'
    WHEN regexp_contains(product_sku, r'MIA') THEN 'Mia Tshirt'
    WHEN regexp_contains(product_sku, r'(CHARLT|CHARLOTTE)') THEN 'Charlotte Bodysuit'
    WHEN regexp_contains(product_sku, r'EMMA') THEN 'Emma Bodysuit'
    WHEN regexp_contains(product_sku, r'(07|MICHELLE)-EC-(MICHEL-(BK|FD))?((XS|S|M|L|XL)-(FD|BK))?') THEN 'Michelle SJ'
    WHEN regexp_contains(product_sku, r'(07|MICHELLE)-ECR-(MICHEL-(BK|FD|RR|FG))?((XS|S|M|L|XL)-(FD|BK|RR|FG))?') THEN 'Michelle RIB'
    WHEN regexp_contains(product_sku, r'OLIVIA') THEN 'Olivia Hairband'
    WHEN regexp_contains(product_sku, r'ROBIN') THEN 'Robin Jumpsuit'
    WHEN regexp_contains(product_sku, r'PAULBELT') THEN 'Paul Belt'
    WHEN regexp_contains(product_sku, r'PAUL') THEN 'Paul Jumpsuit SA'
    WHEN regexp_contains(product_sku, r'AVA') THEN 'Ava Dress'
    WHEN regexp_contains(product_sku, r'VALERY') THEN 'Valery Scarf'
    WHEN regexp_contains(product_sku, r'MILEY') THEN 'Miley Mask'
    WHEN regexp_contains(product_sku, r'RUBY') THEN 'Ruby Scrunchie'
    WHEN regexp_contains(product_sku, r'JADE') THEN 'Jade Pants'
    WHEN regexp_contains(product_sku, r'ABBY') THEN 'Abby Longsleeve'
    WHEN regexp_contains(product_sku, r'CARA2') THEN 'Cara Jumpsuit DB'
    WHEN regexp_contains(product_sku, r'CARA') THEN 'Cara Jumpsuit AB'
    WHEN regexp_contains(product_sku, r'DAISY') THEN 'Daisy Tshirt'
    WHEN regexp_contains(product_sku, r'LILY') THEN 'Lily Top' 
    WHEN regexp_contains(product_sku, r'LOU') THEN 'Lou Top'
    WHEN regexp_contains(product_sku, r'BELLE') THEN 'Belle Pants'
    WHEN regexp_contains(product_sku, r'HARPER') THEN 'Harper Socks'
    WHEN regexp_contains(product_sku, r'HALEY') THEN 'Hayley Bag' 
    WHEN regexp_contains(product_sku, r'APRIL') THEN 'April'
    WHEN regexp_contains(product_sku, r'02-BA-ROB') THEN 'Rob'
    WHEN regexp_contains(product_sku, r'IVY') THEN 'Ivy'
    WHEN regexp_contains(product_sku, r'ZOE') THEN 'Zoey'
    WHEN regexp_contains(product_sku, r'LEO') THEN 'Leo'
    WHEN regexp_contains(product_sku, r'CHARLIE') THEN 'Charlie'
    WHEN regexp_contains(product_sku, r'NOLA') THEN 'Nola Sweater'
    WHEN regexp_contains(product_sku, r'ROSIE') THEN 'Rosie Skirt'
    WHEN regexp_contains(product_sku, r'MARGOT') THEN 'Margot'
    WHEN regexp_contains(product_sku, r'ELLA') THEN 'Ella'
    WHEN regexp_contains(product_sku, r'LUCA') THEN 'Luca'
    WHEN regexp_contains(product_sku, r'LAYLA') THEN 'Layla'
    WHEN regexp_contains(product_sku, r'LAUREN') THEN 'Lauren'
    WHEN regexp_contains(product_sku, r'LOLA') THEN 'Lola'
    WHEN regexp_contains(product_sku, r'NELLY') THEN 'Nelly'
    WHEN regexp_contains(product_sku, r'TONI') THEN 'Toni'
    WHEN regexp_contains(product_sku, r'JOLIE') THEN 'Jolie'
    WHEN regexp_contains(product_sku, r'JOSY') THEN 'Josy'
    WHEN regexp_contains(product_sku, r'ESTEL') THEN 'Estelle'
    WHEN regexp_contains(product_sku, r'JOY') THEN 'Joy'
    WHEN regexp_contains(product_sku, r'JOSY') THEN 'Josy'
    WHEN regexp_contains(product_sku, r'ALEXIS') THEN 'Alexis'
    {%- endif %}
{%- endmacro %}