import pandas as pd
import snowflake.connector
import streamlit as st

class SnowLoader:

    # Time to live: the maximum number of seconds to keep an entry in the cache
    TTL = 24 * 60 * 60

    def __init__(self):
        self.conn = self.init_connection()

    #@st.experimental_singleton
    def init_connection(self):
        return snowflake.connector.connect(
        user= st.secrets["snowflake"]["user"],
        password= st.secrets["snowflake"]["password"],
        account= st.secrets["snowflake"]["account"],
        role = st.secrets["snowflake"]["role"],
        client_session_keep_alive=True)

    
    def run(self, q):
        try:
            cursor = self.conn.cursor()
            cursor.execute(q)
            all_rows = cursor.fetchall()
            field_names = [i[0] for i in cursor.description]
        finally:
            cursor.close()
        df = pd.DataFrame(all_rows)
        df.columns = field_names
  
        return df


class Query:

    email_to_handle_q = """
                        select
                        email, 
                        insta_handle as INFLUENCER_HANDLE
                        from FIVETRAN_TEST_DATABASE.aspire.email_handle
                        """


    influencer_costs_q = """
                        with cost as (
                            select
                            ord.shop_order_processed_at,
                            ord.shop_order_fulfilled_at,
                            cust.EMAIL,
                            cust.FIRST_NAME,
                            cust.LAST_NAME,
                            merch.OS_MERCHANT,
                            ord.os_store_id,
                            ord.shop_order_id,
                            lines.SHOP_PRODUCT_ID,
                            order_line_price,
                            order_line_quantity,
                            lines.OS_BACKDATED_PRODUCT_COST/order_line_quantity as os_backdated_product_cost,
                            ((order_line_price*order_line_quantity)/ord.OS_GROSS_SALES)*ord.os_amt_discounted as os_amt_discounted,
                            (((order_line_price*order_line_quantity)/ord.OS_GROSS_SALES)/order_line_quantity)*ord.OS_PAYMENT_PROCESSING_COGS as order_line_os_payment_processing_cogs,
                            (((order_line_price*order_line_quantity)/ord.OS_GROSS_SALES)/order_line_quantity)*ord.OS_FULFILLMENT_COGS as order_line_os_fulfillment_cogs,
                            (((order_line_price*order_line_quantity)/ord.OS_GROSS_SALES)/order_line_quantity)*ord.os_shipping_cogs as order_line_os_shipping_cogs,
                            (((order_line_price*order_line_quantity)/ord.OS_GROSS_SALES)/order_line_quantity)*ord.os_inventory_receipt_cogs as order_line_os_inventory_cogs 

                            from ANALYTICS.MAIN.OS_ALL_ORDERS ord
                            left join DBT_ANALYTICS.PROD.DEF__OS_SHOPIFY_ORDER_LINES lines
                            on ord.OS_ORDER_ID = lines.OS_ORDER_ID
                            left join ANALYTICS.MAIN.OS_MERCHANTS merch
                            on ord.OS_STORE_ID = merch.OS_STORE_ID
                            left join DBT_ANALYTICS.PROD.DEF__OS_SHOPIFY_CUSTOMER_PASS cust
                            on ord.OS_STORE_ID = cust.OS_STORE_ID 
                            and ord.SHOP_CUSTOMER_ID = cust.SHOP_CUSTOMER_ID

                            where SHOP_APP_ID = '4137415' -- evolve these costs for discount codes 
                            and is_closed = True
                            and SHOP_ORDER_FULFILLMENT_STATUS = 'fulfilled'

                            qualify
                            row_number() over (partition by ord.shop_order_id, shop_product_id order by ord.os_dt desc) =1
                            )
                            select
                            *,
                            os_backdated_product_cost + order_line_os_payment_processing_cogs + order_line_os_fulfillment_cogs + order_line_os_shipping_cogs + order_line_os_inventory_cogs as os_product_cogs
                            from cost
                            order by 1 desc nulls last
    
                            """


    merchant_stats_q = """
                        with sales as (
                            SELECT
                            os_dt,
                            os_merchant,
                            sum(case when o.os_cust_trxn_status = 'New' and  os_order_status in ('sale') then nvl(o.os_net_sales, 0) else 0 end) as os_new_customer_original_net_sales,
                            sum(case when o.os_cust_trxn_status = 'Returning' and  os_order_status in ('sale') then nvl(o.os_net_sales, 0) else 0 end) as os_recurring_customer_original_net_sales,
                            count(distinct iff(o.os_cust_trxn_status = 'New'  and os_order_status in ('sale'), o.os_order_id, null)) as new_customer_orders,
                            count(distinct iff(o.os_cust_trxn_status = 'Returning'  and os_order_status in ('sale'), o.os_order_id, null)) as recurring_customers_orders,
                            DIV0(os_new_customer_original_net_sales, new_customer_orders) as new_aov,
                            DIV0(os_recurring_customer_original_net_sales, recurring_customers_orders) as recurring_aov
                            from ANALYTICS.MAIN.OS_SHOPIFY_ORDERS o
                            left join ANALYTICS.MAIN.OS_MERCHANTS merch
                            on o.OS_STORE_ID = merch.OS_STORE_ID
                            where os_dt > date_trunc("DAY",dateadd(day, -180, current_date())) and dt_closed is not null
                            group by 1, 2
                            )
                            , ad_stats as (
                            select
                            os_dt,
                            os_merchant,
                            DIV0(sum(TOTAL_AD_SPEND),sum(SHOPIFY_NEW_CUSTOMERS)) as cac,
                            avg(FINANCE_GROSS_MARGIN) as gross_margin,
                            avg(finance_adjusted_historical_return_rate) as fin_return_rate
                            from ANALYTICS.MAIN.OS_AGG_DAILY_AD_STATS stats
                            left join ANALYTICS.MAIN.OS_MERCHANTS merch
                            on stats.OS_STORE_ID = merch.OS_STORE_ID
                            where dt_closed is not null
                            and os_dt > date_trunc("DAY",dateadd(day, -180, current_date()))
                            group by 1, 2
                            )
                            select
                            ad_stats.os_dt,
                            ad_stats.os_merchant,
                            new_aov*(1-fin_return_rate) as new_aov,
                            --recurring_aov*(1-fin_return_rate) as recurring_aov_,
                            cac as blended_cac
                            -- gross_margin
                            from ad_stats
                            left join sales
                            on ad_stats.os_merchant = sales.os_merchant
                            and ad_stats.os_dt = sales.os_dt

                         """


    pp_direct_attrib_q = """
                        with reattr as (
                        select os_store_id, os_dt, OS_SESSION_ID, PREFERRED_CHANNEL, platform, enquire_survey_question, enquire_survey_answer,
                            case when enquire_survey_answer ilike any('%influencer%','%testimonials%') then 'Influencer' else PREFERRED_CHANNEL end new_channel
                        from dbt_analytics._dev_dbt_main.DEF__GA4_EVENTS__EVENT_STREAM
                        where enquire_survey_question is not null
                        and enquire_survey_question ilike '%how did you hear%'
                        ), influ as  (
                        select merch.os_merchant, r.*, a.SHOPIFY_ORDER_ID, os_net_sales,
                        OS_GROSS_SALES
                        from dbt_analytics._dev_dbt_main_main.GA4_EVENTS_STREAM a
                        join reattr r
                        on a.OS_STORE_ID = r.OS_STORE_ID
                        and a.OS_SESSION_ID = r.OS_SESSION_ID
                        left join ANALYTICS.MAIN.OS_MERCHANTS merch
                        on a.OS_STORE_ID = merch.os_store_id
                        left join ANALYTICS.MAIN.OS_SHOPIFY_ORDERS ord
                        on a.OS_STORE_ID = ord.OS_STORE_ID
                        and a.SHOPIFY_ORDER_ID = ord.SHOP_ORDER_ID
                        where a.SHOPIFY_ORDER_ID is not null
                        and new_channel = 'Influencer'
                        and SHOP_APP_ID <> '4137415' --strip out Aspire attributed sales
                        )
                        select
                        *
                        from influ
                        """



    ##### BRAND_TO_BRAND X Shopping Orders #####
    cust_orders_x_q = '''
                    SELECT cust.email, merch.os_merchant, COUNT(DISTINCT orders.os_order_id) as num_orders
                    FROM ANALYTICS.MAIN.OS_SHOPIFY_ORDERS orders
                    INNER JOIN ANALYTICS.MAIN.OS_MERCHANTS merch
                        on merch.os_store_id = orders.os_store_id
                    INNER JOIN ANALYTICS.MAIN.OS_SHOPIFY_CUSTOMER cust
                        on cust.os_customer_id = orders.os_customer_id
                    WHERE 
                        cust.email IS NOT NULL 
                        AND merch.dt_closed IS NOT NULL
                        AND orders.shop_order_source_name <> 'shopify_draft_order'
                        and not cust.email ilike any('%@getelevar.com%', '%@open.store%','%andrewjcampbell1@gmail.com%','%joncairo@gmail.com%','%@affiliatemanager.com%')
                    GROUP BY cust.email, merch.os_merchant


                    '''

    products_q = """
                with
                inventory_level as (
                select
                        a.source_schema
                    , b.sku
                    , a.location_id
                    , a.inventory_item_id
                    , a.available
                from
                    os_merchants_prod_database.shopify.inventory_level           a
                    inner join os_merchants_prod_database.shopify.inventory_item b
                    on a.inventory_item_id = b.id
                    and a.source_schema = b.source_schema
                    qualify
                        row_number() over (
                        partition by sku,location_id, a.inventory_item_id order by a._fivetran_synced desc
                        ) = 1
                ),

                inventory as (
                select ii.id as inventory_item_id, available
                from os_merchants_prod_database.shopify.inventory_item ii
                    inner join inventory_level                                   il
                    on il.inventory_item_id = ii.id
                    and ii.source_schema = il.source_schema
                    inner join os_merchants_prod_database.shopify.location       l
                    on il.location_id = l.id
                    and il.source_schema = l.source_schema
                    and (not l._fivetran_deleted or l._fivetran_deleted is null)
                    and l.active = true),

                invs as (select conn.os_store_id, p.source_schema, pv.sku,  pv.id as pvid, avg(pv.price) as price, p.handle as product_handle, p.title product_title, pv.title variant_title,  sum(il.available) as inventory_available, inventory_policy, tracked, pv.product_id
                from os_merchants_prod_database.shopify.product_variant          pv
                    inner join os_merchants_prod_database.shopify.product        p
                    on p.id = pv.product_id
                    and p.source_schema = pv.source_schema
                    and p.title not ilike '%gift card%'
                    and (not p._fivetran_deleted or p._fivetran_deleted is null)
                    and p.status = 'active'
                    left join inventory il
                    on il.inventory_item_id = pv.inventory_item_id
                    left join os_merchants_prod_database.shopify.inventory_item ii
                    on ii.id = pv.inventory_item_id
                    left join  dbt_analytics.prod.def__os_merchant_fivetran_connectors__metrics conn
                    on lower(p.source_schema) = lower(conn.os_source_schema)
                
                group by conn.os_store_id, p.source_schema, pv.sku,  pv.id, pv.product_id, p.handle, p.title, pv.title, inventory_policy, tracked),
                images as (
                select
                ass.*,
                prod.shopify_external_id as shop_product_id
                from catalog_service_prod.aurora_postgres_public.asset ass
                inner join catalog_service_prod.aurora_postgres_public.product_asset jt
                on ass.id = jt.asset_id
                inner join catalog_service_prod.aurora_postgres_public.product prod
                on jt.product_id = prod.id
                ),
                available_inventory as (
                select invs.*,  img.url as imageurl
                from invs
                inner join images img
                on invs.product_id = img.shop_product_id
                where ((inventory_available > 0 and tracked=True and inventory_policy = 'deny') or (tracked = False
                or inventory_policy = 'continue')) and (imageurl is not null and img.position = 0)
                ), sales as (-- get best selling product ids within last 6 months
                select
                os_merchant,
                line.OS_STORE_ID,
                line.SHOP_PRODUCT_ID,
                products.PRODUCT_PUBLISHED_AT,
                sum(case when line.os_dt > current_date - 2 then OS_ORDER_LINE_GROSS_SALES - OS_AMT_DISCOUNTED - OS_AMT_REFUNDED - OS_TAX_AMT_REFUNDED else 0 end) as last_2day_sales,
                sum(case when line.os_dt > current_date - 30 then OS_ORDER_LINE_GROSS_SALES - OS_AMT_DISCOUNTED - OS_AMT_REFUNDED - OS_TAX_AMT_REFUNDED  else 0 end) as last_1mo_sales,
                sum(case when line.os_dt > current_date - 90 then  OS_ORDER_LINE_GROSS_SALES - OS_AMT_DISCOUNTED - OS_AMT_REFUNDED - OS_TAX_AMT_REFUNDED  else 0 end) as last_3mo_sales,
                sum(case when line.os_dt > current_date - 180 then  OS_ORDER_LINE_GROSS_SALES - OS_AMT_DISCOUNTED - OS_AMT_REFUNDED - OS_TAX_AMT_REFUNDED  else 0 end) as last_6mo_sales,
                sum(case when line.os_dt > current_date - 360 then  OS_ORDER_LINE_GROSS_SALES - OS_AMT_DISCOUNTED - OS_AMT_REFUNDED - OS_TAX_AMT_REFUNDED  else 0 end) as last_1yr_sales,
                sum( OS_ORDER_LINE_GROSS_SALES - OS_AMT_DISCOUNTED - OS_AMT_REFUNDED - OS_TAX_AMT_REFUNDED ) as lifetime_sales
                from DBT_ANALYTICS.PROD.DEF__OS_SHOPIFY_ORDER_LINES line
                left join DBT_ANALYTICS.PROD.DEF__OS_SHOPIFY_PRODUCTS products
                on line.OS_STORE_ID = products.os_store_id
                and line.SHOP_PRODUCT_ID = products.SHOP_PRODUCT_ID
                left join DBT_ANALYTICS.PROD.DEF__OS_SHOPIFY_PRODUCT_VARIANTS var
                on line.OS_STORE_ID = var.os_store_id
                and line.SHOP_PRODUCT_ID = var.SHOP_PRODUCT_ID
                left join ANALYTICS.MAIN.OS_MERCHANTS merch
                on line.OS_STORE_ID = merch.OS_STORE_ID

                where products.product_title is not null
                and products.PRODUCT_STATUS = 'active'
                and is_closed = True
                group by 1,2,3,4
                ), product_costs as (
                select
                ord.os_store_id,
                lines.SHOP_PRODUCT_ID,
                lines.OS_BACKDATED_PRODUCT_COST/order_line_quantity as os_backdated_product_cost,
                (((order_line_price*order_line_quantity)/ord.OS_GROSS_SALES)/order_line_quantity)*ord.OS_PAYMENT_PROCESSING_COGS as order_line_os_payment_processing_cogs,
                (((order_line_price*order_line_quantity)/ord.OS_GROSS_SALES)/order_line_quantity)*ord.OS_FULFILLMENT_COGS as order_line_os_fulfillment_cogs,
                (((order_line_price*order_line_quantity)/ord.OS_GROSS_SALES)/order_line_quantity)*ord.os_shipping_cogs as order_line_os_shipping_cogs,
                (((order_line_price*order_line_quantity)/ord.OS_GROSS_SALES)/order_line_quantity)*ord.os_inventory_receipt_cogs as order_line_os_inventory_cogs 

                from ANALYTICS.MAIN.os_agg_shopify_orders_contribution ord
                left join dbt_analytics._dev_dbt_vishnu.def__os_shopify_order_lines_zack_qa lines
                on ord.OS_ORDER_ID = lines.OS_ORDER_ID
                where os_gross_sales > 0
                and os_backdated_product_cost is not null

                qualify
                row_number() over (partition by ord.os_store_id, lines.shop_product_id order by ord.os_dt desc) =1
                ), 
                full_cogs as (
                select
                *,
                os_backdated_product_cost + order_line_os_payment_processing_cogs + order_line_os_fulfillment_cogs + order_line_os_shipping_cogs + order_line_os_inventory_cogs as os_product_cogs
                from product_costs c
                ),
                ranked as (
                select
                prod.*,
                case when product_published_at >= current_date - 30 then 'New Releases' else null end as new_release,
                rank() over (partition by prod.os_store_id order by last_3mo_sales desc nulls last) as last_3mo_sales_rank,
                rank() over (partition by prod.os_store_id order by last_2day_sales desc nulls last) as last_2day_sales_rank,
                rank() over (partition by prod.os_store_id order by lifetime_sales desc nulls last) as lifetime_sales_rank,
                percentile_cont(0.75) WITHIN GROUP (order by lifetime_sales asc nulls last) over (partition by prod.os_store_id) as lifetime_sales_75_percentile
                from sales prod
                ), product_cogs_inv as (
                select
                ranked.*,
                price,
                product_handle, 
                product_title, 
                inventory_available, 
                inventory_policy, 
                tracked, 
                os_product_cogs,
                imageurl,
                1-DIV0(os_product_cogs, price) as product_gross_margin
                from ranked
                inner join full_cogs 
                on ranked.OS_STORE_ID = full_cogs.os_store_id 
                and ranked.SHOP_PRODUCT_ID = full_cogs.shop_product_id
                left join available_inventory inv
                on ranked.os_store_id = inv.os_store_id and ranked.shop_product_id = inv.product_id
                    
                qualify
                    row_number() over (
                    partition by             
                        full_cogs.os_store_id, inv.product_title
                            
                    order by 
                        product_published_at desc
                    ) = 1
                ), product_catalog as (
                select * from product_cogs_inv
                where ((PRODUCT_PUBLISHED_AT > current_date - 30) or (lifetime_sales_rank < 10) or (lifetime_sales > lifetime_sales_75_percentile)) 
                and product_title is not null and not product_title ilike any('%package protection%','%EXO Care%','%Accessories%')
                ), 
                product_gender as (
                select
                cust.os_store_id,
                shop_product_id,
                count(case when CE_SELECTED_GENDER = 'M' then 1 else null end) as males,
                count(case when CE_SELECTED_GENDER = 'F' then 1 else null end) as females,
                DIV0(females, males) as female_to_male_ratio,
                case when female_to_male_ratio >= 2 then 'F'
                    when female_to_male_ratio > 0 and female_to_male_ratio <= 0.5 then 'M'
                    else 'U' end as gender_preference
                from fivetran_test_database.archive.customer_db cust
                left join dbt_analytics.prod.def__os_shopify_customer_pass pass
                on cust.os_customer_id = pass.os_customer_id
                left join analytics.main.os_all_orders ord
                on pass.shop_customer_id = ord.shop_customer_id
                and pass.os_store_id = ord.os_store_id
                left join dbt_analytics.prod.def__os_shopify_order_lines line
                on ord.os_store_id = line.os_store_id
                and ord.os_order_id = line.os_order_id
                where shop_product_id is not null
                group by 1,2
                ), tagged as (
                select
                a.*,
                female_to_male_ratio,
                case when product_title ilike '%men%' then 'M' 
                    when product_title ilike any('%women%','%bra%','%legging%','%tutu%') then 'F'
                    else gender_preference end as gender_preference
                from product_catalog a
                left join product_gender b
                on a.os_store_id = b.os_store_id 
                and a.shop_product_id = b.shop_product_id

                where product_gross_margin is not null and product_gross_margin > 0
                ),
                semi as (
                select
                tagged.*,
                qa.gender_affinity
                from tagged
                left join fivetran_test_database.archive.gumdrop_products_qa qa
                on tagged.shop_product_id = qa.shop_product_id
                where exclude_gumdrop is null and secondary_product_ is null
                )
                select
                semi.*,
                prods.BODY_HTML as description,
                case when gender_affinity is not null then gender_affinity else gender_preference end as gender_cleaned
                from semi
                left join DBT_ANALYTICS.PROD.DEF__OS_SHOPIFY_PRODUCTS prods
                on semi.OS_STORE_ID = prods.OS_STORE_ID and semi.SHOP_PRODUCT_ID = prods.SHOP_PRODUCT_ID
                """



    def get_top_down(merchant, s):
        q = f"""
        with ga_correct as (
        select *,
        case when landing_page like '%api%' and landing_page ilike '%graphql%' then 'API / App Backend'
        else preferred_channel end as preferred_channel_correct
        from analytics.main.DAILY_MERCHANT_TRANSACTIONS_GA_FULL_ATTRIBUTION
        ), reattr as (
        select os_store_id, os_dt, OS_SESSION_ID, PREFERRED_CHANNEL, platform, enquire_survey_question, enquire_survey_answer,
            case when enquire_survey_answer ilike any('%influencer%','%testimonials%') then 'Influencer' else PREFERRED_CHANNEL end new_channel
        from dbt_analytics._dev_dbt_main.DEF__GA4_EVENTS__EVENT_STREAM
        where enquire_survey_question is not null
        and enquire_survey_question ilike '%how did you hear%'
        ),
        influ as  (
        select merch.os_merchant, r.*, ord.SHOP_ORDER_ID, os_net_sales,
        OS_GROSS_SALES
        from dbt_analytics._dev_dbt_main_main.GA4_EVENTS_STREAM a
        join reattr r
        on a.OS_STORE_ID = r.OS_STORE_ID
        and a.OS_SESSION_ID = r.OS_SESSION_ID
        left join ANALYTICS.MAIN.OS_MERCHANTS merch
        on a.OS_STORE_ID = merch.os_store_id
        left join ANALYTICS.MAIN.OS_SHOPIFY_ORDERS ord
        on a.OS_STORE_ID = ord.OS_STORE_ID
        and a.SHOPIFY_ORDER_ID = split_part(ord.SHOP_ORDER_ID, '-', 1)::bigint
        where a.SHOPIFY_ORDER_ID is not null
        and new_channel = 'Influencer'
        and SHOP_APP_ID <> '4137415' --strip out Aspire attributed sales
        ),
        unattrib as (
        SELECT
        orders.os_store_id,
        merch.OS_MERCHANT,
        orders.os_dt,
        shop_ORDER_ID,
        os_net_sales,
        os_gross_sales,
        os_merchant_revenue,
        preferred_channel_correct,
        platform,
        case when 
        (preferred_channel_correct is NULL) 
        or 
        (preferred_channel_correct = 'Affiliate' and PLATFORM = 'Unknown Affiliate Platform')
        OR
        (preferred_channel_correct in ('Organic Search', 'Direct'))
        OR
        (preferred_channel_correct = 'Other' and platform = 'Unknown - Other')
        OR
        (preferred_channel_correct = 'Owned* Social' and (PLATFORM in ('Unknown Social Platform','TikTok') or (PLATFORM = 'Meta' and SUB_PLATFORM = 'Instagram')))
        OR
        (preferred_channel_correct = 'Paid* Social' and (PLATFORM in ('Unknown Social Platform','TikTok') or (PLATFORM = 'Meta' and SUB_PLATFORM in ('Instagram','Stories','Unknown Meta Subplatform'))))
        OR
        (preferred_channel_correct = 'Paid Search' and (SUB_PLATFORM in ('Branded Search','Unknown Paid Search Subplatform')))
        OR
        (preferred_channel_correct= 'Referral' and platform in ('TikTok','Meta','Unknown Referral Platform'))
        then True else False end as unattributed
        from ANALYTICS.MAIN.OS_SHOPIFY_ORDERS orders
        left join ANALYTICS.MAIN.OS_MERCHANTS merch
        on orders.OS_STORE_ID = merch.OS_STORE_ID
        left join ga_correct attrib
        on attrib.os_store_id = orders.OS_STORE_ID AND
        attrib.GA_ORDER_NUMBER = orders.ORDER_NUMBER
        where merch.os_merchant = '{merchant}' and UNATTRIBUTED = True
        and orders.os_dt > current_date - 180
        ),
        final as (
        select
        unattrib.os_dt,
        unattrib.os_net_sales,
        unattrib.os_gross_sales,
        os_merchant_revenue
        from unattrib
        left join influ
        on unattrib.OS_STORE_ID = influ.OS_STORE_ID
        and unattrib.shop_ORDER_ID::text = split_part(influ.SHOP_ORDER_ID, '-', 1)::bigint
        where influ.SHOP_ORDER_ID is null
        )
        SELECT
        OS_DT,
        sum(os_merchant_revenue) as unattributed_rev
        from final
        group by 1
        order by 1
        """
        return s.run(q)


    def get_bottom_up(merchant,s):
        q = f"""
        with ga_correct as (
        select *,
        case when landing_page like '%api%' and landing_page ilike '%graphql%' then 'API / App Backend'
        else preferred_channel end as preferred_channel_correct
        from analytics.main.DAILY_MERCHANT_TRANSACTIONS_GA_FULL_ATTRIBUTION
        ),
        reattr as (
        select os_store_id, os_dt, OS_SESSION_ID, PREFERRED_CHANNEL, platform, enquire_survey_question, enquire_survey_answer,
            case when enquire_survey_answer ilike any('%influencer%','%testimonials%') then 'Influencer' else PREFERRED_CHANNEL end new_channel
        from dbt_analytics._dev_dbt_main.DEF__GA4_EVENTS__EVENT_STREAM
        where enquire_survey_question is not null
        and enquire_survey_question ilike '%how did you hear%'
        ),
        influ as  (
        select merch.os_merchant, r.*, ord.SHOP_ORDER_ID, os_net_sales,
        OS_GROSS_SALES
        from dbt_analytics._dev_dbt_main_main.GA4_EVENTS_STREAM a
        join reattr r
        on a.OS_STORE_ID = r.OS_STORE_ID
        and a.OS_SESSION_ID = r.OS_SESSION_ID
        left join ANALYTICS.MAIN.OS_MERCHANTS merch
        on a.OS_STORE_ID = merch.os_store_id
        left join ANALYTICS.MAIN.OS_SHOPIFY_ORDERS ord
        on a.OS_STORE_ID = ord.OS_STORE_ID
        and a.SHOPIFY_ORDER_ID = split_part(ord.SHOP_ORDER_ID, '-', 1)::bigint
        where a.SHOPIFY_ORDER_ID is not null
        and new_channel = 'Influencer'
        and SHOP_APP_ID <> '4137415'
        ), unattrib as (
        SELECT
        orders.os_store_id,
        merch.OS_MERCHANT,
        orders.os_dt,
        shop_ORDER_ID,
        os_net_sales,
        OS_GROSS_SALES,
        os_merchant_revenue,
        (preferred_channel_correct ||'-'|| ifnull(platform,'') ||'-'|| ifnull(sub_platform,'')) as plat,
        case when 
        (preferred_channel_correct = 'Affiliate' and PLATFORM = 'Unknown Affiliate Platform')
        OR
        (preferred_channel_correct in ('Organic Search', 'Direct'))
        OR
        (preferred_channel_correct = 'Other' and platform = 'Unknown - Other')
        OR
        (preferred_channel_correct = 'Owned* Social' and (PLATFORM in ('Unknown Social Platform','TikTok') or (PLATFORM = 'Meta' and SUB_PLATFORM = 'Instagram')))
        OR
        (preferred_channel_correct = 'Paid* Social' and (PLATFORM in ('Unknown Social Platform','TikTok') or (PLATFORM = 'Meta' and SUB_PLATFORM in ('Instagram','Stories','Unknown Meta Subplatform'))))
        OR
        (preferred_channel_correct = 'Paid Search' and (SUB_PLATFORM in ('Branded Search','Unknown Paid Search Subplatform')))
        OR
        (preferred_channel_correct = 'Referral' and platform in ('TikTok','Meta','Unknown Referral Platform'))
        then True else False end as unattributed
        from ANALYTICS.MAIN.OS_SHOPIFY_ORDERS orders
        left join ANALYTICS.MAIN.OS_MERCHANTS merch
        on orders.OS_STORE_ID = merch.OS_STORE_ID
        left join ga_correct attrib
        on attrib.os_store_id = orders.OS_STORE_ID AND
        attrib.GA_ORDER_NUMBER = orders.ORDER_NUMBER
        where merch.os_merchant = '{merchant}' and unattributed = True
        having plat is not null
        ),
        final as (
        select
        unattrib.*
        from unattrib
        left join influ
        on unattrib.OS_STORE_ID = influ.OS_STORE_ID
        and unattrib.shop_ORDER_ID::text = split_part(influ.SHOP_ORDER_ID, '-', 1)::bigint
        where influ.SHOP_ORDER_ID is null
        )
        select
        *
        from final
        """
        return s.run(q)

    
    def cross_orders(s):
        q = '''
            SELECT cust.email, merch.os_merchant, COUNT(DISTINCT orders.os_order_id) as num_orders
            FROM ANALYTICS.MAIN.OS_SHOPIFY_ORDERS orders
            INNER JOIN ANALYTICS.MAIN.OS_MERCHANTS merch
                on merch.os_store_id = orders.os_store_id
            INNER JOIN ANALYTICS.MAIN.OS_SHOPIFY_CUSTOMER cust
                on cust.os_customer_id = orders.os_customer_id
            WHERE 
                cust.email IS NOT NULL 
                AND merch.dt_closed IS NOT NULL
                AND orders.shop_order_source_name <> 'shopify_draft_order'
            GROUP BY cust.email, merch.os_merchant;
            '''
        return s.run(q)
