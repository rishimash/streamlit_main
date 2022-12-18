import pandas as pd
import snowflake.connector
import streamlit as st

class SnowLoader:

    # Time to live: the maximum number of seconds to keep an entry in the cache
    TTL = 60 * 60

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


    def get_drop(s, influ, release):
        q = f'''
         with prod as (
            select
            tagged.*,
            tagged.min_price as price,
            case when tagged.product_title ilike '%men%' then 'M' 
                when tagged.os_merchant ilike any ('%Buy Secure Mat%', '%Wearva%','%STEM%','%SOL Organics%','%Canvas Cultures%','%Collection Lounge%','%Farm%') then 'U'
                when tagged.product_title ilike any('%women%','%bra%','%legging%','%tutu%') then 'F'
                when avg_gender_consumer > 0.75 then 'M' 
                when avg_gender_consumer < 0.25 then 'F'
                else 'U' end as gender_preference,
            qa.gender_affinity,
            qa.exclude_gumdrop,
            qa.secondary_product_
            from analytics.dbt_exports.export__agg_creator_product_stats tagged
            left join analytics.creator.manual_product_qa qa
            on tagged.shop_product_id = qa.shop_product_id
            where exclude_gumdrop is null and secondary_product_ is null
        )
         ,drops as (
        select
        m.handle as creator_id,
        m.discount_percentage,
        m.assignment,
        m.sort_order,
        m.product_id,
        d.collab_metadata,
        prod.imageurl,
        prod.product_gross_margin,
        prod.breakeven_discount_pct,
        max_claimed_count,
        prod.gender_preference as gender_cleaned,
        prod.min_price as price,
        prod.product_title,
        os_merchant,
        'drop' as status

        from ds_creator_dev_database.service.creator_product_match m
        left join ds_creator_dev_database.service.product_detail d
        on m.product_id = d.product_id
        and m.product_release_id = d.product_release_id
        left join prod 
        on m.product_id = prod.shop_product_id
        where m.handle = '{influ}' 
        and d.product_release_id = '{release}'
    
        )
        -- , browse as (
        -- select distinct
        -- m.creator_id,
        --    null as discount_percentage,
        --    m.assignment,
        --    d.sort_order,
        --    d.product_id,
        --    d.collab_metadata,
        --    d.imageurl,
        --    product_gross_margin,
        --    breakeven_discount_pct,
        --    max_claimed_count,
        --   gender_cleaned,
        --    price,
        --    product_title,
        --   os_merchant,
        --    'scroll' as status
        -- from ds__database.influencer.product_detail d 
        -- left join ds_dev_database.influencer.creator_product_match m
        -- on m.product_release_id = d.product_release_id
        --     and d.product_id <> m.product_id
        -- where creator_id = '{influ}' 
        -- and d.product_release_id = '{release}'
        -- )
        select 
            *
            from drops
        --    union 
        -- select * from browse
    
        qualify

        row_number() over (partition by product_id, status, sort_order order by discount_percentage asc)=1
        order by status, sort_order asc
        '''
        return s.run(q)