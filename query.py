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


    def get_drop(s, influ, release):
        q = f'''
        with drops as (
        select
        m.creator_id,
        m.discount_percentage,
        m.assignment,
        m.sort_order,
        m.product_id,
        d.collab_metadata,
        d.imageurl,
        product_gross_margin,
        breakeven_discount_pct,
        max_claimed_count,
        gender_cleaned,
        price,
        product_title,
        os_merchant,
        'drop' as status

        from ds_dev_database.influencer.creator_product_match m
        left join ds_dev_database.influencer.product_detail d
        on m.product_id = d.product_id
        and m.product_release_id = d.product_release_id
        where creator_id = '{influ}'
        and m.product_release_id = '{release}'
        ), browse as (
        select distinct
        m.creator_id,
            null as discount_percentage,
            m.assignment,
            d.sort_order,
            d.product_id,
            d.collab_metadata,
            d.imageurl,
            product_gross_margin,
            breakeven_discount_pct,
            max_claimed_count,
            gender_cleaned,
            price,
            product_title,
            os_merchant,
            'scroll' as status
        from ds_dev_database.influencer.product_detail d 
        left join ds_dev_database.influencer.creator_product_match m
        on m.product_release_id = d.product_release_id
            and d.product_id <> m.product_id
        where creator_id = '{influ}' 
        and d.product_release_id = '{release}'
        )
        select 
            *
            from drops
            union 
        select * from browse
    
        qualify

        row_number() over (partition by product_id, status, sort_order order by discount_percentage asc)=1
        order by status, sort_order asc
        '''
        return s.run(q)