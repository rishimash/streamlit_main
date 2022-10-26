import pandas as pd
import streamlit as st
from urllib.request import urlopen
from PIL import Image
from dashboard import DashBoard
st.set_page_config(layout="wide")
image = Image.open('assets/os_logo.jpg')



from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from filereader import FileReader
from stats import *
from query import *
from fuzzywuzzy import fuzz
from nltk.corpus import stopwords
import re
import nltk
import datetime



# class DashBoard:


#     def __init__(self, username='Nate',session_time=datetime.datetime.now(), model_name = 'V0'):
#         nltk.download('stopwords')
#         self.username = username
#         self.session_time = session_time
#         self.model_name = model_name
#         self.f = FileReader(SAVE_PATH='main/Archivetables/')
#         self.final_model_output = pd.read_csv('main/Attribution/data.csv', index_col=0)
#         self.s = SnowLoader()
#         self.cust_orders_df = Query.cross_orders(self.s)
        
#         self.prods = self.s.run(Query.products_q)

#         self.audience_interests = create_embedding_matrix(self.f,'audience_interests_inf__audience_followers')
#         self.audience_genders = create_embedding_matrix(self.f,'audience_genders_per_age_inf__audience_followers')
#         self.audience_ethnicity = create_embedding_matrix(self.f, 'audience_ethnicities_inf__audience_followers')
#         self.audience_language = create_embedding_matrix(self.f, 'audience_languages_inf__audience_followers')
#         eng_per_dollar_tbl = self.final_model_output.pivot_table(index=['INFLUENCER_HANDLE','OS_MERCHANT','CODE'], values=['TOTAL_ENGAGEMENT','OS_INFLUENCER_CAC_PER_POST'], aggfunc='mean').reset_index()
#         eng_per_dollar_tbl = eng_per_dollar_tbl.pivot_table(index=['INFLUENCER_HANDLE','OS_MERCHANT'], values=['TOTAL_ENGAGEMENT','OS_INFLUENCER_CAC_PER_POST'], aggfunc='sum')
#         eng_per_dollar_tbl['ENG_PER_$'] = eng_per_dollar_tbl['TOTAL_ENGAGEMENT']/eng_per_dollar_tbl['OS_INFLUENCER_CAC_PER_POST']
#         self.influencer_brand_use_engagement_per_dollar = normalize_by_parts(eng_per_dollar_tbl.pivot_table(index='INFLUENCER_HANDLE', columns ='OS_MERCHANT', values='ENG_PER_$', aggfunc='mean').fillna(0))

#         self.brand_interests = transformer(self.influencer_brand_use_engagement_per_dollar, self.audience_interests)
#         self.brand_gender_age = transformer(self.influencer_brand_use_engagement_per_dollar, self.audience_genders)
#         self.brand_ethnicity = transformer(self.influencer_brand_use_engagement_per_dollar, self.audience_ethnicity)
#         self.brand_language = transformer(self.influencer_brand_use_engagement_per_dollar, self.audience_language)


#         # clean_orders = cust_orders_df[~(cust_orders_df['EMAIL'].str.contains('@getelevar.com') | cust_orders_df['EMAIL'].str.contains('@open.store') | cust_orders_df['EMAIL'].str.contains('andrewjcampbell1@gmail.com') | cust_orders_df['EMAIL'].str.contains('joncairo@gmail.com') | cust_orders_df['EMAIL'].str.contains('@affiliatemanager.com'))]

#         orders_pivot = self.cust_orders_df.pivot(index='EMAIL',columns='OS_MERCHANT',values='NUM_ORDERS').fillna(0)
#         self.brand_to_brand = orders_pivot.T @ orders_pivot


#         self.all_brand_interests = extrapolate_to_all_brands(self.brand_interests.fillna(0), self.brand_to_brand)
#         self.all_brand_gender_age = extrapolate_to_all_brands(self.brand_gender_age.fillna(0), self.brand_to_brand)
#         self.all_brand_languages = extrapolate_to_all_brands(self.brand_language.fillna(0), self.brand_to_brand)
#         self.all_brand_ethnicities = extrapolate_to_all_brands(self.brand_ethnicity.fillna(0), self.brand_to_brand)


#         audience_geo = self.f.reports['audience_geo_countries_inf__audience_followers']
#         audience_us = audience_geo[audience_geo['code']=='US']
#         self.audience_us_pct = audience_us.pivot_table(index=['INFLUENCER_HANDLE'], values='weight', aggfunc='mean').reset_index().rename(columns={'weight':'% US Followers'})

#         self._clean_prods()
#         self._create_eng_hists()
#         self._engagement_rate_pctiler()
#         self._discount_policy()


#     def _clean_prods(self):
#         stop_words = stopwords.words('english')
#         pattern = re.compile('[\W_]+')

#         self.prods['STOPWORD_CLEANER'] = self.prods['PRODUCT_TITLE'].apply(lambda x :  ' '.join([word for word in x.split() if word not in stop_words]))
#         self.prods['CLEANED_TITLE'] = self.prods['STOPWORD_CLEANER'].apply(lambda x: pattern.sub(' ', x).lower())

#         cleaned_prods = []
#         for merch in self.prods['OS_MERCHANT'].unique():
#             tst = self.prods[self.prods['OS_MERCHANT']==merch].sort_values(by='LIFETIME_SALES_RANK', ascending=True).reset_index(drop=True)
#             for val in tst.index:
#                 try:
#                     tst_me = tst.loc[val, 'CLEANED_TITLE']
#                     tst['Distance'] = tst['CLEANED_TITLE'].apply(lambda x : fuzz.ratio(x, tst_me))
#                     tst['Remove'] = tst.apply(lambda x : True if (x['Distance'] <= 99) and (x['Distance'] > 70)  else False, axis=1)
#                     tst = tst[tst['Remove']==False].reset_index(drop=True)
#                 except:
#                     pass
#             cleaned_prods.append(tst)

#         self.prods = pd.concat(cleaned_prods)
#         self.prods['GENDER_CLEANED'] = self.prods['GENDER_CLEANED'].fillna('U')


#     def _create_eng_hists(self):
#         eng_rate_hists = self.f.reports['engagement_rate_histogram']
#         populate_eng_hists = eng_rate_hists.pivot_table(index=['INFLUENCER_HANDLE'], values='CODE', aggfunc='max').reset_index()
#         cleaned_eng_rate_hists =  eng_rate_hists.merge(populate_eng_hists, how='inner', on=['INFLUENCER_HANDLE','CODE'])

#         self.eng_rate_hist_percentile_dict  = {}
#         self.eng_rate_hist_eng_func_dict = {}

#         for merch in cleaned_eng_rate_hists['INFLUENCER_HANDLE'].unique():
#             try:
#                 [f_, f_inv ] = fit_cdf(cleaned_eng_rate_hists[cleaned_eng_rate_hists['INFLUENCER_HANDLE']==merch])
#                 self.eng_rate_hist_percentile_dict.loc[:,merch] = f_
#                 self.eng_rate_hist_eng_func_dict.loc[:,merch] = f_inv
#             except:
#                 pass

#     def _engagement_rate_pctiler(self):
#         self.influ_followers_engagement = self.f.reports['user_profile_info'].sort_values(by=['INFLUENCER_HANDLE','CODE']).drop_duplicates(subset=['INFLUENCER_HANDLE'], keep='last')[[ 'INFLUENCER_HANDLE', 'followers','engagement_rate']]
#         self.influ_followers_engagement = self.influ_followers_engagement.reset_index(drop=True)
#         self.influ_followers_engagement = self.influ_followers_engagement.merge(self.audience_us_pct, how='left')
#         self.influ_followers_engagement = self.influ_followers_engagement.rename(columns={'followers':'raw_followers'})
#         self.influ_followers_engagement['followers'] = self.influ_followers_engagement['raw_followers']*self.influ_followers_engagement['% US Followers']
                
#         self.influ_followers_engagement['engagement_rate_pctile'] = 0

#         for idx in self.influ_followers_engagement.index:
#             try:
#                 func = self.eng_rate_hist_eng_func_dict[self.influ_followers_engagement.loc[idx,'INFLUENCER_HANDLE']]
#                 avg_eng_rate = self.influ_followers_engagement.loc[idx,'engagement_rate']
#                 self.influ_followers_engagement.loc[idx,'engagement_rate_pctile'] = func(avg_eng_rate)
#             except:
#                 self.influ_followers_engagement.loc[idx,'engagement_rate_pctile'] = 0.9

#         self.influ_followers_engagement['follower_pctile'] = pd.qcut(self.influ_followers_engagement['followers'], 5, labels=[0, 0.25, 0.5, 0.75, 1])
#         self.influ_followers_engagement['Discount Policy'] = self.influ_followers_engagement.apply(lambda x : policy_selection(x), axis=1)

#         self.influ_info = self.f.reports['user_profile_info'].drop_duplicates(subset=['username'], keep='last').set_index('username')
#         self.influ_info['GENDER_CODE'] = self.influ_info['gender'].apply(lambda x : 'M' if x == 'MALE' else 'F')


#     def _discount_policy(self):
#         max_thresh = self.prods['PRICE'].quantile(0.9)
#         min_thresh = self.prods['PRICE'].quantile(0.1)

#         self.prods = self.prods.sort_values(by=['OS_MERCHANT','PRICE','OS_PRODUCT_COGS'], ascending=True)
#         self.prods['FREE'] = self.prods.apply(lambda x : product_pricing(x, 'FREE', max_thresh, min_thresh), axis=1)
#         self.prods['COGS'] = self.prods.apply(lambda x : product_pricing(x, 'COGS', max_thresh, min_thresh), axis=1)
#         self.prods['NOMINAL'] = self.prods.apply(lambda x : product_pricing(x, 'NOMINAL', max_thresh, min_thresh), axis=1)

        
#     def recommend_product(self, influ_chk):
        

#         try:
#             startf =self.s.run(f"""select SHOP_PRODUCT_ID, DROP_NUMBER from DS_DEV_DATABASE.INFLUENCER.HUMAN_EVALUATION where INFLUENCER_HANDLE = '{influ_chk}' and USERNAME = '{self.username}'""")
#         except:
#             startf = pd.DataFrame()

#         interest_rank = pd.DataFrame(self.audience_interests.loc[influ_chk]).T.dot(self.all_brand_interests.T)
#         gender_age_rank = pd.DataFrame(self.audience_genders.loc[influ_chk]).T.dot(self.all_brand_gender_age.T)
#         ethinicity_rank = pd.DataFrame(self.audience_ethnicity.loc[influ_chk]).T.dot(self.all_brand_ethnicities.T)
#         language_rank = pd.DataFrame(self.audience_language.loc[influ_chk]).T.dot(self.all_brand_languages.T)

#         brand_score = interest_rank + gender_age_rank + ethinicity_rank + language_rank ### CHANGE TO USE DATA AXLE AGE & GENDER ###
#         sorted_brands = brand_score.T.sort_values(by=influ_chk, ascending=False)

#         subset_to_show = list(sorted_brands.index)

#         if len(startf) > 0:
#             already_shown_prods = list(startf['SHOP_PRODUCT_ID'])
#             drop_number = max(startf['DROP_NUMBER']) + 1
#         else:
#             already_shown_prods = []
#             drop_number = 1

#         influencer_discount_prods = self.prods[self.prods['OS_MERCHANT'].isin(subset_to_show)].sort_values(by=['OS_MERCHANT','GENDER_PREFERENCE','PRICE','PRODUCT_GROSS_MARGIN'], ascending=[True, True, False, False])
#         influencer_discount_prods = influencer_discount_prods[~influencer_discount_prods['SHOP_PRODUCT_ID'].isin(already_shown_prods)]
#         available_set = pd.DataFrame(subset_to_show, columns=['Sort Order']).reset_index().set_index('Sort Order').loc[influencer_discount_prods['OS_MERCHANT'].unique()].sort_values(by='index')

#         prods_to_show = influencer_discount_prods.drop_duplicates(['OS_MERCHANT'], keep='first').set_index('OS_MERCHANT').loc[available_set.index]
#         prods_to_show = prods_to_show[prods_to_show['GENDER_CLEANED'].isin([self.influ_info.loc[influ_chk, 'GENDER_CODE'], 'U' ])]

#         use_col = self.influ_followers_engagement.set_index('INFLUENCER_HANDLE').loc[influ_chk]['Discount Policy']
#         prods_to_show['INFLUENCER_PRODUCT_DISCOUNT'] = prods_to_show[use_col]

#         prods_to_show = prods_to_show.dropna(subset=['PRODUCT_TITLE',"PRICE",'OS_PRODUCT_COGS','PRODUCT_GROSS_MARGIN','INFLUENCER_PRODUCT_DISCOUNT','IMAGEURL'])

#         prods_to_show['USERNAME'] = self.username
#         prods_to_show['CREATED_AT'] = self.session_time
#         prods_to_show['INFLUENCER_HANDLE'] = influ_chk
#         prods_to_show['DROP_NUMBER'] = drop_number
#         prods_to_show['MODEL_NAME'] = self.model_name
        

#         fin = self.snowappender(prods_to_show[['OS_STORE_ID','SHOP_PRODUCT_ID','PRODUCT_TITLE','PRICE','OS_PRODUCT_COGS','PRODUCT_GROSS_MARGIN','INFLUENCER_PRODUCT_DISCOUNT','IMAGEURL','USERNAME','CREATED_AT','INFLUENCER_HANDLE','DROP_NUMBER','MODEL_NAME']])
    
#         return prods_to_show



#     def snowappender(self, df, database='DS_DEV_DATABASE',schema = 'INFLUENCER', table='HUMAN_EVALUATION', startf=None):
#         if startf is None:
#             try:
#                 startf = self.s.run(f'''select * from  {database}.{schema}.{table}''')
#             except:
#                 startf = pd.DataFrame(columns=df.columns)
#         fin = pd.concat([startf, df])
#         self.s.run(f"""use database {database};""")
#         self.s.run(f"""use schema {schema};""")


#         success, num_chunks, num_rows, output = write_pandas(
#                     conn=self.s.conn,
#                     df=df.reset_index(drop=True),
#                     table_name=table,
#                     database=database,
#                     schema=schema
#                 )

#         return fin






h1, h2= st.columns([30,1])

h1.title('Influencer Simulations :tada:')
h2.image(image, caption='@rishimash',width=50)

@st.experimental_singleton(show_spinner=True)
def cacher(username):
    ob = DashBoard(username=username)
    influ_select_from_list = ob.influ_info['INFLUENCER_HANDLE'].unique()

    return ob, influ_select_from_list


col1, col2, col3 = st.columns([1,2, 1])

username = col1.text_input(label='USER',value='NATE',max_chars=30, key='user')

ob, influ_select_from_list = cacher(username)

influ_chk = col2.selectbox(label='INFLUENCER', options = influ_select_from_list)

butt = col3.button('Recommend Products!')

if butt:

    recos = ob.recommend_product(influ_chk)

    influ_data = ob.influ_info.loc[influ_chk]

    # col1, rec1, rec2, rec3, rec4 = st.columns([2, 1, 1, 1, 1])

    st.image(Image.open(urlopen(influ_data['picture'])))

    st.table(pd.concat([pd.DataFrame(ob.influ_info.loc[influ_chk, ['type','fullname','description','gender','age_group','followers','posts_count','engagement_rate','avg_likes','avg_comments','avg_views']]), pd.DataFrame(ob.influ_followers_engagement.set_index('INFLUENCER_HANDLE').loc[influ_chk,['% US Followers','engagement_rate_pctile','follower_pctile','Discount Policy']])]))

    filteredImages = []
    caption = []
    for val in range(0,len(recos),1):
        filteredImages.append(Image.open(urlopen(recos.iloc[val]['IMAGEURL'])))
        caption.append(recos.iloc[val].name + ' | ' + recos.iloc[val]['PRODUCT_TITLE'] + ' | Price : $' +  str(recos.iloc[val]['PRICE']) + ' | % Discount : ' + str(recos.iloc[val]['INFLUENCER_PRODUCT_DISCOUNT']*100) + '% | Gross Margin : '+ str(np.round(recos.iloc[val]['PRODUCT_GROSS_MARGIN'])))

    idx = 0 
    while idx < len(filteredImages)-1:
        # for _ in range(len(filteredImages)):
        cols = st.columns(4) 
        try:
            cols[0].image(filteredImages[idx], width=150, caption=caption[idx])
            idx+=1
        except:
            pass
        try:
            cols[1].image(filteredImages[idx], width=150, caption=caption[idx])
            idx+=1
        except:
            pass
        try:
            cols[2].image(filteredImages[idx], width=150, caption= caption[idx])
            idx+=1
        except:
            pass
        try:
            cols[3].image(filteredImages[idx], width=150, caption= caption[idx])
            idx = idx + 1
        except:
            pass





