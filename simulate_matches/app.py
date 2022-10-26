import pandas as pd
import numpy as np
import streamlit as st
from urllib.request import urlopen
from PIL import Image
from dashboard import DashBoard
st.set_page_config(layout="wide")
#image = Image.open('./assets/os_logo.jpg')


h1, h2= st.columns([30,1])

h1.title('Influencer Simulations :tada:')
#h2.image(image, caption='@rishimash',width=50)

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





