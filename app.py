from curses import keyname
from tkinter import NS
import pandas as pd
import numpy as np
import streamlit as st
from urllib.request import urlopen
from PIL import Image
from dashboard import DashBoard
st.set_page_config(layout="wide")
image = Image.open('./assets/os_logo.jpg')

st.write('<style>div.st-bf{flex-direction:column;} div.st-ag{font-weight:bold;padding-left:2px;}</style>', unsafe_allow_html=True)

def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["ospassword"]["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Whats the word?", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Whats the word?", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 not polite, try again")
        return False
    else:
        # Password correct.
        return True

@st.experimental_singleton(show_spinner=True)
def cacher(username):
    ob = DashBoard(username=username, write=False)
    influ_select_from_list = ob.influ_info['INFLUENCER_HANDLE'].unique()

    return ob, influ_select_from_list

@st.experimental_singleton(show_spinner=True)
def cacherecos(influ_chk, drop_no):
    recos = ob.recommend_product(influ_chk, drop_no=drop_no).iloc[:max_recs,:]

    data = []
    for val in range(0,len(recos),1):
        img = Image.open(urlopen(recos.iloc[val]['IMAGEURL']))
        caption = recos.iloc[val].name + ' | ' + recos.iloc[val]['PRODUCT_TITLE'] + ' | Price : ${:.2f}'.format(recos.iloc[val]['PRICE']) + ' | % Discount : ' + str(recos.iloc[val]['INFLUENCER_PRODUCT_DISCOUNT']*100) + '% | Gross Margin : {:.2%}'.format(recos.iloc[val]['PRODUCT_GROSS_MARGIN'])
        product_id = recos.iloc[val]['SHOP_PRODUCT_ID']
        data.append((img, caption, product_id))

    return [data, recos]
   

h1, h2= st.columns([30,1])

h1.title('Influencer Simulations :tada:')
h2.image(image, caption='@rishimash',width=50)

if check_password():

    col1, col2 = st.columns([1,2])

    username = col1.text_input(label='USER',value='NATE',max_chars=30, key='user')

    ob, influ_select_from_list = cacher(username)

    influ_chk = col2.selectbox(label='INFLUENCER', options = influ_select_from_list)

    influ_data = ob.influ_info.loc[influ_chk]

    st.image(Image.open(urlopen(influ_data['picture'])))

    st.table(pd.concat([pd.DataFrame(ob.influ_info.loc[influ_chk, ['type','fullname','description','gender','age_group','followers','posts_count','engagement_rate','avg_likes','avg_comments','avg_views']]), pd.DataFrame(ob.influ_followers_engagement.set_index('INFLUENCER_HANDLE').loc[influ_chk,['% US Followers','engagement_rate_pctile','follower_pctile','Discount Policy']])]))

    c1, c2 = st.columns([1,1])
    drop_no = c1.number_input('Drop #',1)

    max_recs = c2.number_input('# Recommendations', 10)

    [data, recos] = cacherecos(influ_chk, drop_no)

    session_vals = {}

    idx = 0 
    while idx <= len(data)-1:
        # for _ in range(len(filteredImages)):
        cols = st.columns(2) 
        try:
            cols[0].image(data[idx][0], width=150, caption=data[idx][1])
        except:
            pass
        try:
            session_vals[data[idx][2]] = cols[1].radio("Rating # {}".format(idx+1),("NOT OK", "OK", "VERY OK"), index = 1, key= data[idx][2])
        except:
            pass
        idx+=1
        
    rescore = {'OK': 0, 'VERY OK':1, 'NOT OK': -1}
    recos['HUMAN_SCORE'] =  recos['SHOP_PRODUCT_ID'].apply(lambda x : rescore[session_vals[x]] if x in session_vals.keys() else np.nan)


    butt = st.button('Save')
    if butt:
        st.warning('Hey ' + username + ', fyi Influencer {}'.format(influ_chk) + '. Drop # {}'.format(drop_no) + ' scores have been saved, you may not see the same drops ever again!')
        recos['CREATED_AT'] = np.datetime64('now')
        recos['CREATED_AT'] = recos['CREATED_AT'].dt.tz_localize('UTC')
        ob.snowappender(recos[['OS_STORE_ID','SHOP_PRODUCT_ID','PRODUCT_TITLE','PRICE','OS_PRODUCT_COGS','PRODUCT_GROSS_MARGIN','INFLUENCER_PRODUCT_DISCOUNT','IMAGEURL','USERNAME','CREATED_AT','INFLUENCER_HANDLE','DROP_NUMBER','MODEL_NAME']])
        st.success('Thanks for your valuable feedback '+username+ ', Rishi will decide if its useful!')





