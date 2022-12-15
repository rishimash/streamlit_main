import pandas as pd
import streamlit as st
from urllib.request import urlopen
from PIL import Image
from json2html import *
import json
from query import SnowLoader, Query
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
            "Whats the 🪄 word?", type="password", on_change=password_entered, key="password"
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

@st.experimental_singleton()
def cacher(username):
    ob = SnowLoader()
    releases = ob.run('''select * from ds_creator_dev_database.service.product_release''')
    release_dict = releases.set_index('STARTS_AT')['ID'].to_dict()
    creators = ob.run('''select * from dbt_analytics.prod.base__user_profile_info order by _OS_LOADED_AT desc''')
    creators = creators.drop_duplicates(subset=['INFLUENCER_HANDLE'], keep='last').set_index('INFLUENCER_HANDLE')
    #ob.influ_info['engagement_rate'] = ob.influ_info['engagement_rate'].apply(lambda x : '{:.2%}'.format(float(x)))

    influ_select_from_list = list(creators.index)
    release_select_from_list = list(release_dict.keys())
    return [ob, creators, release_dict, release_select_from_list, influ_select_from_list]

@st.experimental_singleton()
def cacherecos(_s, influ_chk, release_dict, release_chk, max_recs):
    
    drops = Query.get_drop(_s, influ_chk, release_dict[release_chk])

    data = []
    for val in range(0,max_recs,1):
        img = Image.open(urlopen(drops.iloc[val]['IMAGEURL']))
        caption = drops.iloc[val]['STATUS'] +' | ' + drops.iloc[val]['OS_MERCHANT'] + ' | ' + drops.iloc[val]['PRODUCT_TITLE'] + ' | Price : ${:.2f}'.format(drops.iloc[val]['PRICE']) + ' | % Discount : ' + str(drops.iloc[val]['DISCOUNT_PERCENTAGE']*100) + '% | Gross Margin : {:.2%}'.format(drops.iloc[val]['PRODUCT_GROSS_MARGIN'])
        product_id = drops.iloc[val]['PRODUCT_ID']
        if drops.iloc[val]['STATUS'] == 'drop':
            try:
                description = json2html.convert(json =json.loads(drops.iloc[val]['COLLAB_METADATA']))
            except:
                description = '<b>DROP</b>'
        else:
            description = '<b>SCROLL</b>'
        data.append((img, caption, product_id, description))

    return [data, drops]
   

h1, h2= st.columns([30,1])

h1.title('Influencer Simulations 📱')
h2.image(image, caption='@rishimash',width=50)

if check_password():

    col1, col2, col3= st.columns([1,2,2])

    username = col1.text_input(label='USER',value='Nate',max_chars=30, key='user')

    with st.spinner('Give me like 1 min to load all the 💩 ...'):
        [ob, creators, release_dict, release_select_from_list, influ_select_from_list] = cacher(username)

    influ_chk = col2.selectbox(label='INFLUENCER', options = influ_select_from_list)
    st.image(Image.open(urlopen(creators.loc[influ_chk]['PICTURE'])))
    st.table(pd.DataFrame(creators.loc[influ_chk]))

    c1, c2 = st.columns([1,1])
    drop_no  = c1.selectbox(label='Release', options = release_select_from_list)
    max_recs = c2.number_input('# Obs', 5)

    if st.button('Run!'):
        with st.spinner('Wait for it 🕒 ...'):
            [data, drops] = cacherecos(ob, influ_chk, release_dict, drop_no, max_recs)

        session_vals = {}

        idx = 0 
        while idx <= len(data)-1:
            cols = st.columns(2) 
            try:
                cols[0].image(data[idx][0], width=150, caption=data[idx][1])
                cols[1].markdown(data[idx][3], unsafe_allow_html=True)
            except:
                pass
            # try:
            #     session_vals[data[idx][2]] = cols[2].radio("Rating {}".format(idx),("NOT OK", "OK", "VERY OK"), index = 1, key= data[idx][2])
            # except:
            #     pass
            idx+=1
            
        # rescore = {'OK': 0, 'VERY OK':1, 'NOT OK': -1}
        # recos['HUMAN_SCORE'] =  recos['SHOP_PRODUCT_ID'].apply(lambda x : rescore[session_vals[x]] if x in session_vals.keys() else np.nan)


        # butt = st.button('Save')
        # if butt:
        #     st.warning('Hey ' + username + ', fyi Influencer {}'.format(influ_chk) + '. Drop # {}'.format(drop_no) + ' scores have been saved, you may not see the same drops ever again!')
        #    # recos['CREATED_AT'] = np.datetime64('now')
        #    # recos['CREATED_AT'] = recos['CREATED_AT'].dt.tz_localize('UTC')
        #     #ob.snowappender(recos[['OS_STORE_ID','SHOP_PRODUCT_ID','PRODUCT_TITLE','PRICE','OS_PRODUCT_COGS','PRODUCT_GROSS_MARGIN','INFLUENCER_PRODUCT_DISCOUNT','IMAGEURL','USERNAME','CREATED_AT','INFLUENCER_HANDLE','DROP_NUMBER','MODEL_NAME']])
        #     st.success('Thanks for your valuable feedback '+username+ ', Rishi will decide if its useful!')





