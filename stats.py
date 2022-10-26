from scipy import interpolate
import numpy as np
import math
import pandas as pd
from scipy.spatial.distance import euclidean
from scipy.stats.stats import pearsonr

def fit_cdf(hist_test):
  hist_test['min'] = hist_test['min'].fillna(0)
  hist_test['max'] = hist_test['max'].fillna(method='ffill')
  t = hist_test[['min','max','total']]
  t['cum_freq'] = (t['total'].cumsum()/t['total'].sum())
  f = interpolate.interp1d(x=t['cum_freq'], y=t['max'], kind='quadratic')
  f_inv = interpolate.interp1d(x=t[:-1]['max'], y=t[:-1]['cum_freq'], kind='quadratic')
  return [f, f_inv]

def NormalizeData(data):
    return (data - np.min(data)) / (np.max(data) - np.min(data))

def StandardizeData(data):
    return (data - np.mean(data)) / np.std(data)

def lambda_apply_subset(df_main, condition, x):
        
        d_copy = df_main.dropna(subset=x)
        sampler = pd.DataFrame(d_copy.groupby(condition)[x].agg(list))

        df_main[x] = df_main.apply(lambda y : np.median(sampler.loc[y[condition]][x] if (pd.isna(y[x]) or y[x]==0) else y[x] ) , axis=1)
        return df_main

def normalize_by_parts(df, by='col', func='scale'):
    if by != 'col':
        df = df.T
    for col in df.columns:
        if func=='norm':
            df[col] = NormalizeData(df[col])
        elif func=='std':
            df[col] = StandardizeData(df[col])
        else:
            df[col] = df[col]/df[col].sum()

    if by != 'col':
        df = df.T
    return df

def decay(start, half_life, length):
    coef = math.exp(-math.log(2)/half_life)
    return list(map(lambda t: start * coef ** t, range(length) ))


def reduce_matrix(mat1, mat2):
    rows = set(mat1.index).intersection(mat2.index)
    cols = set(mat1.columns).intersection(mat2.columns)
    return [mat1.loc[rows, cols], mat2.loc[rows, cols]]


def feat_engineer(fin, field):
    chk = fin.reports[field]
    if field == 'audience_genders_per_age_inf__audience_followers':
        feat_emg = pd.DataFrame(chk.set_index(['OS_MERCHANT','OS_HANDLE','INFLUENCER_HANDLE','CODE','_OS_CREATED_AT','code']).stack()).reset_index().rename(columns={0:'weight'})
        feat_emg['name'] = feat_emg['code'] + '_' + feat_emg['level_6']
    else:
        feat_emg = chk

    return feat_emg

def similarity_func(u, v):
    dist_metric = {}
    for row in u.index:
        dist_metric[row] =  (1/(1+euclidean(u.loc[row],v.loc[row])), pearsonr(u.loc[row],v.loc[row]) )
    return dist_metric


def create_embedding_matrix(fin, field):
    field_emg = feat_engineer(fin, field)

    aud_feat = field_emg.drop_duplicates(subset=['name','INFLUENCER_HANDLE'], keep='last')
    
    if 'affinity' not in aud_feat.columns:
        aud_feat.loc[:,'affinity'] = 1
    aud_feat.loc[:,'wt_affinity'] = aud_feat.loc[:,'weight']*aud_feat.loc[:,'affinity']
    audience_features = aud_feat.pivot_table(index='INFLUENCER_HANDLE', columns='name', values='wt_affinity', aggfunc='sum').fillna(0)
    audience_features.index.name = 'AUDIENCE_INFLUENCER_HANDLE_' +  field.split('_')[1].upper()
    audience_features = normalize_by_parts(audience_features, by='row')

    return audience_features


def transformer(influencer_brand_matrix, audience_feature):
    influ_list = set(influencer_brand_matrix.index).intersection(set(audience_feature.index))
    brand_feature = influencer_brand_matrix.loc[influ_list].T.dot(audience_feature.loc[influ_list])
    return brand_feature


def extrapolate_to_all_brands(brand_feature, brand_to_brand_matrix):
    return normalize_by_parts(brand_to_brand_matrix[brand_feature.index] , by='row', func='scale').dot(brand_feature)



def product_pricing(x, tier, max_thresh, min_thresh):
    if tier == 'FREE':
        if x['PRICE'] > max_thresh:
            return np.round(x['PRODUCT_GROSS_MARGIN'], 1)
        else:
            return 1
    if tier == 'COGS':
        if x['PRICE'] < min_thresh:
            return 1
        else:
            return np.round(x['PRODUCT_GROSS_MARGIN'], 1)
    else:
        if x['PRICE'] < min_thresh:
            return np.round(x['PRODUCT_GROSS_MARGIN'], 1)
        elif x['PRICE'] > max_thresh:
            return np.round(x['PRODUCT_GROSS_MARGIN']/4, 1)
        else:
            return np.round(x['PRODUCT_GROSS_MARGIN']/2, 1)




def policy_selection(x):
    if (x['engagement_rate_pctile'] > 0.75) or (x['follower_pctile'] > 0.75):
        return 'FREE'
    elif (x['engagement_rate_pctile'] > 0.5) or (x['follower_pctile'] > 0.5):
        return 'COGS'
    else:
        return 'NOMINAL'



    