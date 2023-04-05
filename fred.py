import pandas as pd
import numpy as np

import hvplot.pandas
pd.options.plotting.backend='plotly'

import param
from dataclasses import dataclass

import json
import requests
import urllib.parse
from functools import lru_cache
import hashlib
API_KEY ='cab3e725510931b4d65601d15dfe93a2'
@dataclass
class Escalation:
    series_id = 'GDP'
    _api_key = API_KEY
    _series = pd.DataFrame()
    
    #def __post_init__(self,api_key='', series_id=None):
        #self._series = self.get_series(self.series_id)
    
    
     
    def __hash__(self):
        h = hashlib.sha256(str(self.__dict__).encode('utf-8')).hexdigest()
        return hash(h)
    
    def get_series(self, series_ids,url='https://api.stlouisfed.org/fred/series/observations?', **kwargs):

        if isinstance(series_ids, str):
            series_ids = [series_ids]
        if not isinstance(series_ids, list):
            print('series_id must be a string or a list')
            return False
        df = self._series
        for series_id in series_ids:
            info = self.series_info(series_id=series_id)
            results = (self
                       .request_df(series_id=series_id,url=url,records_name='observations', **kwargs)
                       .assign(value=lambda x: pd.to_numeric(x.value,errors='coerce')
                             
                              )
                      )
            
            is_percent = "percent" in info.units.str.lower()[0]
            results = (info
                       .merge(results, left_on='id', right_on='series_id', suffixes=("", "_obs"))
                       .assign( rate = lambda x: x.value/100 if is_percent else x.value/x.value.shift(1)-1)
                      )
            #tmp_df = pd.DataFrame(dict(date = results.index, values=results.values , rate = results /results.shift(1)-1)).assign(**info).assign(**kwargs)
            df = df.append(results, ignore_index=True)
        df.date = pd.to_datetime(df.date)

        return df

    
    
    
    def series_info(self,series_id, url='https://api.stlouisfed.org/fred/series?',**kwargs):
        df = self.request_df(series_id=series_id, url=url, records_name='seriess').merge(self.get_series_sources(series_id),left_on='series_id', right_on='series_id', suffixes=("", "_sources"))
        #df = self.request_df(series_id=series_id, url=url, records_name='seriess')
        return df
    
    
    def search(self,search_text,url='https://api.stlouisfed.org/fred/series/search?', **kwargs):
        df = self.request_df(search_text=search_text,url=url, records_name='seriess', **kwargs)                
        return df
    
    
    def make_index(self,series_id= None,df=pd.DataFrame(), base_year=2022, outlays =(.25,.25,.5),FY=tuple(range(2010, 2030)), forecast_method='median'):
        kwargs = locals()
        def raw(df,rate_col='rate'):
            df['raw'] = 1
            for i in range(1,df.shape[0]):
                df.loc[i, 'raw'] = df.at[i-1, 'raw']* (1+ df.at[i,rate_col])
            return df

        def wtd(df,outlay_cols=(0,1,2)):

            df['wtd']=None
            for i in range(df.shape[0]-len(outlay_cols)):
                pass
                df.loc[i, 'wtd'] = (df.loc[range(i, i+len(outlay_cols)), 'raw'].values * df.loc[i,list(outlay_cols)].values).sum()
            return df

    
    
    
        if series_id is None and df.empty:
            print('provide series id or dataframe')
            return pd.DataFrame()
        if isinstance(series_id, str):
            df = self.get_series(series_id,frequency='a',aggregation_method='eop' )
        if isinstance(series_id, list):
            
            del kwargs['series_id']
            del kwargs['self']
            #print(kwargs)
            index = pd.DataFrame()
            for s in series_id:
                #tmp = self.make_index(series_id = s, **kwargs)
                index = index.append(self.make_index(series_id=s,**kwargs), ignore_index=True) 
            return index
        if len (df.series_id.unique()) >1:
            kwargs = locals()
            del kwargs['series_id']
            del kwargs['df']
            #print(kwargs)
            index = pd.DataFrame()
            for frame in df.groupby('series_id'):
                index = index.append(self.make_index(s,**kwargs), ignore_index=True) 
            return index


        forecast_rate = df.rate.median()
        
        index = pd.DataFrame(dict(
            FY = range(1970, 2061))).assign(
            series_id = df.id.unique()[0], id = df.id.unique()[0] ,title = df.title.unique()[0],units=df.units.unique()[0],
            #source=df.source.unique()[0],source_links=df.source_links.unique()[0],
            forecast_method=forecast_method)
        for i, outlay in enumerate(outlays):
            index[i] = outlay

        index['outlay_sum'] = index[list(range(len(outlays)))].fillna(0).sum(axis=1)

        index = (index
                 .merge(df.assign(FY= lambda x: x.date.dt.year).groupby('FY')[['value','rate']].mean().reset_index(), on='FY', how='left').fillna({'rate':forecast_rate})
                 .pipe(raw)
                 .pipe(wtd,list(range(len(outlays)) ))
                 .assign(combo_factor = lambda x: (x.wtd / x.raw).fillna(method='ffill'))
                 .assign(base_year = base_year, raw = lambda x: x.raw / x.query(f'FY=={base_year}').raw.mean(), wtd = lambda x: x.combo_factor*x.raw )
                )

        return index    

    def get_series_from_search(self,search_term,limit=1, **kwargs):
        if isinstance(search_term, str):
            search_term = [search_term]
        if isinstance(search_term, list):
            
            ids = list()
            for search in search_term:
                ids= ids + self.search(search, limit=limit, **kwargs).id.unique().tolist()
            print(ids)
            df = self.get_series(ids, **kwargs)
            return df
        else:
            print("Please provide a string or a list of strings. Returning an empty dataframe")
            return pd.DataFrame()

    
    def _list_of_strs(self,var):
        if isinstance(var, str):
            return [var]
        if isinstance(var,list):
            return var
        else:
            raise TypeError("Provide a string or list object")
    
    def make_index_from_search(self,search_term,limit=1,base_year=2022, outlays =(.25,.25,.5),FY=tuple(range(2010, 2030)), forecast_method='median'):
        kwargs = locals()
        del kwargs['self']
        del kwargs['search_term']
        del kwargs['limit']
        df = pd.DataFrame()
        search_term = self._list_of_strs(search_term)
        for search in search_term:
            try:
                ids = self.get_series_from_search(search, limit=limit).id.unique().tolist()
                df = df.append(self.make_index(series_id = ids, **kwargs), ignore_index=True)
            except:
                print("search failed: ", search)
                pass

        return df        
        
    def get_release(self,series_id, url = 'https://api.stlouisfed.org/fred/series/release?'):
        df = self.request_df(url, series_id=series_id, records_name='releases')

        return df
    
    def get_series_sources(self,series_id, url = 'https://api.stlouisfed.org/fred/release/sources?'):
        df = self.get_release(series_id).rename({'id':'release_id', 'name':'release_name', 'link':'release_link'}, axis=1)
        release_id = df.release_id.unique()[0]
        sources = self.request_df(url=url, release_id=release_id, records_name='sources')
        df=df.assign(source = ", ".join(sources.name.unique()), source_links=", ".join(sources.link.unique()))
        
        return df
                     
    def request_json(self,url,**payload):
        kw= urllib.parse.urlencode(payload)
        url = url +'&api_key='+ self._api_key +'&file_type=json&'+ kw
        #print(url)
        r=requests.request('GET', url)
        
        return r.json()
    
    @lru_cache
    def request_df(self,url,records_name,**payload):
        j = self.request_json(url, **payload)
        df = pd.DataFrame.from_records(j[records_name])
        for key, item in payload.items():
            if key in df.columns:
                key=key+"_param"
            df.insert(0 ,column=key, value= item)
        return df
    