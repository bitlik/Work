def Rolling_data_PCA_Percentile(df,rank_dates, nr_days, features):
    # output path
    Output_path='C:\My_work\MXE\CM_Config\output'
    
    # Coverting date column to datetime datatype
    df.date = pd.to_datetime(df.date)
    dates1 = df.date
    # creating Blank data frames for storing the output of the iterations 
    rolledup_data=pd.DataFrame(columns=["ACCESS_FAILURE_CC_503.1.223._VOLTE_IMS","CALL_SETUP_DELAY_VOLTE_IMS",
                                        "DROP_CC_408_VOLTE_IMS","DROP_CC_OTHER_VOLTE_IMS","GARBLING_VOLTE_VOICE",
                                        "IMMEDIATE_VOLUNTARY_DROP_DUE_TO_MEDIA_STOP_VOLTE","MUTING_VOLTE_VOICE",
                                        "SOFT_DROP_DUE_TO_MEDIA_STOP_VOLTE_IMS","hard_drop_call_503_481",
                                        "cell_name_anon","band","date"])

    final_df_band2100 = pd.DataFrame(columns=["cell_name_anon","band","score","date","rank","percentile"])
    final_df_band700 = pd.DataFrame(columns=["cell_name_anon","band","score","date","rank","percentile"])
   # final_df_band1900_1 = pd.DataFrame(columns=["cell_name_anon","band","score","date","rank","percentile"])
        
    for rank_date in rank_dates:
        
        # coverting rank_date string to date 
        rank_date = dt.date(*map(int, rank_date.split('-')))
        print(rank_date)
        # Preparing the window for 7 days
        
        start_date = rank_date - dt.timedelta(1)
        end_date = start_date - dt.timedelta(nr_days)
        #print(df['date'].head(2))
        dff = df[(df['date'] < rank_date) & (df['date'] >= end_date)]
        # CHANGE MADE HERE
        dff['hard_drop_call_503_481']= dff['DROP_CC_481_VOLTE_IMS']+ dff['DROP_CC_503_VOLTE_IMS']
         # CHNAGE MADE HERE
        # grouping by cell_name_anon , band
        grp_dict1= {'ACCESS_FAILURE_CC_503.1.223._VOLTE_IMS' :'sum',
            'CALL_SETUP_DELAY_VOLTE_IMS': 'sum',
            'DROP_CC_408_VOLTE_IMS' : 'sum',
           'DROP_CC_OTHER_VOLTE_IMS': 'sum',
           'GARBLING_VOLTE_VOICE': 'sum',
           'IMMEDIATE_VOLUNTARY_DROP_DUE_TO_MEDIA_STOP_VOLTE': 'sum',
           'MUTING_VOLTE_VOICE': 'sum', 
           'SOFT_DROP_DUE_TO_MEDIA_STOP_VOLTE_IMS':  'sum',
           'hard_drop_call_503_481': 'sum',
           'call_count': 'sum'} 
    
        df1 = dff.groupby(['cell_name_anon','band']).agg(grp_dict1 , as_index = False)
    
        df1.reset_index(inplace=True)
    
        df2= df1.iloc[:,2:].div(df1.call_count, axis=0)
    
        df3 = pd.concat([df2.iloc[:,0:9], df1[['cell_name_anon','band']]], axis = 1)
        
        df3['date']=rank_date
        
         # append all iterations
        rolledup_data=rolledup_data.append(df3,ignore_index = True)
    
        ##########################################
        # ----------------Band 2100---------- ####
        ##########################################
        # Separating out the features
        df_band2100 = df3[df3.band == 'band2100']
        df_band2100.reset_index(inplace = True)
        x = df_band2100.loc[:, features1].values
        # Standardizing the features
        x = StandardScaler().fit_transform(x)
        # doing PCA
        pca = PCA()
        principalComponents = pca.fit_transform(x)
        # converting to dataframe
        principalDf = pd.DataFrame(data = principalComponents
             , columns = ['principal component 1', 'principal component 2','principal component 3',
                          'principal component 4','principal component 5', 'principal component 6',
                         'principal component 7','principal component 8', 'principal component 9'
                          ])
        # selecting where explained variance >= 1
        exp_variance = pca.explained_variance_
        # fetching indexes
        idx = np.where(exp_variance >= 1)[0]
        #print(idx) ; print(type(idx[0]))
        #fetching multipliers
        exp_ratio = pca.explained_variance_ratio_[idx]
        # slicing Principal components and adding cell_name_anon & band
        cols=['cell_name_anon', 'band']
        _df_band2100 =  pd.concat([principalDf.iloc[:,idx], df_band2100[cols]], axis = 1)
        _df_band2100['score'] = np.sum(_df_band2100.iloc[:,idx] * exp_ratio , axis = 1)
        _df_band2100['date'] = rank_date
        _df_band2100.sort_values(by=['score'],ascending=True , inplace=True)
        _df_band2100.reset_index(inplace= True)
        _df_band2100['rank'] = _df_band2100.index.values+1
        _df_band2100 = _df_band2100[['cell_name_anon','band','score','date','rank']]
        
        _df_band2100['percentile'] = _df_band2100.score.rank(pct = True)
        _df_band2100.percentile = 1 - _df_band2100.percentile 
        _df_band2100.percentile = _df_band2100.percentile * 100
              
        # append all iterations
        final_df_band2100=_df_band2100.append(final_df_band2100,ignore_index = True)
        #print('success')
        print(final_df_band2100.head(2))
               
        ##########################################
        # ----------------Band 700---------- ####
        ##########################################
        # Separating out the features
        df_band700 = df3[df3.band == 'band700']
        df_band700.reset_index(inplace = True)
        x = df_band700.loc[:, features1].values
        # Standardizing the features
        x = StandardScaler().fit_transform(x)
        # doing PCA
        pca = PCA()
        principalComponents = pca.fit_transform(x)
        # converting to dataframe
        principalDf = pd.DataFrame(data = principalComponents
             , columns = ['principal component 1', 'principal component 2','principal component 3',
                          'principal component 4','principal component 5', 'principal component 6',
                         'principal component 7','principal component 8', 'principal component 9'
                          ])
        # selecting where explained variance >= 1
        exp_variance = pca.explained_variance_
        # fetching indexes
        idx = np.where(exp_variance >= 1)[0]
        #print(idx) ; print(type(idx[0]))
        #fetching multipliers
        exp_ratio = pca.explained_variance_ratio_[idx]
        # slicing Principal components and adding cell_name_anon & band
        cols=['cell_name_anon', 'band']
        _df_band700 =  pd.concat([principalDf.iloc[:,idx], df_band700[cols]], axis = 1)
        _df_band700['score'] = np.sum(_df_band700.iloc[:,idx] * exp_ratio , axis = 1)
        _df_band700['date'] = rank_date
        _df_band700.sort_values(by=['score'],ascending=True , inplace=True)
        _df_band700.reset_index(inplace= True)
        _df_band700['rank'] = _df_band700.index.values+1
        _df_band700 = _df_band700[['cell_name_anon','band','score','date','rank']]
        _df_band700['percentile'] = _df_band700.score.rank(pct = True)
        _df_band700.percentile = 1 - _df_band700.percentile 
        _df_band700.percentile = _df_band700.percentile * 100
        
        # append all iterations
        final_df_band700=_df_band700.append(final_df_band700,ignore_index = True)
        #print('success')
        print(final_df_band700.head(2))
        
    return final_df_band2100,final_df_band700, rolledup_data  