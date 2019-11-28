# Author: Raihan Shafique
# This code has been tried in Python version 3.6


import PIL.Image
import io
import os
import pyodbc
import pandas as pd
import spacy
import sklearn
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation

# Get Current Directory to save file later in this location.
cwd = os.getcwd()

# Connect to SQL Server DB - Python3.6 virtual env
server = os.getenv('ifms_sql_server_prod2')
database = 'EDW'
username = os.getenv('ifms_sql_server_user_name')
password = os.getenv('ifms_sql_server_pass')

#pyodbc connection details

cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
# cursor = cnxn.cursor()
# Run your sql commmand to extract the varbinary image data - change accordingly
query = "Select convert(date, convert(varchar(10), date_SID), 101) as Date, a.* \
FROM[dbo].[Fact_Service_Request] a \
where \
format(convert(date, convert(varchar(10), date_SID), 101), 'yyyyMM') >= format(dateadd(month, -6, getdate()), 'yyyyMM') \
and classification in ('Compliment', 'Complaint', 'Feedback')"

df = pd.read_sql(query,cnxn)

# nlp = spacy.load('en_code_web_sm')
# nlp = spacy.load('en_code_web_lg') for the larger file


# from sklearn.feature_extraction.text import CountVectorizer
cv = CountVectorizer(max_df=0.9, min_df=2,stop_words='english')

# list(df.columns)
stream = df.Stream_SID.unique()
classification = df.Classification.unique()
location = df.Location_SID.unique()

columns = ['Location','Stream','Classification','Topic','Word']
df_topics = pd.DataFrame(columns= columns)
df_topics_list = []

columns_pivot = ['Location','Stream','Classification','Topic1']
df_topics_pivot1 = pd.DataFrame(columns=columns_pivot)

columns_pivot = ['Location','Stream','Classification','Topic2']
df_topics_pivot2 = pd.DataFrame(columns=columns_pivot)

columns_pivot = ['Location','Stream','Classification','Topic3']
df_topics_pivot3 = pd.DataFrame(columns=columns_pivot)

columns_pivot = ['Location','Stream','Classification','Topic4']
df_topics_pivot4 = pd.DataFrame(columns=columns_pivot)




for lo in enumerate(location):
    print(lo[1])

    for st in enumerate(stream):
        print(st)
        for cl in enumerate(classification):
            print(cl)

            dff = df[(df.Stream_SID == st[1]) & (df.Classification == cl[1]) & (df.Location_SID == lo[1])]
            # print(dff.shape)

            try:
                dtm = cv.fit_transform(dff['Service_Request_Description'])
                # from sklearn.decomposition import LatentDirichletAllocation
                LDA = LatentDirichletAllocation(n_components=4, random_state=42)
                LDA.fit(dtm)

                for i, topic in enumerate(LDA.components_):
                    print(f'THE TOP 20 WORDS FOR TOPIC #[{i+1}]')
                    print([cv.get_feature_names()[index] for index in topic.argsort()[-20:]])

                    '''
                    for index in topic.argsort()[-20:]:
                        df_topics_list.append([cl[1],st[1],i,cv.get_feature_names()[index]])
                        print(cv.get_feature_names()[index])
                    '''

                    [df_topics_list.append([lo[1], st[1], cl[1], i + 1, cv.get_feature_names()[index]]) for index in topic.argsort()[-20:]]

                    for index in topic.argsort()[-20:]:

                        if i + 1 == 1:
                            df_topics_pivot1 = df_topics_pivot1.append(
                                {'Location': lo[1], 'Stream': st[1], 'Classification': cl[1],
                                 'Topic' + str(i + 1): cv.get_feature_names()[index]}, ignore_index=True)
                        elif i + 1 == 2:
                            df_topics_pivot2 = df_topics_pivot2.append(
                                {'Location': lo[1], 'Stream': st[1], 'Classification': cl[1],
                                 'Topic' + str(i + 1): cv.get_feature_names()[index]}, ignore_index=True)
                        elif i + 1 == 3:
                            df_topics_pivot3 = df_topics_pivot3.append(
                                {'Location': lo[1], 'Stream': st[1], 'Classification': cl[1],
                                 'Topic' + str(i + 1): cv.get_feature_names()[index]}, ignore_index=True)
                        else:
                            df_topics_pivot4 = df_topics_pivot4.append(
                                {'Location': lo[1], 'Stream': st[1], 'Classification': cl[1],
                                 'Topic' + str(i + 1): cv.get_feature_names()[index]}, ignore_index=True)

                    print(df_topics_list)
            except:
                s = st[1]
                c = cl[1]
                print(
                    "Could not fit.transform dtm as max_df corresponds to < documents in min_df in the \n\t Stream: {} \n\t Classification: {}".format(
                        s, c))

        df_topics = pd.DataFrame(df_topics_list, columns=columns)

df_topics_pivot = pd.concat([df_topics_pivot1,df_topics_pivot2,df_topics_pivot3,df_topics_pivot4],sort=False,ignore_index=True,axis=1)

df_topics_pivot.drop(df_topics_pivot.columns[[4,5,6,8,9,10,12,13,14]],axis=1,inplace=True)

df_topics_pivot.columns = ['Location','Stream','Classification','Topic1','Topic2','Topic3','Topic4']









