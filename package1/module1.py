import pandas as pd
from sqlalchemy import create_engine


# originaly at http://potacho.com/files/ironhack/Albrodmar.db
def createdf() -> object:
    engine = create_engine('sqlite:///data/raw/Albrodmar.db')
    df_business = pd.read_sql_query("SELECT * FROM business_info", engine)
    df_personal = pd.read_sql_query("SELECT * FROM personal_info", engine)
    df_rank = pd.read_sql_query('SELECT * FROM rank_info', engine)
    df_temp = pd.merge(df_business, df_personal, on='id', how='outer')
    df_merged = pd.merge(df_temp, df_rank, on='id', how='outer')
    df_merged = df_merged.drop(
        ['realTimeWorth', 'lastName', 'Unnamed: 0_x', 'Unnamed: 0_y', 'Unnamed: 0', 'position', 'worthChange',
         'realTimePosition'], axis=1)
    df_merged[['industry', 'company_name']] = df_merged.Source.str.split(' ==>', expand=True)

    # drop old column Source
    df_merged = df_merged.drop(['Source'], axis=1)
    df_merged['name'] = df_merged['name'].str.title()
    df_merged['age'] = df_merged.age.str.replace(' years old', '')
    df_merged['age'] = df_merged['age'].apply(pd.to_numeric)
    df_merged['age'] = df_merged['age'].apply(lambda x: 2018 - x if x > 150 else x)
    df_merged['worth'] = df_merged.worth.str.replace('BUSD', '')
    df_merged.rename(columns={'worth': 'worth in Billion USD', 'country': 'Country'}, inplace=True)
    df_merged['worth in Billion USD'] = df_merged['worth in Billion USD'].apply(pd.to_numeric)
    m_filter = df_merged['gender'] == 'M'
    df_merged.loc[m_filter, 'gender'] = df_merged.loc[m_filter, 'gender'].replace('M', 'Male')
    f_filter = df_merged['gender'] == 'F'
    df_merged.loc[f_filter, 'gender'] = df_merged.loc[f_filter, 'gender'].replace('F', 'Female')
    df_merged[df_merged['Country'].isnull()].count()
    df = df_merged[pd.notnull(df_merged['Country'])]
    df.to_csv('data/processed/billionaires.csv', index=False)
    print('CSV file generated')  #at billionaires.csv generated at /data/processed


