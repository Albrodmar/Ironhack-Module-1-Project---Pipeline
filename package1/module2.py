import matplotlib.pyplot as plt
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
import os

def webscrapping():
    url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)'
    df = pd.read_html(url)
    df[2] #there are several tables but just need this one
    gdp = df[2]
    gdp.rename(columns = {0:'Rank', 1:'Country', 2:'GDP in Billions USD'}, inplace = True) #still values in millions
    gdp_new = gdp.iloc[198:388]
    gdp_new = gdp_new.reset_index()
    gdp_new = gdp_new.drop(columns = ['Rank','index'])
    gdp_new['Country'] = gdp_new['Country'].str.replace(r'\[.+','')
    gdp_new['GDP in Billions USD'] = gdp_new['GDP in Billions USD'].str.replace(r'\ \(2017\)','')
    gdp_new['GDP in Billions USD'] = gdp_new['GDP in Billions USD'].str.replace(r'\[.+','')
    gdp_new['GDP in Billions USD'] = gdp_new['GDP in Billions USD'].str.replace(',','')
    gdp_new['GDP in Billions USD'] = gdp_new['GDP in Billions USD'].astype(int)
    gdp_new['GDP in Billions USD'] = gdp_new['GDP in Billions USD']/1000
    gdp_new.to_csv('/home/alberto/Ironhack/projects/Ironhack-Module-1-Project---Pipeline/data/processed/gdp.csv', index = False)
    print('gdp.csv file generated at /data/processed')

def mergingcsv():
    df_b = pd.read_csv('/home/alberto/Ironhack/projects/Ironhack-Module-1-Project---Pipeline/data/processed/billionaires.csv', index_col=False)
    df_g = pd.read_csv('/home/alberto/Ironhack/projects/Ironhack-Module-1-Project---Pipeline/data/processed/gdp.csv', index_col=False)
    df = pd.merge(df_b, df_g, on='Country', how='inner')
    df.to_csv('/home/alberto/Ironhack/projects/Ironhack-Module-1-Project---Pipeline/data/processed/final.csv', index=False)
    print ('stage1 file and webscrapping file merged at /data/processed/final.csv')

def ploting():
    df = pd.read_csv('/home/alberto/Ironhack/projects/Ironhack-Module-1-Project---Pipeline/data/processed/final.csv')
    plt.style.use('seaborn')
    bycountry = df.groupby(['Country', 'GDP in Billions USD'])['worth in Billion USD'].sum().reset_index(
        name='Total richest worth')
    bycountry['% of GDP own by the richest'] = round(
        bycountry['Total richest worth'] / bycountry['GDP in Billions USD'] * 100)
    bycountry.sort_values(by=['% of GDP own by the richest'], inplace=True, ascending=False)
    x = bycountry['Country']
    y = bycountry['% of GDP own by the richest']
    bycountry.plot(x='Country', y='% of GDP own by the richest', kind='barh', figsize=(6, 12))
    fig1 = plt.gcf()
    fig1.savefig('/home/alberto/Ironhack/projects/Ironhack-Module-1-Project---Pipeline/data/results/image.png')
    print('image generated with the plot at /data/results/image.png')

def final_emailing():
    load_dotenv()
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    sender_email_id = os.getenv('EMAIL')
    sender_email_id_password = os.getenv('PASSWORD')
    receiver_email_id = input('Please introduce address to receive the email: ')
    msg = MIMEMultipart()
    msg['From'] = sender_email_id
    msg['To'] = receiver_email_id
    msg['Subject'] = 'GDP own by millionaires by country'
    body = 'Please find attached an image with the info'
    msg.attach(MIMEText(body, 'plain'))
    filename = "image.jpg"
    file_path = ('/home/alberto/Ironhack/projects/Ironhack-Module-1-Project---Pipeline/data/results/image.png')
    attachment = open(file_path, 'rb')
    attached_MIME = MIMEBase('application', 'octet-stream')
    attached_MIME.set_payload((attachment).read())
    encoders.encode_base64(attached_MIME)
    attached_MIME.add_header('Content-Disposition', f'attachment; filename= {filename}', )
    msg.attach(attached_MIME)
    text = msg.as_string()
    s.login(sender_email_id, sender_email_id_password)
    s.sendmail(sender_email_id, receiver_email_id, text)
    s.quit()
    print('Email sent')

if __name__ == '__main__':
    webscrapping()
    mergingcsv()
    ploting()
    final_emailing()