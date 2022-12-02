import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# this mapper works to extract channel's name based on html id in the source code of the site for free, nova and cosmote channels.
channels_mapper={
    'free_channels':{
        'channel18':'ERT1',
        'channel87':'ERT2',
        'channel2':'ANT1',
        'channel3':'STAR',
        'channel5':'ALPHA',
        'channel7':'SKAI',
        'channel1':'MEGA',
        'channel99':'OPEN BEYOND',
        'channel6':'ERT3',
        'channel74':'NICKELODEON',
        'channel17':'TV MAKEDONIA',
        'channel80':'VOULI',
        },
    'nova_channels':{
          'channel74': 'NICKELODEON',
          'channel20': 'Animal Planet',
          'channel43': 'Boomerang',
          'channel116': 'CNN',
          'channel21': 'Discovery Channel',
          'channel44': 'Disney Channel',
          'channel42': 'Disney XD',
          'channel106': 'E! Entertainment',
          'channel35': 'EuroSport1',
          'channel36': 'EuroSport2',
          'channel66': 'Euronews',
          'channel37': 'FOX',
          'channel65': 'FOXlife',
          'channel19': 'MAD',
          'channel16': 'MTV',
         'channel114': 'MTV Hits',
         'channel113': 'MTV Live',
         'channel111': 'Mad GREEKZ',
         'channel40': 'Motors TV',
         'channel109': 'National Geographic HD',
         'channel24': 'NovaCinema1',
         'channel25': 'NovaCinema2',
         'channel33': 'NovaCinema3',
         'channel104': 'NovaCinema4',
         'channel115': 'Novalife',
         'channel34': 'The History Channel',
         'channel23': 'Travel Channel',
         'channel27': 'NovaSports1',
         'channel28': 'NovaSports2',
         'channel29': 'NovaSports3',
         'channel30': 'NovaSports4',
         'channel31': 'NovaSports5',
         
         },
     'cosmote_channels':{
         'channel74': 'Nickelodeon',
         'channel50': 'Al Jazeera',
         'channel78': 'BBC World News',
         'channel58': 'Bloomberg',
         'channel117': 'CBS Reality',
         'channel81': 'CNBC',
         'channel83': 'Cosmote Cinema 1',
         'channel95': 'Cosmote Cinema 2',
         'channel96': 'Cosmote Cinema 3',
         'channel98': 'Cosmote History',
         'channel68': 'Cosmote Sport 1',
         'channel56': 'Cosmote Sport 2',
         'channel69': 'Cosmote Sport 3',
         'channel92': 'Cosmote Sport 4',
         'channel93': 'Cosmote Sport 5',
         'channel94': 'Cosmote Sport 6',
         'channel61': 'Deutsche Welle',
         'channel66': 'Euronews',
         'channel37': 'FOX',
         'channel65': 'FOXlife',
         'channel60': 'France 24 Fr',
         'channel123': 'MAD Viral',
         'channel86': 'Village Cinema',
         #'channel77': 'TLC',
         #'channel86': 'Village Cinema',
         #'channel84': 'iConcerts HD',
         } 
}

# here is the 3 urls for each category (free, nova, cosmote)
site_selections={'free_channels':'https://programmatileorasis.gr/?date=',
                'nova_channels':'https://programmatileorasis.gr/nova.php?',
                'cosmote_channels':'https://programmatileorasis.gr/otetv.php?'}

def deCFEmail(fp):
    '''
    Here is a function that converts the [email protected] value to the actual text value
    '''
    try:
        r = int(fp[:2],16)
        email = ''.join([chr(int(fp[i:i+2], 16) ^ r) for i in range(2, len(fp), 2)])
        return email
    except (ValueError):
        pass

def generate_days_list(starting_date):
    '''
    Generates a list containing all days between starting_date and today.
    '''
    days_list=[]
    starting_date = starting_date
    today = datetime.date.today().strftime('%Y-%m-%d')
    starting_date = datetime.datetime.strptime(starting_date, '%Y-%m-%d')
    today = datetime.datetime.strptime(today, '%Y-%m-%d')
    step = datetime.timedelta(days=1)
    while starting_date<= today:
         days_list.append(starting_date.date().strftime( '%Y-%m-%d'))
         starting_date += step
    return days_list
    

def site_connection(date,channels_type):
    '''
    Url initialisation based on the free channels, nova or cosmote.
    '''
    url=   site_selections[channels_type]+date
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    return soup


def extract_channel_schedule(soup,channel_id):
    '''
    For a url (e.g. free channels) extracts the raw text of a single channel's schedule.
    '''
    channel_time_and_title=[]
    table = soup.find('table', id=channel_id)
    rows = table.findAll('td')
    for row in rows:
        channel_raw=row.text
        if channel_raw.split("🔍")[0]=='[email\xa0protected]':
            channel_raw=channel_raw.replace('[email\xa0protected]',deCFEmail(row.find('a')['data-cfemail']))
        #channel_raw=row.text
        #row_wo_description=channel_raw.split("🔍")[0]
        channel_time_and_title.append(channel_raw)
    channel_time_and_title=channel_time_and_title[2:]
    return channel_time_and_title
    

def create_dataframe(soup,channel_time_and_title,date,channel_name):
    '''
    For a url (e.g free channels) creates a dataframe containing program title start hour channel name for a single date for a single channel.
    '''
    program_time=[el for el in channel_time_and_title if channel_time_and_title.index(el)%2==0]
    program_title_description=[el for el in channel_time_and_title if channel_time_and_title.index(el)%2!=0]
    description=[]
    program_title=[]
    for row in program_title_description:
        program_title.append(row.split("🔍")[0])
        try:
            description.append(row.split("🔍")[1])
        except:
            description.append('')
        
    date_final=[]
    channels=[]
    for i in range(len(program_title)):
        date_final.append(date)
        channels.append(channel_name)
        
        
    dataframe=pd.DataFrame({'Date':date_final,'Start_Hour':program_time,'Title':program_title,'Description':description,'Channel':channels})
    return dataframe


def channels_extraction(channels_type):
    '''
    Here for a single channels_type (free nova cosmote) we extract all channels' schedule for each day and append it in a dataframe.
    '''
    final_data=pd.DataFrame({},columns=['Date', 'Start_Hour', 'Title','Description', 'Channel']) 
    for date in days_list:
        conn=site_connection(date,channels_type)
        for key, value in channels_mapper[channels_type].items():
            channel=extract_channel_schedule(conn,key)
            data=create_dataframe(conn,channel,date,value)
            #final_data=pd.concat([data])
            final_data=final_data.append(data)  
    return final_data

 
def convert_next_day(df,series):
    '''
    If start_hour is between 00:00 and 04:59 we add one day to Date.
    '''    
    df[series] = np.where(df['Start_Hour']<datetime.time(5,00,00), df[series]+pd.Timedelta(days=1), df[series])
    return df[series]
        



if __name__=='__main__':

    
    # Here you select the starting date for scrapper. last date is always the current date.
    days_list=generate_days_list(starting_date='2022-11-07')
    
    #extracts data for free channels
    free_channels_data=channels_extraction('free_channels')
    
    #extracts data for nova channels
    nova_channels_data=channels_extraction('nova_channels')
    
    #extracts data for cosmote channels
    cosmote_channels_data=channels_extraction('cosmote_channels')
    
    
    #Union of the results drop duplicates and replace irrelevant characters,
    final_data=pd.concat([free_channels_data,nova_channels_data,cosmote_channels_data])
    final_data.replace({'\t':'','\n':'','\r':''},regex=True,inplace=True)
    final_data.drop_duplicates(inplace=True)

    #final_data.sort_values(by='Date',ascending=True,inplace=True)
    #final_data['Description']=final_data['Description'].str.wrap(100)
    
    #convert date and start_hour to datetime and convert everything between 00:00 and 04:59 as next day.
    final_data['Date']=pd.to_datetime(final_data['Date'])
    final_data['Start_Hour'] = pd.to_datetime(final_data['Start_Hour']).dt.time
    final_data['Date']=convert_next_day(final_data,'Date')

    #Export the result to a csv.
    final_data.to_csv('programma_tileorasis_{}_till_{}.csv'.format(days_list[0],days_list[-1]),index=False, encoding='utf-8-sig')