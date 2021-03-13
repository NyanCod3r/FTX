import os
import ftx
import json
from pprint import pprint
import boto3
import mysql.connector
from datetime import datetime , timedelta
import traceback


REGION = os.environ['REGION']
fromaddress = os.environ['fromaddress']
toaddress = os.environ['toaddress']
APIKEY = os.environ['FTXAPIKEY']
APISECRET = os.environ['FTXAPISECRET']
DBHOST = os.environ['DBHOST']
DBUSER = os.environ['DBUSER']
DBPASS = os.environ['DBPASS']
DBNAME = os.environ['DBNAME']
# Notification settings
DATENOW = datetime.now().date()
DATEDB = DATENOW.strftime("%Y%m%d")
status = 'success'
defaultbody = None
defaultsubject = 'FTX lending rates ' + ' @ ' + str(DATENOW)

def lambda_handler(event, context):
    client = ftx.FtxClient(api_key = APIKEY, api_secret = APISECRET)
    result = client.get_lending_rates()
    #print(result)
    # AAVE
    AAVErates = json.dumps(result[2])
    AAVE = json.loads(AAVErates)
    AAVEestimate = (AAVE['estimate']) * 1000000
    AAVEprevious = (AAVE['previous']) * 1000000
    AAVEstre = str(AAVEestimate) + '%'
    AAVEstrp = str(AAVEprevious) + '%'

    # BTC
    BTCrates = json.dumps(result[62])
    BTC = json.loads(BTCrates)
    BTCestimate = (BTC['estimate']) * 1000000
    BTCprevious = (BTC['previous']) * 1000000
    BTCstre = str(BTCestimate) + '%'
    BTCstrp = str(BTCprevious) + '%'
    
    # CEL
    CELrates = json.dumps(result[27])
    CEL = json.loads(CELrates)
    CELestimate = (CEL['estimate']) * 1000000
    CELprevious = (CEL['previous']) * 1000000
    CELstre = str(CELestimate) + '%'
    CELstrp = str(CELprevious) + '%'

    # ETH
    ETHrates = json.dumps(result[33])
    ETH = json.loads(ETHrates)
    ETHestimate = (ETH['estimate']) * 1000000
    ETHprevious = (ETH['previous']) * 1000000
    ETHstre = str(ETHestimate) + '%'
    ETHstrp = str(ETHprevious) + '%'

    # EUR
    EURrates = json.dumps(result[35])
    EUR = json.loads(EURrates)
    EURestimate = (EUR['estimate']) * 1000000
    EURprevious = (EUR['previous']) * 1000000
    EURstre = str(EURestimate) + '%'
    EURstrp = str(EURprevious) + '%'
 
    # GBP
    GBPrates = json.dumps(result[37])
    GBP = json.loads(GBPrates)
    GBPestimate = (GBP['estimate']) * 1000000
    GBPprevious = (GBP['previous']) * 1000000
    GBPstre = str(GBPestimate) + '%'
    GBPstrp = str(GBPprevious) + '%'


    # LINK
    LINKrates = json.dumps(result[49])
    LINK = json.loads(LINKrates)
    LINKestimate = (LINK['estimate']) * 1000000
    LINKprevious = (LINK['previous']) * 1000000
    LINKstre = str(LINKestimate) + '%'
    LINKstrp = str(LINKprevious) + '%'

    # PAXG
    PAXGrates = json.dumps(result[62])
    PAXG = json.loads(PAXGrates)
    PAXGestimate = (PAXG['estimate']) * 1000000
    PAXGprevious = (PAXG['previous']) * 1000000
    PAXGstre = str(PAXGestimate) + '%'
    PAXGstrp = str(PAXGprevious) + '%'
       
    # TOMO
    TOMOrates = json.dumps(result[75])
    TOMO = json.loads(TOMOrates)
    TOMOestimate = (TOMO['estimate']) * 1000000
    TOMOprevious = (TOMO['previous']) * 1000000
    TOMOstre = str(TOMOestimate) + '%'
    TOMOstrp = str(TOMOprevious) + '%'

    # USD
    USDrates = json.dumps(result[83])
    USD = json.loads(USDrates)
    USDestimate = (USD['estimate']) * 1000000
    USDprevious = (USD['previous']) * 1000000
    USDstre = str(USDestimate) + '%'
    USDstrp = str(USDprevious) + '%'

    # USDT
    USDTrates = json.dumps(result[84])
    USDT = json.loads(USDTrates)
    USDTestimate = (USDT['estimate']) * 1000000
    USDTprevious = (USDT['previous']) * 1000000
    USDTstre = str(USDTestimate) + '%'
    USDTstrp = str(USDTprevious) + '%'

    # YFI
    YFIrates = json.dumps(result[89])
    YFI = json.loads(YFIrates)
    YFIestimate = (YFI['estimate']) * 1000000
    YFIprevious = (YFI['previous']) * 1000000
    YFIstre = str(YFIestimate) + '%'
    YFIstrp = str(YFIprevious) + '%'

    # WBTC
    WBTCrates = json.dumps(result[86])
    WBTC = json.loads(WBTCrates)
    WBTCestimate = (WBTC['estimate']) * 1000000
    WBTCprevious = (WBTC['previous']) * 1000000
    WBTCstre = str(WBTCestimate) + '%'
    WBTCstrp = str(WBTCprevious) + '%'

    msg = (
        '\nAAVE estimate ' + AAVEstre + 
        '\nAAVE previous ' + AAVEstrp + 
        '\nBTC estimate ' + BTCstre + 
        '\nBTC previous ' + BTCstrp +
        '\nCEL estimate ' + CELstre +
        '\nCEL previous ' + CELstrp +
        '\nETH estimate ' + ETHstre +
        '\nETH previous ' + ETHstrp +  
        '\nEUR estimate ' + EURstre +
        '\nEUR previous ' + EURstrp +
        '\nGBP estimate ' + GBPstre +
        '\nGBP previous ' + GBPstrp +
        '\nLINK estimate ' + LINKstre +
        '\nLINK previous ' + LINKstrp +
        '\nPAXG estimate ' + PAXGstre +
        '\nPAXG previous ' + PAXGstrp +
        '\nTOMO estimate ' + TOMOstre +
        '\nTOMO previous ' + TOMOstrp +
        '\nUSD estimate ' + USDstre +
        '\nUSD previous ' + USDstrp +
        '\nUSDT estimate ' + USDTstre +
        '\nUSDT previous ' + USDTstrp + 
        '\nYFI estimate ' + YFIstre +
        '\nYFI previous ' + YFIstrp +
        '\nWBTC estimate ' + WBTCstre +
        '\nWBTC previous ' + WBTCstrp
    )
    mailer_func(fromaddress,toaddress,msg,status,DATEDB,AAVEestimate,BTCestimate,CELestimate,ETHestimate,EURestimate,GBPestimate,LINKestimate,PAXGestimate,TOMOestimate,USDestimate,USDTestimate,YFIestimate,WBTCestimate)

def mailer_func(fromaddress,toaddress,msg,status,DATEDB,AAVEestimate,BTCestimate,CELestimate,ETHestimate,EURestimate,GBPestimate,LINKestimate,PAXGestimate,TOMOestimate,USDestimate,USDTestimate,YFIestimate,WBTCestimate):
    '''
    This function will produce an email
    '''
    try:
        print ('"Starting the mailer"')
        client = boto3.client('ses','eu-central-1')
        response = client.send_email(
            Source= fromaddress,
            Destination={
                'ToAddresses': [
                    toaddress,
                ]
            },
            Message={
                'Subject': {
                    'Data': defaultsubject,
                },
                'Body': {
                    'Text': {
                        'Data': msg,
                    }
                }
            }
        )
        print(response)
        print ('\nMailer completed')
        database_rg(DATEDB,AAVEestimate,BTCestimate,CELestimate,ETHestimate,EURestimate,GBPestimate,LINKestimate,PAXGestimate,TOMOestimate,USDestimate,USDTestimate,YFIestimate,WBTCestimate)
    except Exception as error:
        errdescr = type(error).__name__ + '\n' + traceback.format_exc()
        print(errdescr)
'''
Register to DB
'''
def database_rg(DATEDB,AAVEestimate,BTCestimate,CELestimate,ETHestimate,EURestimate,GBPestimate,LINKestimate,PAXGestimate,TOMOestimate,USDestimate,USDTestimate,YFIestimate,WBTCestimate):
    try:
        db_connection = mysql.connector.connect(
            host = DBHOST,
            user = DBUSER,
            password = DBPASS ,
            database = DBNAME
            )
        db_cursor = db_connection.cursor()
        rate_sql_query = "INSERT INTO Rates(RatingDate,AVE,BTC,CEL,ETH,EUR,GBP,LNK,PAG,TOM,USD,UST,YFI,WBT) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = [
            DATEDB,
            AAVEestimate,
            BTCestimate,
            CELestimate,
            ETHestimate,
            EURestimate,
            GBPestimate,
            LINKestimate,
            PAXGestimate,
            TOMOestimate,
            USDestimate,
            USDTestimate,
            YFIestimate,
            WBTCestimate
            ]
        db_cursor.execute(rate_sql_query, values)
        db_connection.commit()
        print(db_cursor.rowcount, "Record Inserted")
        exit_func()

    except Exception as error:
        errdescr = type(error).__name__ + '\n' + traceback.format_exc()
        print(errdescr)


def error_func(errdescr):
    '''
    This function will handle the errors by setting a status and compiling a message
    '''
    try:
        status = 'failed'
        msg = 'Somethign went really wrong' + ' with \n' + errdescr
        mailer_func(fromaddress,toaddress,msg,status)
    except Exception as error:
        errdescr = type(error).__name__ + '\n' + traceback.format_exc()
        print(errdescr)

def exit_func():
    '''
    This function is just to exit the lambda script
    '''
    return {'statusCode': 200,'body': json.dumps(status)}
    



lambda_handler('RequestId: 371258a2-1392-478e-9125-c918b4d33182','RequestId: 371258a2-1392-478e-9125-c918b4d33182')
#database_rg(20211109 , 0.00001, 1.2)
