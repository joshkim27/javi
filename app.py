# app.py

import os
import urllib

import boto3
import requests

import logging.handlers
from datetime import datetime, timedelta, date
from flask import Flask, jsonify, request
from dateutil.relativedelta import relativedelta
from calendar import monthrange, monthcalendar
from boto3.dynamodb.conditions import Key, Attr

import pytz

istTimeZone = pytz.timezone('Asia/Kolkata')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s')

streamHandler = logging.StreamHandler()
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

app = Flask(__name__)

USERS_TABLE = os.environ['JAVI_USERS_TABLE']
DAILY_TABLE = os.environ['JAVI_DAILY_TABLE']
WEEKLY_TABLE = os.environ['JAVI_WEEKLY_TABLE']
MONTHLY_TABLE = os.environ['JAVI_MONTHLY_TABLE']
YEARLY_TABLE = os.environ['JAVI_YEARLY_TABLE']
CONFIG_TABLE = os.environ['JAVI_CONFIG_TABLE']
LEDGER_TABLE = os.environ['JAVI_LEDGER_TABLE']
BOT_ID = os.environ['BOT_ID']
TOKEN = os.environ['TOKEN']
STAGE = os.environ['STAGE']

IS_OFFLINE = os.environ.get('IS_OFFLINE')

if IS_OFFLINE:
    client = boto3.client(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
    dynamodb = boto3.resource('dynamodb', region_name='localhost', endpoint_url="http://localhost:8000")

else:
    client = boto3.client('dynamodb')
    dynamodb = boto3.resource('dynamodb')

@app.route("/")
def hello():
    return jsonify({'message': 'Hello'})


@app.route("/welcome", methods=['POST'])
def create_user():
    # input parameter 처리
    # dynamodb 에 널값 입력 안됨??
    userId = request.form['messenger user id']
    if not userId:
        return jsonify({'error': 'Please provider userId'}), 400

    first_name = request.form['first name']
    if not first_name:
        first_name = 'None'
    last_name = request.form['last name']
    if not last_name:
        last_name = 'None'
    gender = request.form['gender']
    if not gender:
        gender = 'None'
    chatfuel_userId = request.form['chatfuel user id']
    if not chatfuel_userId:
        chatfuel_userId = 'None'
    source = request.form['source']
    if not source:
        source = 'None'
    profile_pic_url = request.form['profile pic url']
    if not profile_pic_url:
        profile_pic_url = 'None'
    locale = request.form['locale']
    if not locale:
        locale = 'None'
    timezone = request.form['timezone']
    if not timezone:
        timezone = 'None'
    ref = request.form['ref']
    if not ref:
        ref = 'None'
    longitude = request.form['longitude']
    if not longitude:
        longitude = 'None'
    latitude = request.form['latitude']
    if not latitude:
        latitude = 'None'
    uioNotiSales = request.form['uioNotiSales']
    if not uioNotiSales:
        uioNotiSales = '21'
    uioNotiLedger = request.form['uioNotiLedger']
    if not uioNotiLedger:
        uioNotiLedger = '09'

    put_user(userId, first_name, last_name, gender, chatfuel_userId, source, profile_pic_url, locale, timezone, ref,
             longitude, latitude, uioNotiSales, uioNotiLedger)

    return jsonify({"text": "Welcome to the Javi Rockets!"})


@app.route("/daily", methods=['POST'])
def create_daily():
    userId = request.form['messenger user id']
    if not userId:
        return jsonify({'error': 'Please provider userId'}), 400

    uimDailySales = request.form['uimDailySales']
    uimDailyBuying = request.form['uimDailyBuying']

    logger.info('daily_input|'+userId+'|'+uimDailySales+'|'+uimDailyBuying)

    todayDate = datetime.today()

    isExist = put_daily(userId, uimDailySales, uimDailyBuying, todayDate)

    # 일입력 체크
    update_dailyInputCheck(userId, True)

    # daily 없는 경우만 합산처리 하는 걸로
    # daily 가 누적으로 바뀌는 경우 체크 없애야 함
    if not isExist:
        # weekly 합산 처리
        put_weekly(userId, uimDailySales, uimDailyBuying, todayDate)

        # monthly 합산 처리
        put_monthly(userId, uimDailySales, uimDailyBuying, todayDate)

        # yearly 합산 처리
        put_yearly(userId, uimDailySales, uimDailyBuying, todayDate)

    return jsonify({})

@app.route("/monthly/cost", methods=['POST'])
def put_monthly_cost():
    userId = request.form['messenger user id']
    if not userId:
        return jsonify({'error': 'Please provider userId'}), 400

    uioRentalPeriod = request.form['uioRentalPeriod']
    if not uioRentalPeriod:
        uioRentalPeriod = 'None'

    uimRentalPayDate = request.form['uimRentalPayDate']
    if not uimRentalPayDate:
        uimRentalPayDate = '0'

    uioRentalAmount = request.form['uioRentalAmount']
    if not uioRentalAmount:
        uioRentalAmount = '0'

    uioEmployeeNumber = request.form['uioEmployeeNumber']
    if not uioEmployeeNumber:
        uioEmployeeNumber = '0'

    uioEmployeeAmount = request.form['uioEmployeeAmount']
    if not uioEmployeeAmount:
        uioEmployeeAmount = '0'

    uioOtherCost = request.form['uioOtherCost']
    if not uioOtherCost:
        uioOtherCost = '0'

    uioOtherCostDueDate = request.form['uioOtherCostDueDate']
    if not uioOtherCostDueDate:
        uioOtherCostDueDate = '0'

    uimEmployeePayDate = request.form['uimEmployeePayDate']
    if not uimEmployeePayDate:
        uimEmployeePayDate = '0'

    this_month = datetime.now().strftime('%Y%m')

    update_monthly_cost(userId + this_month, uioRentalPeriod, uimRentalPayDate, uioOtherCostDueDate, uimEmployeePayDate,
                        uioRentalAmount, uioEmployeeNumber, uioEmployeeAmount, uioOtherCost)

    return jsonify({})


# 리포트 호출 시점의 값 조회 방
@app.route("/daily/today/<string:userId>")
def get_daily_report(userId):
    today = datetime.now().strftime("%Y%m%d")
    cvToday = datetime.now().strftime("%Y-%m-%d")

    resp = client.get_item(
        TableName=DAILY_TABLE,
        Key={
            'userDailyId': {'S': userId + today}
        }
    )

    item = resp.get('Item')
    if not item:
        uimDailySales = '0'
        uimDailyBuying = '0'
        cvDailyProfit = '0'
    else:
        uimDailySales = item.get('uimDailySales').get('N')
        uimDailyBuying = item.get('uimDailyBuying').get('N')
        cvDailyProfit = str(int(uimDailySales) - int(uimDailyBuying))

    return jsonify({
        "set_attributes": {
            "uimDailySales": uimDailySales,
            "uimDailyBuying": uimDailyBuying,
            "cvDailyProfit": cvDailyProfit,
            "cvToday": cvToday
        },
    })


@app.route("/weekly/thisweek/<string:userId>")
def get_weekly_report(userId):
    this_week = datetime.now().strftime('%Y%W')
    cvWeek = 'W' + datetime.now().strftime('%W')

    resp_weekly = client.get_item(
        TableName=WEEKLY_TABLE,
        Key={
            'userWeeklyId': {'S': userId + this_week}
        }
    )

    item_weekly = resp_weekly.get('Item')
    if not item_weekly:
        cvWeeklySales = '0'
        cvWeeklyBuying = '0'
        cvWeeklyProfit = '0'
    else:
        cvWeeklySales = item_weekly.get('cvWeeklySales').get('N')
        cvWeeklyBuying = item_weekly.get('cvWeeklyBuying').get('N')
        cvWeeklyProfit = str(int(cvWeeklySales) - int(cvWeeklyBuying))

    # 데이터 없는 경우 처리 필요


    # month period 처리
    this_month = datetime.now().strftime('%Y%m')
    month_start_date = date.today().replace(day=1)
    today = date.today()
    cvMonthPeriod = datetime.strftime(month_start_date, '%d %b') + ' - ' + datetime.strftime(today, '%d %b')
    cvMonth = datetime.now().strftime('%B')

    resp_monthly = client.get_item(
        TableName=MONTHLY_TABLE,
        Key={
            'userMonthlyId': {'S': userId + this_month}
        }
    )

    item_monthly = resp_monthly.get('Item')
    if not item_monthly:
        cvMonthlySales = '0'
        cvMonthlyBuying = '0'
        cvMonthlyProfit = '0'

        uioRentalPeriod = 'None'
        uimRentalPayDate = 0
        uioRentalAmount = '0'
        uioEmployeeNumber = '0'
        uioEmployeeAmount = '0'
        uimEmployeePayDate = 0
        uioOtherCostDueDate = 0
        uioOtherCost = '0'
        cvMonthlyCost = '0'
        cvMonthlyNetProfit = '0'
        cvExpectedProfit = '0'
        cvExpectedNetProfit = '0'
        cvPaymentDate = 'None'
    else:
        cvMonthlySales = item_monthly.get('cvMonthlySales').get('N')
        cvMonthlyBuying = item_monthly.get('cvMonthlyBuying').get('N')
        cvMonthlyProfit = str(int(cvMonthlySales) - int(cvMonthlyBuying))

        uioRentalPeriod = item_monthly.get('uioRentalPeriod').get('S')
        uimRentalPayDate = int(item_monthly.get('uimRentalPayDate').get('N'))
        uioRentalAmount = item_monthly.get('uioRentalAmount').get('N')
        uioEmployeeNumber = item_monthly.get('uioEmployeeNumber').get('N')
        uioEmployeeAmount = item_monthly.get('uioEmployeeAmount').get('N')
        uimEmployeePayDate = int(item_monthly.get('uimEmployeePayDate').get('N'))
        uioOtherCostDueDate = int(item_monthly.get('uioOtherCostDueDate').get('N'))
        uioOtherCost = item_monthly.get('uioOtherCost').get('N')

        # 렌탈 입력일에 따라 구분 buying 합산 안함 날짜 상관없음
        cvMonthlyCost = str(int(uioEmployeeAmount) + int(uioRentalAmount) + int(uioOtherCost))

        cvMonthlyNetProfit = str(int(cvMonthlySales) - int(cvMonthlyCost) - int(cvMonthlyBuying))

        # 일 입력 치 평균으로 계산 분모는 입력 날짜
        temp = daily_average(userId)
        cvExpectedProfit = str(int(cvMonthlyProfit) + (temp['avg_profit'] * temp['left_days']))
        cvExpectedNetProfit = str(int(cvExpectedProfit) - int(cvMonthlyCost))

        # cvPaymentDate = cal_payment_date(uimRentalPayDate)
        cvPaymentDate = cal_next_payment_date(uimRentalPayDate, uimEmployeePayDate, uioOtherCostDueDate)
        # date 뒤에 postfix 추가
        postfix_date = {'01': 'st', '21': 'st', '31': 'st', '02': 'nd', '22': 'nd', '03': 'rd', '23': 'rd'}
        cvPaymentDate = cvPaymentDate[:2] + postfix_date.get(cvPaymentDate[:2], 'th') + cvPaymentDate[2:]

    return jsonify({
        "set_attributes": {
            "cvWeeklySales": cvWeeklySales,
            "cvWeeklyBuying": cvWeeklyBuying,
            "cvWeeklyProfit": cvWeeklyProfit,
            "cvWeek": cvWeek,
            "cvMonth": cvMonth,
            "cvMonthPeriod": cvMonthPeriod,
            "cvMonthlyNetProfit": cvMonthlyNetProfit,
            "cvMonthlyCost": cvMonthlyCost,
            "cvMonthlyProfit": cvMonthlyProfit,
            "cvMonthlySales": cvMonthlySales,
            "cvMonthlyBuying": cvMonthlyBuying,
            "uioRentalPeriod": uioRentalPeriod,
            "uimRentalPayDate": uimRentalPayDate,
            "uioRentalAmount": uioRentalAmount,
            "uioEmployeeNumber": uioEmployeeNumber,
            "uioEmployeeAmount": uioEmployeeAmount,
            "uioOtherCost": uioOtherCost,
            "cvExpectedProfit": cvExpectedProfit,
            "cvExpectedNetProfit": cvExpectedNetProfit,
            "cvPaymentDate": cvPaymentDate

        },
    })

def add_postfix(date):
    postfix_date = {'01': 'st', '21': 'st', '31': 'st', '02': 'nd', '22': 'nd', '03': 'rd', '23': 'rd'}
    date = date[-2:] + postfix_date.get(date[-2:], 'th')
    return date

@app.route("/weekly/thisweek2/<string:userId>")
def get_weekly_report2(userId):
    weekday = datetime.now().weekday()
    cvWeek = 'W' + datetime.now().strftime('%W')

    daily_table = dynamodb.Table(DAILY_TABLE)

    dates = []

    for x in range(weekday+1):
        dates.append((datetime.now() - timedelta(days=(weekday-x))).strftime('%Y%m%d'))

    fe = Key('userDailyId').between(userId + dates[0], userId + dates[weekday])
    response = daily_table.scan(
        FilterExpression=fe,
    )

    temp ={}

    for i in response['Items']:
        logger.debug(i)
        temp[i['userDailyId']] = [i['uimDailySales'], i['uimDailyBuying']]

    message_header = cvWeek +" Sales report\n" + add_postfix_date(int(dates[0][-2:])) + ' ~ ' \
                     + add_postfix_date(int(dates[weekday][-2:])) +'\n\n'
    message_body = 'Date|Sales|Buying|Profit\n'

    sum_buying =0
    sum_sales =0

    for y in dates:

        if (userId+y) in temp:
            z = temp[userId + y]
            message_body = message_body + add_postfix(y) + '|' + str(z[0])+ '|'+ str(z[1])+ '|' + str(z[0]-z[1])
            sum_sales = sum_sales + z[0]
            sum_buying = sum_buying + z[1]
        else:
            message_body = message_body + add_postfix(y) + '|'+ "    -|    -|    -"

        message_body = message_body+ '\n'

    message_foot = '\n' + cvWeek + '|' + str(sum_sales) + '|' + str(sum_buying) + '|' + str(sum_sales-sum_buying)

    return jsonify({
        "messages":[ {"text": message_header + message_body + message_foot }]
    })

@app.route("/duedate/<string:userId>")
def get_duedate(userId):
    this_month = datetime.now().strftime('%Y%m')

    monthly_table = dynamodb.Table(MONTHLY_TABLE)

    response = monthly_table.query(
        KeyConditionExpression=Key('userMonthlyId').eq(userId+this_month),
    )

    item_monthly = response.get('Items')[0]
    logger.debug(item_monthly)
    if not item_monthly:
        message_body =  " - No due date"
    else:
        uimRentalPayDate = item_monthly['uimRentalPayDate']
        uioRentalAmount = item_monthly['uioRentalAmount']
        uioEmployeeAmount = item_monthly['uioEmployeeAmount']
        uimEmployeePayDate = item_monthly['uimEmployeePayDate']
        uioOtherCostDueDate = item_monthly['uioOtherCostDueDate']
        uioOtherCost = item_monthly['uioOtherCost']

        temp, cvPaymentDate = cal_next_payment_date(uimRentalPayDate, uimEmployeePayDate, uioOtherCostDueDate)
        # date 뒤에 postfix 추가
        postfix_date = {'01': 'st', '21': 'st', '31': 'st', '02': 'nd', '22': 'nd', '03': 'rd', '23': 'rd'}
        cvPaymentDate = cvPaymentDate[:2] + postfix_date.get(cvPaymentDate[:2], 'th') + cvPaymentDate[2:]

        if temp == uimRentalPayDate:
            message_body = ' - '+ str(uioRentalAmount) + 'rs. Rental fee on \n' + cvPaymentDate
        elif temp == uimEmployeePayDate:
            message_body = ' - '+ str(uioEmployeeAmount) + 'rs. Salary pay on \n' + cvPaymentDate
        elif temp == uioOtherCostDueDate:
            message_body = ' - ' + str(uioOtherCost) + 'rs. Other cost on \n' + cvPaymentDate

    message_header = 'Next Due Date\n'

    return jsonify({
        "messages":[ {"text": message_header + message_body }]
    })

@app.route("/callblock/<string:userId>/<string:blockName>")
def call_block(userId, blockName):
    return send_message(userId, blockName, {})


@app.route("/user/<string:userId>")
def get_user(userId):
    logger.debug('Call get_user')

    resp = client.get_item(
        TableName=USERS_TABLE,
        Key={
            'userId': {'S': userId}
        }
    )
    item = resp.get('Item')
    if not item:
        return jsonify({'error': 'User does not exist'}), 404

    logger.debug("item : %s", item)

    return jsonify(item)

@app.route("/cost/<string:userId>")
def get_cost(userId):
    logger.debug('Call get_cost')
    this_month = datetime.now().strftime('%Y%m')

    resp_monthly = client.get_item(
        TableName=MONTHLY_TABLE,
        Key={
            'userMonthlyId': {'S': userId + this_month}
        }
    )

    item = resp_monthly.get('Item')
    if not item:
        return jsonify({'error': 'User does not exist'}), 404

    uimRentalPayDate = int(item.get('uimRentalPayDate').get('N'))
    uimEmployeePayDate = int(item.get('uimEmployeePayDate').get('N'))
    uioOtherCostDueDate = int(item.get('uioOtherCostDueDate').get('N'))

    return jsonify({
        "set_attributes": {
            "cvRentalPayDate": add_postfix_date(uimRentalPayDate),
            "cvEmployeePayDate": add_postfix_date(uimEmployeePayDate),
            "cvOtherCostDueDate": add_postfix_date(uioOtherCostDueDate)
        }
    })


@app.route("/user/delete/<string:userId>")
def delete_user(userId):
    res = client.delete_item(
        TableName=USERS_TABLE,
        Key={
            'userId': {'S': userId}
        }
    )
    return jsonify(res)


@app.route("/daily/delete/<string:userDailyId>")
def delete_daily(userDailyId):
    res = client.delete_item(
        TableName=DAILY_TABLE,
        Key={
            'userDailyId': {'S': userDailyId}
        }
    )
    return jsonify(res)


@app.route("/weekly/delete/<string:userWeeklyId>")
def delete_weekly(userWeeklyId):
    res = client.delete_item(
        TableName=WEEKLY_TABLE,
        Key={
            'userWeeklyId': {'S': userWeeklyId}
        }
    )
    return jsonify(res)


@app.route("/monthly/delete/<string:userMonthlyId>")
def delete_monthly(userMonthlyId):
    res = client.delete_item(
        TableName=MONTHLY_TABLE,
        Key={
            'userMonthlyId': {'S': userMonthlyId}
        }
    )
    return jsonify(res)


@app.route("/yearly/delete/<string:userYearlyId>")
def delete_yealy(userYearlyId):
    res = client.delete_item(
        TableName=YEARLY_TABLE,
        Key={
            'userYearlyId': {'S': userYearlyId}
        }
    )
    return jsonify(res)


######### Utility functions ##############

def add_postfix_date(date):
    this_month = datetime.today().strftime('%b')
    postfix_date = {1: 'st', 21: 'st', 31: 'st', 2: 'nd', 22: 'nd', 3: 'rd', 23: 'rd'}

    ret_date = str(date) + postfix_date.get(date, 'th') + ' ' + this_month + '.'

    return ret_date

def add_postfix_date_month(date):
    this_month = datetime.strptime(date, '%Y%m%d').strftime('%b')
    postfix_date = {1: 'st', 21: 'st', 31: 'st', 2: 'nd', 22: 'nd', 3: 'rd', 23: 'rd'}

    ret_date = add_postfix(date) + ' ' + this_month + '.'

    return ret_date


def update_monthly_cost(userMonthlyId, uioRentalPeriod, uimRentalPayDate, uioOtherCostDueDate, uimEmployeePayDate,
                        uioRentalAmount, uioEmployeeNumber, uioEmployeeAmount, uioOtherCost):
    client.update_item(
        TableName=MONTHLY_TABLE,
        Key={
            'userMonthlyId': {'S': userMonthlyId}
        },
        AttributeUpdates={
            'uioRentalPeriod': {'Value': {'S': uioRentalPeriod}, 'Action': 'PUT'},
            'uimRentalPayDate': {'Value': {'N': uimRentalPayDate}, 'Action': 'PUT'},
            'uioOtherCostDueDate': {'Value': {'N': uioOtherCostDueDate}, 'Action': 'PUT'},
            'uimEmployeePayDate': {'Value': {'N': uimEmployeePayDate}, 'Action': 'PUT'},
            'uioRentalAmount': {'Value': {'N': uioRentalAmount}, 'Action': 'PUT'},
            'uioEmployeeNumber': {'Value': {'N': uioEmployeeNumber}, 'Action': 'PUT'},
            'uioEmployeeAmount': {'Value': {'N': uioEmployeeAmount}, 'Action': 'PUT'},
            'uioOtherCost': {'Value': {'N': uioOtherCost}, 'Action': 'PUT'}

        }
    )


def send_message(userId, blockName, contents):
    logger.debug('send_message')

    base_url = 'https://api.chatfuel.com/bots/'
    bot_id = BOT_ID + '/'
    token = TOKEN
    chatfuel_message_tag = 'NON_PROMOTIONAL_SUBSCRIPTION'
    contents_string = ""

    if isinstance(contents, dict):
        for i in contents.keys():
            logger.debug(contents)
            logger.debug(contents[i])
            contents_string = '&' + contents_string + i + '=' + contents[i]
    else:
        return jsonify({'message': 'contents are not defined'}), 500

    target = base_url + bot_id + 'users/' + userId + '/send?chatfuel_token=' + token + '&chatfuel_message_tag=' + \
             chatfuel_message_tag + '&chatfuel_block_name=' + blockName + contents_string
    headers = {'content-type': 'application/json'}

    logger.debug("request url is %s", target)

    try:
        # remove in prodution
        logger.debug("request open start")
        response = requests.post(target, data='', headers=headers)
        # remove in prodution
        logger.info("response is %s ", response)
        # remove in prodution
        logger.info("Message post result [%s]", response.text)
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)

    return jsonify({'action': 'Successfully sent'})


def put_user(userId, first_name, last_name, gender, chatfuel_userId, source, profile_pic_url, locale, timezone, ref,
             longitude, latitude, uioNotiSales, uioNotiLedger):
    user_table = dynamodb.Table(USERS_TABLE)
    user_table.put_item(
        Item={
            'userId': userId,
            'first_name': first_name,
            'last_name': last_name,
            'gender': gender,
            'chatfuel_userId': chatfuel_userId,
            'source': source,
            'profile_pic_url': profile_pic_url,
            'locale': locale,
            'timezone': timezone,
            'longitude': longitude,
            'latitude': latitude,
            'ref': ref,
            'registration_date': datetime.utcnow().isoformat(),
            'dailyInputCheck': False ,
            'noti':{'uioNotiSales':uioNotiSales, 'uioNotiLedger':uioNotiLedger}
        }
    )


def put_daily(userId, uimDailySales, uimDailyBuying, todayDate):
    today = todayDate.strftime('%Y%m%d')
    userDailyId = userId + today

    isExist = False

    resp = client.get_item(
        TableName=DAILY_TABLE,
        Key={
            'userDailyId': {'S': userDailyId}
        }
    )

    item = resp.get('Item')
    if item:
        isExist = True

    client.put_item(
        TableName=DAILY_TABLE,
        Item={
            'userDailyId': {'S': userDailyId},
            'uimDailySales': {'N': uimDailySales},
            'uimDailyBuying': {'N': uimDailyBuying},
            'created_at': {'S': datetime.utcnow().isoformat()}
        }
    )

    return isExist


def put_weekly(userId, uimDailySales, uimDailyBuying, todayDate):
    this_week = todayDate.strftime('%Y%W')
    userWeeklyId = userId + this_week

    client.update_item(
        TableName=WEEKLY_TABLE,
        Key={
            'userWeeklyId': {'S': userWeeklyId}
        },
        AttributeUpdates={
            'cvWeeklySales': {
                'Value': {'N': uimDailySales},
                'Action': 'ADD'
            },
            'cvWeeklyBuying': {
                'Value': {'N': uimDailyBuying},
                'Action': 'ADD'
            },
            'updated_at': {
                'Value': {'S': datetime.utcnow().isoformat()},
                'Action': 'PUT'
            }
        }
    )


def put_monthly(userId, uimDailySales, uimDailyBuying, todayDate):
    this_month = todayDate.strftime('%Y%m')
    userMonthlyId = userId + this_month

    client.update_item(
        TableName=MONTHLY_TABLE,
        Key={
            'userMonthlyId': {'S': userMonthlyId}
        },
        AttributeUpdates={
            'cvMonthlySales': {
                'Value': {'N': uimDailySales},
                'Action': 'ADD'
            },
            'cvMonthlyBuying': {
                'Value': {'N': uimDailyBuying},
                'Action': 'ADD'
            },
            'updated_at': {
                'Value': {'S': datetime.utcnow().isoformat()},
                'Action': 'PUT'
            }
        }
    )


def put_yearly(userId, uimDailySales, uimDailyBuying, todayDate):
    this_year = todayDate.strftime('%Y')
    userYearlyId = userId + this_year

    client.update_item(
        TableName=YEARLY_TABLE,
        Key={
            'userYearlyId': {'S': userYearlyId}
        },
        AttributeUpdates={
            'cvYearlySales': {
                'Value': {'N': uimDailySales},
                'Action': 'ADD'
            },
            'cvYearlyBuying': {
                'Value': {'N': uimDailyBuying},
                'Action': 'ADD'
            },
            'updated_at': {
                'Value': {'S': datetime.utcnow().isoformat()},
                'Action': 'PUT'
            }
        }
    )


def update_dailyInputCheck(userId, dailyInputCheck):
    client.update_item(
        TableName=USERS_TABLE,
        Key={
            'userId': {'S': userId}
        },
        UpdateExpression='SET #field = :val',
        ExpressionAttributeNames={"#field": 'dailyInputCheck'},
        ExpressionAttributeValues={':val': {'BOOL': dailyInputCheck}}
    )


# 28일 이후면 오류 발생할 수 있음
def cal_payment_date(input_date):
    if date.today() > date.today().replace(day=input_date):
        return (date.today() + relativedelta(months=+1)).replace(day=input_date).strftime('%d %b')
    else:
        return date.today().replace(day=input_date).strftime('%d %b')


def cal_next_payment_date(uimRentalPayDate, uimEmployeePayDate, uioOtherCostDueDate):
    date_list = [uimRentalPayDate, uimEmployeePayDate, uioOtherCostDueDate]
    date_list.sort()

    today_date = date.today()

    for payment_date in date_list:
        temp = date.today().replace(day=payment_date)
        if temp > today_date:
            return payment_date, temp.strftime('%d %b')

    return payment_date, (date.today().replace(day=date_list[0]) + relativedelta(months=+1)).strftime('%d %b')


def daily_average(userId):
    today = datetime.now().strftime('%Y%m%d')
    this_month = datetime.now().strftime('%Y%m')
    resp = client.scan(
        TableName=DAILY_TABLE,
        ScanFilter={
            "userDailyId":{
                "AttributeValueList":[ {"S":userId+this_month} ],
                "ComparisonOperator": "BEGINS_WITH"
            }
        }
    )
    item = resp.get('Items')
    if not item:
        return jsonify({'message': 'No data of this month'})
    else:
        start_date = this_month + '01'

    return calculate_average(item, start_date, today)


def calculate_average(item, start_date, end_date):
    input_count = 0
    sum_sales = 0
    sum_buying = 0

    logger.debug('===================calculate_average=========================')
    logger.debug('start_date [%s]  ::: end_date  [%s]', start_date, end_date)
    logger.debug(item)

    for i in item:
        input_count = input_count + 1
        sum_sales = sum_sales + int(i.get('uimDailySales').get('N'))
        sum_buying = sum_buying + int(i.get('uimDailyBuying').get('N'))

    date_range = monthrange(int(datetime.now().strftime('%Y')), int(datetime.now().strftime('%m')))
    # tdelta = datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(end_date, '%Y%m%d')
    return {'input_count': input_count,
            'sum_sales': sum_sales,
            'sum_buying': sum_buying,
            'avg_sales': int(sum_sales / input_count),
            'avg_buying': int(sum_buying / input_count),
            'avg_profit': int(sum_sales / input_count) - int(sum_buying / input_count),
            'date_range': date_range,
            'temp': [int(datetime.now().strftime('%Y')), int(datetime.now().strftime('%m'))],
            'left_days': date_range[1] - int(datetime.now().strftime('%d'))
            }


def calculate_average_back(item, start_date, end_date):
    input_count = 0
    date_count = 0
    sum_sales = 0
    sum_buying = 0

    logger.debug('===================calculate_average=========================')
    logger.debug('start_date [%s]  ::: end_date  [%s]', start_date, end_date)
    logger.debug(item)

    while start_date <= end_date:
        temp = item.get(start_date)
        date_count = date_count + 1

        if temp:
            input_count = input_count + 1
            sum_sales = sum_sales + int(temp.get('M').get('uimDailySales').get('N'))
            sum_buying = sum_buying + int(temp.get('M').get('uimDailyBuying').get('N'))

        start_date = (datetime.strptime(start_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')

    date_range = monthrange(int(datetime.now().strftime('%Y')), int(datetime.now().strftime('%m')))
    # tdelta = datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(end_date, '%Y%m%d')
    return {'input_count': input_count,
            'sum_sales': sum_sales,
            'sum_buying': sum_buying,
            'avg_sales': int(sum_sales / input_count),
            'avg_buying': int(sum_buying / input_count),
            'avg_profit': int(sum_sales / input_count) - int(sum_buying / input_count),
            'date_count': date_count,
            'date_range': date_range,
            'temp': [int(datetime.now().strftime('%Y')), int(datetime.now().strftime('%m'))],
            'left_days': date_range[1] - int(datetime.now().strftime('%d'))
            }
# dailiyInputCheck 매일 자정에 초기화
def resetDailyInputCheck(event, context):
    scan_response = client.scan(
        TableName=USERS_TABLE
    )
    targetList = []
    for i in scan_response['Items']:
        # test start
        if i['dailyInputCheck']:
            targetList.append(i['userId'])
        #test end

        update_response = client.update_item(
            TableName=USERS_TABLE,
            Key={
                'userId': {'S': i['userId']['S']}
            },
            AttributeUpdates={
                'dailyInputCheck': {'Value': {'BOOL': False}, 'Action': 'PUT'}
            }
        )
    # test start
    client.put_item(
        TableName=CONFIG_TABLE,
        Item={
            'configKey': {'S': 'dailyInputTarget'},
            'targetList': {'L': targetList},
        }
    )
    # test end

    # ledger 통계 보내기
    configTable = dynamodb.Table(CONFIG_TABLE)
    response = configTable.query(
        KeyConditionExpression=Key('configKey').eq('statistics')
    )
    statisticsList = response['Items'][0]['statistics']
    isExist = False
    date = (datetime.now(istTimeZone) - timedelta(days=1)).strftime('%Y%m%d')
    message = ''
    for index, item in enumerate(statisticsList):
        if date in item:
            # 기존 날짜 존재
            isExist = True
            message = 'date: ' + date
            statisticsObj = item[date]
            for messageItem in statisticsObj:
                message += ': ' + str(messageItem)
            break

    if STAGE == 'prod':
        send_slack_notification(message)


@app.route("/weather/<string:userId>")
def get_weather(userId):
    # user의 위도 경도 호출
    # resp = client.get_item(
    #     TableName=DAILY_TABLE,
    #     Key={
    #         'userId': { 'S': userId }
    #     }
    # )

    # item = resp.get('Item')
    # if not item:
    #     return jsonify({'error': 'User does not exist'}), 404

    # longitude = item.get('longitude').get('S')
    # latitude = item.get('latitude').get('S')
    longitude = '28.451850'
    latitude = '77.08684'

    # yahoo api 호출
    yql = 'https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20weather.forecast%20where%20woeid%20in%20(SELECT%20woeid%20FROM%20geo.places%20WHERE%20text%3D%22(' + longitude + ',' + latitude + ')%22)%20AND%20u=%27c%27&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys'

    try:
        response = requests.get(yql, verify=True)
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)


    # AQI
    aqiUrl = 'https://api.breezometer.com/air-quality/v2/current-conditions?lat=28.451850&lon=77.08684&key=7740b958325645ad999516d78f9072de&features=local_aqi'
    try:
        responseAqi = requests.get(aqiUrl, verify=True)
    except HTTPError as e:
        logger.error("AQI Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("AQI Server connection failed: %s", e.reason)

    aqi = responseAqi.json()['data']['indexes']['ind_cpcb']['aqi']

    jsonify(response.json()['query']['results']['channel']['item']['forecast'][0])
    forecast = response.json()['query']['results']['channel']['item']['forecast']
    forecast_dict = {
        "set_attributes": {
            "weatherDay1": forecast[0]['day'],
            "weatherDate1": forecast[0]['date'],
            "weatherDay1High": forecast[0]['high'],
            "weatherDay1Low": forecast[0]['low'],
            "weatherDay1Text": forecast[0]['text'],
            "weatherDay2": forecast[1]['day'],
            "weatherDate2": forecast[1]['date'],
            "weatherDay2High": forecast[1]['high'],
            "weatherDay2Low": forecast[1]['low'],
            "weatherDay2Text": forecast[1]['text'],
            "weatherDay3": forecast[2]['day'],
            "weatherDate3": forecast[2]['date'],
            "weatherDay3High": forecast[2]['high'],
            "weatherDay3Low": forecast[2]['low'],
            "weatherDay3Text": forecast[2]['text'],
            "aqiValue": "AQI " + str(aqi) + " (" + datetime.now(istTimeZone).strftime('%Y%m%d %I%p') + ")"
        }
    }

    return jsonify(forecast_dict)

@app.route("/ledger/add", methods=['POST'])
def addLedger():
    print(request.form)
    userId = request.form['messenger user id']
    customerName = request.form['uioCustomerName']
    productAmount = request.form['uioProductAmount']

    ledgerTable = dynamodb.Table(LEDGER_TABLE)

    response = ledgerTable.query(
        KeyConditionExpression=Key('userLedgerId').eq(userId)
    )
    count = response['Count']
    item = response['Items']
    if count == 0:
        ledgerTable.put_item(
            Item={
                'userLedgerId': userId,
                'activeLedgers': [{
                    'index': count + 1,
                    'customerName': customerName,
                    'productAmount': productAmount,
                    'date': datetime.now(istTimeZone).strftime('%Y%m%d'),
                }]
            }
        )
    else:
        ledgerAppended = item[0]['activeLedgers']
        ledgerToAdd = {
            'index': count + 1,
            'customerName': customerName,
            'productAmount': productAmount,
            'date': datetime.now(istTimeZone).strftime('%Y%m%d'),
        }
        ledgerAppended.append(ledgerToAdd)
        ledgerTable.update_item(
            Key={
                'userLedgerId': userId,
            },
            UpdateExpression='SET activeLedgers = :val1',
            ExpressionAttributeValues={
                ':val1': ledgerAppended
            }
        )

    update_statistics(datetime.now(istTimeZone).strftime('%Y%m%d'), 'ledgerAdd')

    return jsonify({})

@app.route("/ledger/list/<string:userId>")
def getLedgerList(userId):
    ledgerTable = dynamodb.Table(LEDGER_TABLE)

    response = ledgerTable.query(
        KeyConditionExpression=Key('userLedgerId').eq(userId)
    )
    items = response['Items'][0]['activeLedgers']
    responseString = ''
    if len(items) == 0:
        return jsonify({
            "messages":[ {"text": "No list" }]
        })
    for idx, x in enumerate(items):
        responseString +='#' + str(idx + 1) + ' Date: ' + add_postfix_date_month(x['date'])
        responseString += '\nName: ' + x['customerName']
        responseString += '\nProduct Amount: ' + x['productAmount']
        responseString += '\n\n'

    return jsonify({
        "messages":[ {"text": responseString }]
    })

@app.route("/ledger/<string:userId>/<string:ledgerIndex>")
def getLedger(userId, ledgerIndex):
    ledgerTable = dynamodb.Table(LEDGER_TABLE)

    response = ledgerTable.query(
        KeyConditionExpression=Key('userLedgerId').eq(userId)
    )
    items = response['Items'][0]['activeLedgers']
    responseString = ''
    if len(items) == 0:
        return jsonify({
            "messages":[ {"text": "No list" }]
        })

    item = items[int(ledgerIndex) - 1]

    responseString +='#' + ledgerIndex + ' Date: ' + add_postfix_date_month(item['date'])
    responseString += '\nName: ' + item['customerName']
    responseString += '\nProduct Amount: ' + item['productAmount']
    responseString += '\n\n'

    return jsonify({
        "messages":[ {"text": responseString }]
    })

@app.route("/ledger/delete", methods=['POST'])
def deleteLedger():
    userId = request.form['messenger user id']
    indexToDelete = request.form['uioIndexToDelete']
    ledgerTable = dynamodb.Table(LEDGER_TABLE)

    response = ledgerTable.query(
        KeyConditionExpression=Key('userLedgerId').eq(userId)
    )
    activeLedgers = response['Items'][0]['activeLedgers']
    inactiveLedgers = []
    if 'inactiveLedgers' in response['Items'][0]:
        inactiveLedgers = response['Items'][0]['inactiveLedgers']
    
    inactiveLedgers.append(activeLedgers[int(indexToDelete) - 1])
    del(activeLedgers[int(indexToDelete) - 1])

    ledgerTable.update_item(
        Key={
            'userLedgerId': userId,
        },
        UpdateExpression='SET activeLedgers = :val1, inactiveLedgers = :val2',
        ExpressionAttributeValues={
            ':val1': activeLedgers,
            ':val2': inactiveLedgers,
        }
    )

    update_statistics(datetime.now(istTimeZone).strftime('%Y%m%d'), 'ledgerEdit')

    return jsonify({
        "messages":[ {"text": "Ledger deleted" }]
    })

@app.route("/ledger/edit", methods=['POST'])
def editLedger():
    userId = request.form['messenger user id']
    indexToEdit = request.form['uioIndexToEdit']
    ledgerEditAmount = request.form['uioLedgerEditAmount']
    ledgerTable = dynamodb.Table(LEDGER_TABLE)

    response = ledgerTable.query(
        KeyConditionExpression=Key('userLedgerId').eq(userId)
    )
    activeLedgers = response['Items'][0]['activeLedgers']
    selectedLedger = activeLedgers[int(indexToEdit) - 1]
    updateExpression = 'SET activeLedgers[' + str((int(indexToEdit) - 1)) + '].productAmount = :val1'
    ledgerTable.update_item(
        Key={
            'userLedgerId': userId,
        },
        UpdateExpression=updateExpression,
        ExpressionAttributeValues={
            ':val1': ledgerEditAmount
        }
    )

    update_statistics(datetime.now(istTimeZone).strftime('%Y%m%d'), 'ledgerDelete')

    return jsonify({
        "messages":[ {"text": "ledger updated" }]
    })

def update_statistics(date, key):
    configTable = dynamodb.Table(CONFIG_TABLE)
    response = configTable.query(
        KeyConditionExpression=Key('configKey').eq('statistics')
    )
    statisticsList = response['Items'][0]['statistics']
    updatedstatisticsList = statisticsList
    # 날짜 탐색하면서 데이터 있는지 확인
    isExist = False
    for index, item in enumerate(statisticsList):
        print(item)
        if date in item:
            # 기존 날짜 존재
            updatedStatisticsItemList = accumulate_statistics(item[date], key)
            updatedstatisticsList[index] = {date: updatedStatisticsItemList}
            isExist = True
            break
        
    # 날짜 데이터 없음 생성 하기
    if not isExist:
        newStatisticsItemList = accumulate_statistics([{'ledgerAdd':0}, {'ledgerDelete':0}, {'ledgerEdit':0}, ], key)
        updatedstatisticsList.append({date: newStatisticsItemList})
    

    configTable.update_item(
        Key={
            'configKey': 'statistics'
        },
        UpdateExpression='SET statistics = :val1',
        ExpressionAttributeValues={
            ':val1': updatedstatisticsList
        }
    )
    return jsonify()

def accumulate_statistics(dict, keyToUpdate):
    for index, item in enumerate(dict):
        if keyToUpdate in item:
            dict[index][keyToUpdate] = dict[index][keyToUpdate] + 1
            return dict

def send_slack_notification(text):
    statisticsUrl = 'https://hooks.slack.com/services/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'
    payload = '{"text": "' + text + '"}'
    headers = {'content-type': 'application/json'}
    response = requests.post(statisticsUrl, data=payload, headers=headers)

################# test source start ##########################

# 전날 dailyInput 한 사용자에게만 broadcasting
# def broadcastingSendDailyInput(event, context):
#     scan_response = client.scan(
#         TableName=USERS_TABLE
#     )
#     for i in scan_response['Items']:
#         print(i['userId']['S'])
#         if i['dailyInputCheck']:
#             pass
#         update_response = client.update_item(
#             TableName=USERS_TABLE,
#             Key={
#                 'userId': {'S': i['userId']['S']}
#             },
#             AttributeUpdates={
#                 'dailyInputCheck': {'Value': {'BOOL': False}, 'Action': 'PUT'}
#             }
#         )
    

@app.route("/test/statistics")
def test_statistics():
    configTable = dynamodb.Table(CONFIG_TABLE)
    emptyArray = [{'20181128':[{'ledgerAdd':0}, {'ledgerDelete':0}, {'ledgerEdit':0}, ]}]

    configTable.put_item(
        Item={
            'configKey': 'statistics',
            'statistics': emptyArray
        }
    )

@app.route("/test/daily/<string:userId>/<string:uimDailySales>/<string:uimDailyBuying>/<string:testDate>",
           methods=['GET'])
def test_create_daily(userId, uimDailySales, uimDailyBuying, testDate):
    testDateObj = datetime.strptime(testDate, '%Y%m%d')
    isExist = put_daily(userId, uimDailySales, uimDailyBuying, testDateObj)

    if not isExist:
        # weekly 합산 처리
        put_weekly(userId, uimDailySales, uimDailyBuying, testDateObj)

        # monthly 합산 처리
        put_monthly(userId, uimDailySales, uimDailyBuying, testDateObj)

        # yearly 합산 처리
        put_yearly(userId, uimDailySales, uimDailyBuying, testDateObj)

    # 해당 블럭 값만 변경이 되는 건지 아니면 전체 변수가 변경되는 건지 확인 필요
    # 리포트 호출할 때 JSON API 호출하는 형태가 나을까??
    return jsonify({})


@app.route("/test/cal_next_payment_date/<int:uimRentalPayDate>/<int:uimEmployeePayDate>/<int:uioOtherCostDueDate>")
def test_cal_next_payment_date(uimRentalPayDate, uimEmployeePayDate, uioOtherCostDueDate):
    return cal_next_payment_date(uimRentalPayDate, uimEmployeePayDate, uioOtherCostDueDate)


@app.route("/test/daily/<string:userId>")
def test_daily_put(userId):
    put_daily('1950214548368383', '100', '100')
    return jsonify({'message': 'success'})


@app.route("/test/average/<string:userId>")
def test_average(userId):
    today = datetime.now().strftime('%Y%m%d')
    this_month = datetime.now().strftime('%Y%m')
    resp = client.scan(
        TableName=DAILY_TABLE,
        ScanFilter={
            "userDailyId":{
                "AttributeValueList":[ {"S":userId+this_month} ],
                "ComparisonOperator": "BEGINS_WITH"
            }
        }
    )
    logger.debug(resp)
    item = resp.get('Items')
    logger.debug(item)
    if not item:
        return jsonify({'message': 'No data of this month'})
    else:
        start_date = this_month + '01'

    return jsonify(calculate_average(item, start_date,today))

@app.route("/test/cost/<string:userMonthlyId>/<string:uimRentalPayDate>/<string:uioOtherCostDueDate>/<string:uimEmployeePayDate>")
def test_cost(userMonthlyId, uimRentalPayDate, uioOtherCostDueDate, uimEmployeePayDate):
    update_monthly_cost(userMonthlyId, 'once a month', uimRentalPayDate, uioOtherCostDueDate, uimEmployeePayDate,
                        '10000', '1', '10000', '10000')

    return jsonify({})

@app.route("/test/monthly/migrate/<string:fromMonth>/<string:toMonth>")
def test_monthly_migrate(fromMonth, toMonth):
    monthly_table = dynamodb.Table(MONTHLY_TABLE)
    scan_response = monthly_table.scan()
    for i in scan_response['Items']:
        if  i['userMonthlyId'][-6:] == fromMonth:
            print(i)
            try:
                monthly_table.put_item(
                   Item={
                        'userMonthlyId': i['userMonthlyId'][:16] + toMonth,
                        'cvMonthlyBuying': 0,
                        'cvMonthlySales': 0,
                        'updated_at': datetime.utcnow().isoformat(),
                        'uimEmployeePayDate': i['uimEmployeePayDate'],
                        'uimRentalPayDate': i['uimRentalPayDate'],
                        'uioEmployeeAmount': i['uioEmployeeAmount'],
                        'uioEmployeeNumber': i['uioEmployeeNumber'],
                        'uioOtherCost': i['uioOtherCost'],
                        'uioOtherCostDueDate': i['uioOtherCostDueDate'],
                        'uioRentalAmount': i['uioRentalAmount'],
                        'uioRentalPeriod': i['uioRentalPeriod']
                    }
                )
            except KeyError:
                logger.info('keys not exist')
    
    return jsonify({})


# dynamodb resource 를 이용한 데일리 업데이트
def put_daily2(userId, uimDailySales, uimDailyBuying, todayDate):
    today = todayDate.strftime('%Y%m%d')
    userDailyId = userId + today

    isExist = False

    daily_table = dynamodb.Table(DAILY_TABLE)

    resp = daily_table.get_item(
        Key={'userDailyId':userDailyId}
    )

    item = resp.get('Item')
    if item:
        isExist = True

    daily_table.put_item(
        Item={
            'userDailyId': userDailyId,
            'uimDailySales': uimDailySales,
            'uimDailyBuying': uimDailyBuying,
            'created_at': datetime.utcnow().isoformat()
        }
    )

    return isExist

@app.route("/monthly/this/<string:userId>")
def get_this_montly_report(userId):
    now = datetime.now()
    return jsonify({
        "messages":[{"text": get_montly_report(userId,now)}]
    })


@app.route("/monthly/previous/<string:userId>")
def get_previous_montly_report(userId):
    now = datetime.now() - relativedelta(months=1)
    now = now.replace(day=(monthrange(int(now.strftime('%Y')), int(now.strftime('%m')))[1]))
    return jsonify({
        "messages":[{"text": get_montly_report(userId,now)}]
    })


def get_montly_report(userId, now):

    today = now.strftime('%Y%m%d')
    this_month = now.strftime(('%Y%m'))
    this_month_text = now.strftime('%b')
    start_date_month = this_month + '01'

    dates = monthcalendar(int(now.strftime('%Y')), int(now.strftime('%m')))
    message = now.strftime('%b') + ' Sales Report\n' + add_postfix(start_date_month[6:]) + \
              ' ' + this_month_text + ' ~ ' + add_postfix(today[6:8]) + ' ' + this_month_text + '\n' +\
              '\nWeek \n : Buying | Sales | Profit\n'

    daily_table = dynamodb.Table(DAILY_TABLE)

    fe = Key('userDailyId').between(userId + start_date_month, userId + today)
    response = daily_table.scan(
        FilterExpression=fe,
    )
    temp = {}
    for i in response['Items']:
        logger.debug(i)
        temp[i['userDailyId']] = [i['uimDailySales'], i['uimDailyBuying']]

    logger.debug(temp)

    total_sales = 0
    total_buying = 0

    for week_days in dates:
        logger.debug(week_days)
        sum_sales = 0
        sum_buying = 0
        lastday_of_week = 0
        uncountable_days = 0


        for day in week_days :
            logger.debug(day)
            if day == 0:
                uncountable_days = uncountable_days + 1
            elif day < 10:
                daily_id = userId + this_month + '0' + str(day)
                lastday_of_week = day
            else:
                daily_id = userId + this_month + str(day)
                lastday_of_week = day

            if day > 0 and (daily_id in temp):

                add_sales = temp[daily_id][0]
                add_buying = temp[daily_id][1]
            else:
                add_sales = 0
                add_buying = 0

            sum_sales = sum_sales + add_sales
            sum_buying = sum_buying + add_buying

        text_week = 'W' + (now.replace(day=lastday_of_week)).strftime('%W')
        if uncountable_days > 0 :
            text_week = text_week + '(%ddays)'%(7-uncountable_days)

        text_week = text_week + '\n'

        message = message + text_week + ' :%6d | %6d | %6d'%( sum_sales, sum_buying, (sum_sales - sum_buying)) + '\n'

        total_sales = total_sales + sum_sales
        total_buying = total_buying + sum_buying

        logger.debug(message)
    message = message + '\n' + this_month_text + '\n' + \
              ' :%6d | %6d | %6d'%(total_sales, total_buying, (total_sales - total_buying)) + \
              '\n\nNet Profit\n= Profit - Monthly Cost\n\n'
    monthly_table = dynamodb.Table(MONTHLY_TABLE)

    fe = Key('userMonthlyId').eq(userId + this_month)
    res = monthly_table.scan(
        FilterExpression=fe,
    )

    logger.debug(res)

    item_monthly = res.get('Items')
    if not item_monthly:
        total_cost = 0
    else:
        total_cost = item_monthly[0].get('uioRentalAmount') + item_monthly[0].get('uioEmployeeAmount') + item_monthly[0].get('uioOtherCost')

    message = message + '%drs\n= %d - %d'%((total_sales - total_buying - total_cost),(total_sales - total_buying), total_cost )

    logger.debug(message)

    return message

@app.route("/daily/<string:update_date>", methods=['POST'])
def update_daily(update_date):
    userId = request.form['messenger user id']
    if not userId:
        return jsonify({'error': 'Please provider userId'}), 400

    uimDailySales = request.form['uimDailySales']
    uimDailyBuying = request.form['uimDailyBuying']

    logger.info('daily_input|' + userId + '|' + uimDailySales + '|' + uimDailyBuying)

    put_daily(userId, uimDailySales, uimDailyBuying, datetime.strptime(update_date,'%Y%m%d'))

    # 일입력 체크
    update_dailyInputCheck(userId, True)

    return jsonify({})

# noti update
@app.route("/noti/<string:userId>", methods=['POST'])
def put_noti(userId):

    user_table = dynamodb.Table(USERS_TABLE)

    resp = user_table.get_item(
        Key={'userId':userId}
    )
    logger.info(resp)

    uioNotiSales = request.form['uioNotiSales']
    uioNotiLedger = request.form['uioNotiLedger']

    item = resp.get('Item')
    if not item:
        return jsonify({})

    resp = user_table.update_item(
        Key = {
            'userId':userId
        },
        UpdateExpression = "set noti = :noti, created_at= :created_at",
        ExpressionAttributeValues={
            ':noti': {'uioNotiSales':uioNotiSales, 'uioNotiLedger': uioNotiLedger},
            ':created_at': datetime.utcnow().isoformat()
        }
    )

    return jsonify({})


@app.route("/noti/ledger/broadcast", methods=['GET'])
def broadcast_ledger_noti():
    hour = datetime.now(istTimeZone).strftime('%H')
    logger.debug(hour)
    user_table = dynamodb.Table(USERS_TABLE)

    fe = Attr('noti.uioNotiLedger').eq(hour)
    resp = user_table.scan(
        FilterExpression=fe,
    )

    logger.debug(resp)

    items = resp.get('Items')
    if not items:
        return jsonify({})

    ret = []

    for i in items:
        logger.debug(i['userId'])
        send_message(i['userId'], 'ListofLedger', {})
        ret.append(i['userId'])

    return jsonify({'listOfUser':ret})


@app.route("/noti/sales/broadcast", methods=['GET'])
def broadcast_sales_noti():
    hour = datetime.now(istTimeZone).strftime('%H')
    logger.debug(hour)
    user_table = dynamodb.Table(USERS_TABLE)

    fe = Attr('noti.uioNotiSales').eq(hour)
    resp = user_table.scan(
        FilterExpression=fe,
    )

    logger.debug(resp)

    items = resp.get('Items')
    if not items:
        return jsonify({})

    ret = []

    for i in items:
        logger.debug(i['userId'])
        send_message(i['userId'], 'DailyInput_BR', {})
        ret.append(i['userId'])

    return jsonify({'listOfUser':ret})


@app.route("/noti/all", methods=['GET'])
def put_noti_all():

    user_table = dynamodb.Table(USERS_TABLE)

    fe = Attr('noti').not_exists()
    resp = user_table.scan(
        FilterExpression=fe,
    )

    logger.debug(resp)

    items = resp.get('Items')
    if not items:
        return jsonify({})

    ret = []
    logger.debug(resp)

    for i in items:
        logger.debug(' ================== '+i['userId'] + i['first_name'])
        resp = user_table.update_item(
            Key={
                'userId': i['userId']
            },
            UpdateExpression="set noti = :noti",
            ExpressionAttributeValues={
                ':noti': {'uioNotiSales': '21', 'uioNotiLedger': '09'}
            }
        )

        ret.append(i['userId'])

    return jsonify({'listOfUser':ret})

@app.route("/migrate/cost", methods=['GET'])
def migrate_monthly_cost():
    user_table = dynamodb.Table(USERS_TABLE)
    monthly_table = dynamodb.Table(MONTHLY_TABLE)

    fe = Attr('uioRentalPeriod').exists() & Attr('userMonthlyId').contains('201812')
    resp = monthly_table.scan(
        FilterExpression=fe,
    )

    items = resp.get('Items')
    if not items:
        return jsonify({})

    ret = []
    logger.debug(resp)

    for i in items:
        logger.debug(' ============='+ i['userMonthlyId'] + ': '+ str(i))
        resp = user_table.update_item(
            Key={
                'userId':i['userMonthlyId'][:-6]
            },
            UpdateExpression="set cost = :cost",
            ExpressionAttributeValues={
                ':cost': {'uioRentalPeriod': i['uioRentalPeriod'],
                          'uimRentalPayDate': i['uimRentalPayDate'],
                          'uioRentalAmount': i['uioRentalAmount'],
                          'uimEmployeePayDate': i['uimEmployeePayDate'],
                          'uioEmployeeNumber': i['uioEmployeeNumber'],
                          'uioEmployeeAmount': i['uioEmployeeAmount'],
                          'uioOtherCostDueDate': i['uioOtherCostDueDate'],
                          'uioOtherCost': i['uioOtherCost']}
            }
        )
        ret.append(i['userMonthlyId'][:-6])

    return jsonify({'listOfUser':ret})

