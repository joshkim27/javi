# app.py

import os
import urllib

import boto3
import requests

import logging.handlers
from datetime import datetime, timedelta, date
from flask import Flask, jsonify, request
from dateutil.relativedelta import relativedelta
from calendar import monthrange

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

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
BOT_ID = os.environ['BOT_ID']
TOKEN = os.environ['TOKEN']

IS_OFFLINE = os.environ.get('IS_OFFLINE')

if IS_OFFLINE:
    client = boto3.client(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
else:
    client = boto3.client('dynamodb')


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

    put_user(userId, first_name, last_name, gender, chatfuel_userId, source, profile_pic_url, locale, timezone, ref)

    return jsonify({"text": "Welcome to the Javi Rockets!"})


@app.route("/daily", methods=['POST'])
def create_daily():
    userId = request.form['messenger user id']
    if not userId:
        return jsonify({'error': 'Please provider userId'}), 400

    uimDailySales = request.form['uimDailySales']
    uimDailyBuying = request.form['uimDailyBuying']

    todayDate = datetime.today()

    isExist = put_daily(userId, uimDailySales, uimDailyBuying, todayDate)

    # 일입력 체크
    update_dailyInputCheck(userId, True)

    # daily 없는 경우만 합산처리 하는 걸로
    # daily 가 누적으로 바뀌는 경우 체크 없애야 함
    if not isExist :
        # weekly 합산 처리
        put_weekly(userId, uimDailySales, uimDailyBuying, todayDate)

        # monthly 합산 처리
        put_monthly(userId, uimDailySales, uimDailyBuying, todayDate)

        # yearly 합산 처리
        put_yearly(userId, uimDailySales, uimDailyBuying, todayDate)

    return jsonify({})

@app.route("/test/daily/<string:userId>/<string:uimDailySales>/<string:uimDailyBuying>/<string:testDate>", methods=['GET'])
def test_create_daily(userId, uimDailySales, uimDailyBuying, testDate):
    testDateObj = datetime.strptime(testDate, '%Y%m%d')
    put_daily(userId, uimDailySales, uimDailyBuying, testDateObj)

    # weekly 합산 처리
    put_weekly(userId, uimDailySales, uimDailyBuying, testDateObj)

    # monthly 합산 처리
    put_monthly(userId, uimDailySales, uimDailyBuying, testDateObj)

    # yearly 합산 처리
    put_yearly(userId, uimDailySales, uimDailyBuying, testDateObj)

    # 해당 블럭 값만 변경이 되는 건지 아니면 전체 변수가 변경되는 건지 확인 필요
    # 리포트 호출할 때 JSON API 호출하는 형태가 나을까??
    return jsonify({})


@app.route("/monthly/cost", methods=['POST'])
def update_monthly_cost():
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

    update_monthly_cost(userId+this_month, uioRentalPeriod, uimRentalPayDate, uioOtherCostDueDate, uimEmployeePayDate,
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
            'userDailyId': {'S': userId+today}
        }
    )

    item = resp.get('Item')
    if not item:
        return jsonify({'error': 'User does not exist'}), 404

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
            'userWeeklyId': {'S': userId+this_week}
        }
    )

    item_weekly = resp_weekly.get('Item')
    if not item_weekly:
        return jsonify({'error': 'This week data does not exist'}), 404

    # 데이터 없는 경우 처리 필요
    cvWeeklySales = item_weekly.get('cvWeeklySales').get('N')
    cvWeeklyBuying = item_weekly.get('cvWeeklyBuying').get('N')
    cvWeeklyProfit = str(int(cvWeeklySales) - int(cvWeeklyBuying))

    # month period 처리
    this_month = datetime.now().strftime('%Y%m')
    month_start_date = date.today().replace(day=1)
    today = date.today()
    cvMonthPeriod = datetime.strftime(month_start_date, '%d %b') + ' - ' + datetime.strftime(today, '%d %b')
    cvMonth = datetime.now().strftime('%B')

    resp_monthly = client.get_item(
        TableName=MONTHLY_TABLE,
        Key={
            'userMonthlyId': {'S': userId+this_month}
        }
    )

    item_monthly = resp_monthly.get('Item')
    if not item_monthly:
        return jsonify({'error': 'This month data does not exist'}), 404

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


def update_monthly_cost(userMonthlyId, uioRentalPeriod, uimRentalPayDate, uioOtherCostDueDate, uimEmployeePayDate,
                        uioRentalAmount, uioEmployeeNumber, uioEmployeeAmount, uioOtherCost):
    client.update_item(
        TableName=MONTHLY_TABLE,
        Key={
            'userMonthlyId': {'S': userMonthlyId}
        },
        AttributeUpdates={
            'uioRentalPeriod': {'Value': {'S': uioRentalPeriod},'Action': 'PUT'},
            'uimRentalPayDate': {'Value': {'N': uimRentalPayDate},'Action': 'PUT'},
            'uioOtherCostDueDate': {'Value': {'N': uioOtherCostDueDate},'Action': 'PUT'},
            'uimEmployeePayDate': {'Value': {'N': uimEmployeePayDate},'Action': 'PUT'},
            'uioRentalAmount': {'Value': {'N': uioRentalAmount},'Action': 'PUT'},
            'uioEmployeeNumber': {'Value': {'N': uioEmployeeNumber},'Action': 'PUT'},
            'uioEmployeeAmount': {'Value': {'N': uioEmployeeAmount},'Action': 'PUT'},
            'uioOtherCost': {'Value': {'N': uioOtherCost},'Action': 'PUT'}

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



def put_user( userId, first_name, last_name, gender, chatfuel_userId, source, profile_pic_url, locale, timezone, ref):

    client.put_item(
        TableName=USERS_TABLE,
        Item={
            'userId': {'S': userId},
            'first_name': {'S': first_name},
            'last_name': {'S': last_name},
            'gender': {'S': gender},
            'chatfuel_userId': {'S': chatfuel_userId},
            'source': {'S': source},
            'profile_pic_url': {'S': profile_pic_url},
            'locale': {'S': locale},
            'timezone': {'S': timezone},
            'ref': {'S': ref},
            'registration_date': {'S': datetime.utcnow().isoformat()},
            'dailyInputCheck': {'BOOL': False}
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
            return temp.strftime('%d %b')

    return (date.today().replace(day=date_list[0]) + relativedelta(months=+1)).strftime('%d %b')


def daily_average(userId):
    today = datetime.now().strftime('%Y%m%d')
    this_month = datetime.now().strftime('%Y%m')
    resp = client.get_item(
        TableName=DAILY_TABLE,
        Key={
            'userId': {'S': userId}
        }
    )
    item = resp.get('Item')
    if not item:
        return jsonify({'message': 'No data of this month'})
    else:
        start_date = this_month + '01'

    return calculate_average(item, start_date, today)


def calculate_average(item, start_date, end_date):
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


################# test source start ##########################


@app.route("/test/daily/<string:userId>/<string:uimDailySales>/<string:uimDailyBuying>/<string:testDate>", methods=['GET'])
def test_create_daily(userId, uimDailySales, uimDailyBuying, testDate):
    testDateObj = datetime.strptime(testDate, '%Y%m%d')
    isExist = put_daily(userId, uimDailySales, uimDailyBuying, testDateObj)

    if not isExist :
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
def test_daily_average(userId):
    today = datetime.now().strftime('%Y%m%d')
    this_month = datetime.now().strftime('%Y%m')
    resp = client.get_item(
        TableName=DAILY_TABLE,
        Key={
            'userId': {'S': userId}
        }
    )
    item = resp.get('Item')
    if not item:
        return jsonify({'message': 'No data of this month'})
    else:
        start_date = this_month + '01'

    return jsonify(calculate_average(item, start_date, today))


################# test source ################################


def run(event, context):
    response = client.scan(
        TableName=USERS_TABLE
    )

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

    yql = 'https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20weather.forecast%20where%20woeid%20in%20(SELECT%20woeid%20FROM%20geo.places%20WHERE%20text%3D%22(' + longitude + ',' + latitude + ')%22)&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys'
 
    try:
        response = requests.get(yql, verify=True)
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)

    jsonify(response.json()['query']['results']['channel']['item']['forecast'][0])
    forecast = response.json()['query']['results']['channel']['item']['forecast']
    forecast_dict = {
        "weatherDay1": forecast[0]['date'],
        "weatherDay1High": forecast[0]['high'],
        "weatherDay1Low": forecast[0]['low'],
        "weatherDay1Text": forecast[0]['text'],
        "weatherDay2": forecast[1]['date'],
        "weatherDay2High": forecast[1]['high'],
        "weatherDay2Low": forecast[1]['low'],
        "weatherDay2Text": forecast[1]['text']
    }
    return send_message(userId,'WeatherReport', forecast_dict)
    