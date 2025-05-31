import random
import requests
import base64
import os
import sys
import datetime, time
import traceback
import shutil
import pandas as pd
#import openpyxl

import math

import numpy as np

#from pandas import json_normalize

#import xml.etree.ElementTree as ET

import xmltodict
#from flatten_json import flatten

import urllib
from sqlalchemy import create_engine

# Убрать ошибку на ВМ - ModuleNotFoundError: No module named 'sqlalchemy.sql.default_comparator'
import sqlalchemy.sql.default_comparator


import json


import pyodbc
import schedule

#from zeep import Client
import zeep

# from zeep.transports import Transport
# from xml.etree import ElementTree
# class CustomTransport(Transport):
#     def post_xml(self, address, envelope, headers):
#         message = ElementTree.tostring(envelope, encoding="unicode")
#         message = message.replace("&lt;", "<")
#         message = message.replace("&gt;", ">")
#         return self.post(address, message, headers)


#import math
#import pandas_read_xml as pdx

import api_utils

# Для преобразования xml в df #######
# import xml.etree.ElementTree as ET
# import pandas as pd
#
# class XML2DataFrame:
#
#     def __init__(self, xml_data):
#         self.root = ET.XML(xml_data)
#
#     def parse_root(self, root):
#         return [self.parse_element(child) for child in iter(root)]
#
#     def parse_element(self, element, parsed=None):
#         if parsed is None:
#             parsed = dict()
#         for key in element.keys():
#             parsed[key] = element.attrib.get(key)
#         if element.text:
#             parsed[element.tag] = element.text
#         for child in list(element):
#             self.parse_element(child, parsed)
#         return parsed
#
#     def process_data(self):
#         structure_data = self.parse_root(self.root)
#         return pd.DataFrame(structure_data)
# Не подошла - не забирает вложенные: позиции и оплаты
# xml2df = XML2DataFrame(file_xml)
# df_purchase = xml2df.process_data()
# df_purchase.to_excel("df_purchase_NEW.xlsx")
#####################################



# INFO
"""
    Адрес развернутого сервера 'http://192.168.187.4:8090'
    
    ИМПОРТ данных из Crystals.
    Из описания: https://crystals.atlassian.net/wiki/spaces/INT/pages/1646806/SetRetail10+ERP+-+SetRetail10
    Методы веб-сервиса для экспорта чеков:         
    *** За заданный операционный день (getPurchasesByOperDay)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    dateOperDay - date - Операционный день в формате YYYY-MM-DD
    
    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByOperDay('2024-09-03')
    # #print(result)
    # #print(result.decode("utf-8"))
    # file_xml = result.decode("utf-8")

    *** За заданный операционный день c вводом параметров (getPurchasesByOperDay)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    Year -  integer - Год в формате YYYY

    Mobth - string
    Параметр mobth для вызова установки значений месяца должен использоваться именно в таком написании.
    Его наименование не совпадает со словом месяц (month) на английском языке!
    Месяц в текстовом формате:
    JANUARY
    FEBRUARY
    MARCH
    APRIL
    MAY
    JUNE
    JULY
    AUGUST
    SEPTEMBER
    OCTOBER
    NOVEMBER
    DECEMBE

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByOperDay('2024')

    *** За заданный период (getPurchasesByPeriod)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    fromDate - Начало диапазона в формате YYYY-MM-DD
    toDate - Конец диапазона в формате YYYY-MM-DD

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByPeriod('2024-01-01', '2024-09-02')

    *** За заданный период по товару (getPurchasesByPeriodAndProduct)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    fromDate - date - Начало диапазона в формате YYYY-MM-DD
    toDate - date -Конец диапазона в формате YYYY-MM-DD
    goodsCode - string - Код товара
    
    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByPeriodAndProduct('2024-01-01', '2024-09-02', '786')

    *** По заданным параметрам (getPurchasesByParams)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    dateOperDay - date - Операционный день в формате YYYY-MM-DD
    shopNumber - integer - Номер магазина
    cashNumber - integer - Номер кассы
    shiftNumber - integer - Номер смены
    purchaseNumber - integer - Номер чека

    Параметры shopNumber, cashNumber, shiftNumber, purchaseNumber – являются необязательными.
    В зависимости от полноты указания параметров, в ответе будет возвращаться соответствующее количество чеков.

    Кейсы:
    dateOperDay - в отчёт попадают все чеки всех магазинов за операционный день dateOperDay.
    dateOperDay, shopNumber - в отчёт попадают все чеки за операционный день dateOperDay с магазина shopNumber.
    dateOperDay, shopNumber, cashNumber - в отчёт попадают все чеки за операционный день dateOperDay с магазина shopNumber с кассы cashNumber.
    dateOperDay, shopNumber, cashNumber, shiftNumber - в отчёт попадают все чеки смены shiftNumber за операционный день dateOperDay с магазина shopNumber с кассы cashNumber.
    dateOperDay, shopNumber, cashNumber, shiftNumber, purchaseNumber - в отчёт попадает только один конкретный чек под номером purchaseNumber из сменыshiftNumber за операционный день dateOperDay с магазина shopNumber с кассы cashNumber.

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByParams('2024-01-01T15:00:00', 29591)

    *** Экспорт новых, не отправленных чеков за операционный день с вводом параметров (getNewPurchasesByParams)
    Выгружаются все новые чеки за указанный операционный день, либо удовлетворяющие заданным параметрам, если они указаны.

    dateOperDay - date - Операционный день в формате YYYY-MM-DD
    shopNumber - integer - Номер магазина
    cashNumber - integer - Номер кассы
    shiftNumber - integer - Номер смены
    purchaseNumber - integer - Номер чека

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getNewPurchasesByParams('2024-01-01T15:00:00', 1)

    *** Экспорт новых, не отправленных чеков (getNewPurchasesByOperDay) --- !!!! Не тестировал
    В отчёте выгружаются только новые чеки (те которые ещё не забирали).
    Выгружаются все новые чеки за указанный операционный день, либо удовлетворяющие заданным параметрам, если они указаны.

    arrayOfParams - array - Массив параметров по следующему формату:
    [OperDay (DateTime, REQUIRED), shop(Long), cash(Long), shift(Long), number(Long)]

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # param = {'2024-01-01T12:00:00.000',
    #          1,
    #          1,
    #          1,
    #          1
    #          }
    # client = Client(url_c)
    # result = client.service.getNewPurchasesByOperDay(param)
    # ERROR: zeep.exceptions.Fault: java.lang.ClassCastException: com.sun.org.apache.xerces.internal.dom.ElementNSImpl cannot be cast to javax.xml.datatype.XMLGregorianCalendar

    *** Получение новых чеков, которые не отправлялись веб-сервисом (getNewFullPurchasesByOperDay) --- !!!! Не тестировал
    * Минимальный размер массива аргументов метода - 1 (потому что параметр "дата опердня" обязательный).
    * Если требуется пропустить, параметр "номер смены", тогда установите значение null, потому что за номером смены следует номер чека.
    * Если требуется номер чека, массиву допустимо быть длиной 4, потому что за параметром "номер чека" ничего не следует.

    0 - Date - Дата, за которую из операционного дня требуется получить новые чеки
    1 - Long - Номер магазина, от которого из операционного дня следует выбрать новые чеки
    2 - Long - Номер кассы, от которой из операционного дня следует выбрать новые чеки
    3 - Long - Номер смены, от которой из операционного дня следует выбрать новые чеки
    4 - Long - Номер чека, от которой из операционного дня следует выбрать новые чеки

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getNewFullPurchasesByOperDay(['2024-01-01T12:00:00.000'])
    # # ERROR: zeep.exceptions.Fault: java.lang.ClassCastException: com.sun.org.apache.xerces.internal.dom.ElementNSImpl cannot be cast to javax.xml.datatype.XMLGregorianCalendar



    *** ПРОВЕРКА СХЕМ
    # import xmlschema
    # data_schema = xmlschema.XMLSchema('goods-catalog-schema.xsd')
    # data=data_schema.to_dict('goods-catalog.xml')
    # print(data_schema)
        
    # # 2 сопоставить xml схеме
    # import xmlschema
    # import json
    # from xml.etree.ElementTree import ElementTree
    # #my_xsd = '<?xml version="1.0"?> <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"> <xs:element name="note" type="xs:string"/> </xs:schema>'
    # schema = xmlschema.XMLSchema('goods-catalog-schema.xsd')
    # data = json.dumps({'note': 'this is a Note text'})
    # xml = xmlschema.from_json(data, schema=schema, preserve_root=True)
    # ElementTree(xml).write('goods-catalog.xml')    
    
    # # просмотр объекта схема
    # from xmlschema import XMLSchema
    # obj = XMLSchema.meta_schema.decode('goods-catalog-schema.xsd')
    # print(obj)

"""


# Создать словарь из xml
def xml_to_dict(element):
    result = {}
    for child in element:
        if len(child) > 0:
            value = xml_to_dict(child)
        else:
            value = child.text

        key = child.tag
        if key in result:
            if not isinstance(result[key], list):
                result[key] = [result[key]]
            result[key].append(value)
        else:
            result[key] = value

    return result


# Выполнить задания из КИС (прогрузка касс)
def do_getKISaction():
    query_action = "select ID as ActionID, TaskID, SubObjectID, FileName from Crystals_Action where Done is null"
    df_action = api_utils.select_query(query_action, login_sql, password_sql, server_sql, driver_sql, database)

    #print(df_action)
    #now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    api_utils.InsertLog('Получить из КИС активные задания. Всего заданий: ' + str(len(df_action)))

    if not df_action.empty:
        # Отправка каталога (1), Выгрузка кассиров (2), Отправка МРЦ (5)
        df_files = df_action.loc[((df_action['TaskID'] == 1) | (df_action['TaskID'] == 2) | (df_action['TaskID'] == 5))]
        if not df_files.empty:
            send_files_to_crystals(df_files)

        # Отправка марок (3)
        df_mark = df_action.loc[(df_action['TaskID'] == 3)]
        for i, row in df_mark.iterrows():
            ActionID = row['ActionID']
            SubObjectID = row['SubObjectID']

            api_utils.InsertLog('Экспорт. Отправка марок в set mark. ActionID=' + str(ActionID) +
                                ', SubObjectID=' + str(SubObjectID))

            try:
                Error = send_files_to_setmark(SubObjectID)

                # Обновить ответ в КИС
                spCrystals_UpdateAction(ActionID, 3, SubObjectID, None, Error)
            except:
                traceback.print_exc()
                startTime = datetime.datetime.now()
                t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
                log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+', encoding="utf-8")
                log.write(t_log + ' ## Ошибка при отправке марок в set mark')
                traceback.print_exc(file=log)
                log.close()
                time.sleep(1)


# Обновить задание в КИС
def spCrystals_UpdateAction (ActionID, TaskID, SubObjectID, FileName, Error):

    api_utils.InsertLog('Обновить задание в КИС. ActionID=' + str(ActionID) + ', TaskID=' + str(TaskID) +
                        ', SubObjectID=' + str(SubObjectID) + ', FileName=' + str(FileName) + ', Error=' + str(Error))

    conn = pyodbc.connect('Driver=' + odbc_driver + ';'
                          'Server=' + server_sql + ';'
                          'Database=' + database + ';'
                          'UID=' + login_sql + ';'
                          'PWD=' + password_sql + ';'
                          'Trusted_Connection=no;')
    cursor = conn.cursor()

    cursor.execute("exec [dbo].[spCrystals_UpdateAction] " +
                   str(ActionID) + ', ' + str(TaskID) + ', ' +
                   str(SubObjectID) + ", '" + str(FileName) + "', " + str(Error))

    conn.commit()
    conn.close()

    api_utils.InsertLog('Обновить задание в КИС: Успешно.')

    return


# Получить из Crystals продажи за период
def do_getPurchasesByPeriod():
    NowDT = datetime.datetime.now()
    date_end = NowDT.strftime("%Y-%m-%dT%H:%M:%S")
    date_begin = (NowDT - datetime.timedelta(hours=max_hours_getPurchasesByPeriod) - datetime.timedelta(
        minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")  # Вычесть дополнительно 5 минут, чтобы не пропустить продажи

    api_utils.InsertLog('Получить из Crystals продажи за период: ' + str(date_begin) + ' - ' + str(date_end))

    getPurchasesByPeriod(url_srv, date_begin, date_end)


# Получить из Crystals Z-отчёты за операционный день
def do_getZReportsByOperDay():
    operdate = datetime.datetime.now().strftime("%Y-%m-%d")
    #now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    api_utils.InsertLog('Получить из Crystals данные z-отчётов за операционный день: ' + str(operdate))

    getZReportsByOperDay(url_srv, operdate)



# OUT. Отправить файлы из расшаренной папки \\piter-sql\Export\Crystals в SetRetail10. Crystals (getGoodsCatalog)
def send_files_to_crystals(df_action): #ActionID, TaskID, SubObjectID, FileName):
    # Отправляемые файлы должны находиться в корневой папке программы, в папке OutFiles
    dir = './OutFiles'
    if not os.path.exists(dir):
        api_utils.InsertLog('Ошибка, не обнаружена папка OutFiles')
        sys.exit()

    # Получение файлов из расшаренной папки, куда выгружаются xml из базы
    files = (file for file in os.listdir(shared_folder) if os.path.isfile(os.path.join(shared_folder, file)))
    files_count = len(list(files))

    if files_count > 0:
        api_utils.InsertLog('Запуск перемещения xml файлов из расшаренной папки ' + shared_folder + ' в папку с программой')
        api_utils.InsertLog('Всего файлов в папке ' + shared_folder + ': ' + str(files_count))
        f = 0
        files = (file for file in os.listdir(shared_folder) if
                 os.path.isfile(os.path.join(shared_folder, file)))
        for file in files:
            if file.endswith('.xml'):
                file_from = os.path.join(shared_folder, file)
                file_to = dir + '/' + file
                if os.path.isfile(file_from):
                    shutil.move(file_from, file_to)
                    f += 1
        api_utils.InsertLog('Успешно. Файлов перемещено: ' + str(f))

    # Отправить файлы в Crystals. Взять только файлы xml
    allfiles = (file for file in os.listdir(dir) if os.path.isfile(os.path.join(dir, file)))
    for file in allfiles:
        if file.endswith('.xml'):
            api_utils.InsertLog('Найден файл: ' + file)

            # Если есть задание на выгрузку этого файла, то выгружаем
            df_task = df_action.loc[(df_action['FileName'] == file)]
            print(df_task)

            if not df_task.empty:
                file_name = file
                file = dir + '/' + file
                # print(file)

                TaskID = df_task['TaskID'].values[0]
                ActionID = df_task['ActionID'].values[0]
                SubObjectID = df_task['SubObjectID'].values[0]

                # Выгрузка каталога (getGoodsCatalog)
                if TaskID in (1, 5):
                    if TaskID == 1:
                        api_utils.InsertLog('Экспорт. Выгрузка каталога')
                    elif TaskID == 5:
                        api_utils.InsertLog('Экспорт. Выгрузка МРЦ')
                        # МРЦ находится внутри каталога
                        # Ограничения минимальной цены <min-price-restriction>
                        # <min-price-restriction id="minprice0000437" subject-type="GOOD" subject-code="0100129" value="300">

                    Error = send_goods_catalog(url_srv, file_name, file)
                    print('Error=',Error, type(Error))
                    spCrystals_UpdateAction(ActionID, TaskID, SubObjectID, file_name, Error)

                # Выгрузка кассиров (importCashiersWithTi)
                if TaskID == 2:
                    api_utils.InsertLog('Экспорт. Выгрузка кассиров')
                    Error = send_сashiers(url_srv, file_name, file)
                    print(Error, type(Error))
                    spCrystals_UpdateAction(ActionID, TaskID, SubObjectID, file_name, Error)

                #sys.exit()

                # Переместить успешно отправленный файл в архив.
                if Error == 0:
                    # Создание папки для хранения успешно отправленных файлов (архив). Например: Output\23072024
                    dpath = datetime.datetime.now().strftime('%d%m%Y')
                    if os.path.exists(dir + '/Output/' + dpath) == False:
                        if os.path.exists(dir + '/Output/') == False:
                            os.mkdir(dir + '/Output/')
                        os.mkdir(dir + '/Output/' + dpath)

                    file_path = os.path.join(dir, file_name)
                    if os.path.isfile(file_path):
                        os.replace(dir + '/' + file_name, dir + '/Output/' + dpath + '/' + file_name)

                # Отправка через SOAP
                # # СОЗДАТЬ SOAP
                # data = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:plug="http://plugins.products.ERPIntegration.crystals.ru/">
                # <soapenv:Header/>
                # <soapenv:Body>
                #  <plug:getGoodsCatalog>
                #  <goodsCatalogXML>""" + file64 + """</goodsCatalogXML>
                #  </plug:getGoodsCatalog>
                # </soapenv:Body>
                # </soapenv:Envelope>"""
                # print(data)
                # # Отправить SOAP
                # r = requests.post(url, data=data, headers=headers)
                # print(r)
                # print(r.text)


# OUT. Отправка каталога (getGoodsCatalog) в SetRetail10 (TaskID=1)
def send_goods_catalog(url_srv, file_name, file):
    # url интеграции
    url = url_srv + "/SET-ERPIntegration/SET/WSGoodsCatalogImport?wsdl"

    try:
        # Открыть XML и перекодировать в base64
        with open(file, "rb") as file:
            file64 = base64.encodebytes(file.read()).decode("utf-8")

        # Отправить файл на сервис (через zeep)
        client = zeep.Client(url)
        result = client.service.getGoodsCatalog(file64)
        print('result=',result, type(result))
        api_utils.InsertLog('Выгружен ' + file_name + '. Ответ: ' + str(result))
    except:
        traceback.print_exc()
        startTime = datetime.datetime.now()
        t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
        log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+', encoding="utf-8")
        log.write(t_log + ' ## Ошибка при отправке файла\n')
        traceback.print_exc(file=log)
        log.close()
        return 1

    if result:
        return 0
    else:
        return 1


# OUT. Отправка кассиров (importCashiersWithTi) в SetRetail10 (TaskID=2)
def send_сashiers(url_srv, file_name, file):
    # url интеграции
    url = url_srv + "/SET-ERPIntegration/CashiersImport?wsdl"

    error = 1

    # Уникальный идентификатор пакета
    ti = int(random.uniform(1, 100000))

    try:
        # Открыть XML
        with open(file, "rb") as file:
            file_xml = file.read().decode("utf-8")

        #print(file_xml)

        # Отправить файл на сервис (через zeep)
        client = zeep.Client(url)
        result = client.service.importCashiersWithTi(file_xml, ti)


        #result = client.service.importCashiers(file_xml)
        # https://stackoverflow.com/questions/48655638/python-zeep-send-un-escaped-xml-as-content
        # I used this plugin for keeping the characters '<' and '>' in a CDATA element.
        # client = zeep.Client(url, transport=CustomTransport())
        # result = client.service.importCashiersWithTi(file, ti)

        print(result, type(result))
        api_utils.InsertLog('Выгрузили ' + file_name + '. Ответ: ' + str(result))
    except:
        traceback.print_exc()
        startTime = datetime.datetime.now()
        t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
        log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+', encoding="utf-8")
        log.write(t_log + ' ## Ошибка при отправке файла\n')
        traceback.print_exc(file=log)
        log.close()
        return 1

    if result:
        return 0
    else:
        return 1


# OUT. Отправить марки в SetMark (erp/add_mark) (TaskID=3)
def send_files_to_setmark(SubObjectID, Mark = None):
    # Загрузка списка марок из внешней системы
    # https://crystals.atlassian.net/wiki/spaces/SR10SUPPORT/pages/785940525/SetMark+API+ERP

    # Получить сервер set mark из подобъектов
    query_srv_setmark = 'select ServerSetMark from ObjectReg1 where ID = ' + str(SubObjectID)
    url_srv_setmark = api_utils.select_query(query_srv_setmark, login_sql, password_sql, server_sql, driver_sql,
                                             database, isList=True)

    if not url_srv_setmark:
        api_utils.InsertLog('В подобъекте ' + str(SubObjectID) + ' не заполнен сервер set mark')
        return 1
    else:
        url_srv_setmark = url_srv_setmark[0]

    api_utils.InsertLog('url_srv_setmark = ' + str(url_srv_setmark))
    api_utils.InsertLog('Забираем марки из базы')

    # Если указали марку - выгрузить только её
    # filter = " and SubObjectID = " + str(SubObjectID)
    # if Mark:
    #     filter = filter + " and E.MarkCode = '" + str(Mark) + "'"
    #
    # query_mark = "select distinct MarkCode as excise, AlcCode as alcocode, isnull(PackGoodsID,GoodsID) as item " \
    #         " from egais_RestBCode_v3View" \
    #         " where isnull(Quantity,0)<>0 " + filter

    query_mark = "exec spCrystals_LoadPOS_Mark " + str(SubObjectID)

    df_mark_all = api_utils.select_query(query_mark, login_sql, password_sql, server_sql, driver_sql, database, isList=False)

    api_utils.InsertLog('Всего марок: ' + str(len(df_mark_all)))

    if df_mark_all.empty:
        return 1

    er = 0

    # Выгружать по 5000
    batchsize = 5000
    api_utils.InsertLog('Отправляем марки циклом по: ' + str(batchsize))

    for i in range(0, len(df_mark_all), batchsize):
        df_mark = df_mark_all.iloc[i: i + batchsize]

        # Уникальный идентификатор пакета
        id_pack = int(random.uniform(1, 100000))

        dict_mark = {"id": id_pack, "version": 1, "inn": inn, "shop": SubObjectID, "operationType": 1, "productType": 1,
                     "data": df_mark.to_dict(orient='records')}

        file = json.dumps(dict_mark, ensure_ascii=False)

        # Отправка файла с марками в SetMark
        url = url_srv_setmark + "/erp/add_mark"
        headers = {'Content-Type': 'application/json'}
        error = 0

        r = requests.post(url, data=file, headers=headers, verify=False, timeout=200)  #headers=headers,json.dumps(file_dict)
        #print(r)

        api_utils.InsertLog('Отправили ' + str(len(df_mark)) + ' марок. Ответ: ' + str(r.status_code))

        if r.status_code != 200:
            er += 1

    if er > 1:
        er = 1

    return er


# OUT. Обновление кассиров (updateCashiers) в SetRetail10 (TaskID=4)
def update_сashiers(url_srv, file_name, file, tabNum):
    # url интеграции
    url = url_srv + "/SET-Cashiers/CashiersUpdateWS?wsdl"

    error = 1

    try:
        # Открыть XML
        with open(file, "rb") as file:
            file_xml = base64.encodebytes(file.read()).decode("utf-8")

        # print(file_xml)
        # sys.exit()

        # Отправить файл на сервис (через zeep)
        client = zeep.Client(url)
        result = client.service.updateCashiers(file_xml)

        print(result, type(result))
        api_utils.InsertLog('Выгрузили ' + file_name + '. tabNum= ' + str(tabNum) + '. Ответ: ' + str(result))
    except:
        traceback.print_exc()
        startTime = datetime.datetime.now()
        t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
        log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+', encoding="utf-8")
        log.write(t_log + ' ## Ошибка при отправке файла\n')
        traceback.print_exc(file=log)
        log.close()
        return 1

    if result:
        return 0
    else:
        return 1


# IN. Получить продажи за период
def getPurchasesByPeriod(url_srv, date_begin, date_end):
    # Получить данные за заданный период (getPurchasesByPeriod)
    url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    client = zeep.Client(url_c)
    result = client.service.getPurchasesByPeriod(date_begin, date_end)
    # result = client.service.getPurchasesByPeriod('2024-09-03', '2024-09-03T23:59:00.000')
    file_xml = result.decode("utf-8")
    # print(file_xml)
    # #
    # # Сохранить в файл
    # with open('out_cheque_21052025.xml', 'w', encoding='utf-8') as f:
    #     f.write(file_xml)
    #     f.close()
    #
    # sys.exit()

    # Преобразовать xml в словарь
    dict_data = xmltodict.parse(file_xml)
    # print(dict_data)
    perchases_count = dict_data.get('purchases')["@count"]
    api_utils.InsertLog('Всего продаж: ' + str(perchases_count))

    if perchases_count == '0':
        return

    # Преобразовать словарь в df
    df_purchase = pd.json_normalize(dict_data['purchases']['purchase'])

    # print(df_purchase)
    # print('purchases2*************')
    # df_purchase.to_excel("df_purchase_23122024.xlsx")
    # sys.exit()

    # Чтобы забрать ФИО кассира
    df_purchase['CASHIER_NAME'] = None

    # Оплаты в одной таблице (чтобы делать вставку в базу 1 раз)
    # df_payment_all = pd.read_xml(file_xml, xpath='(.//payments/*)')

    df_payment_all = pd.DataFrame(columns=['order', 'typeClass', 'amount', 'description',  # 4
                                           'pay.frcode', 'code', 'bank.id', 'ref.number', 'card.type', 'bank.type',
                                           'card.number',  # 11
                                           'terminal.number', 'cash.transaction.date', 'subclass', 'merchant.id'])  # 15

    # Позиции со всех чеков в одной таблице (чтобы делать вставку в базу 1 раз)
    # Сначала загрузим всё из xml (чтобы создались колонки)
    # А потом ниже удалим пустые (df_position_all.dropna(subset=['cash']))
    df_position_all = pd.read_xml(file_xml, xpath="/purchases/purchase/positions/position")


    # print(pd.read_xml(file_xml, xpath="/purchases/purchase/positions/position"))
    # print('+++++++++++++++++++++')

    # Колонки для позиций
    col_position = ['@order', '@departNumber', '@goodsCode', '@barCode',
                    '@count', '@cost', '@nds', '@ndsSum', '@discountValue', '@costWithDiscount',
                    '@amount', '@dateCommit', '@insertType'
                    ]
    # print(df_payment_all.columns)
    # print('*****************')

    # Дополнительные колонки для оплат
    add_column_payment = ['pay.frcode', 'code', 'bank.id', 'ref.number', 'card.type', 'bank.type', 'card.number',  # 7
                          'terminal.number', 'cash.transaction.date', 'subclass', 'merchant.id']  # 11
    #df_payment_all[[add_column_payment]] = None #None, None, None, None, None, None, None, None, None, None, None  # 11

    # print(df_payment_all.columns)
    # sys.exit()

    # Марки
    df_exciseBottles_all = pd.DataFrame(columns=['shop', 'cash', 'shift', 'number', 'factorynum'])

    # Соберём данные чеков, позиций и оплат в разные df
    for i, row in df_purchase.iterrows():
        # print(f"Index: {i}")
        # print(f"{row}\n")
        # print('***************')


        # Взять идентификаторы смены
        shop = df_purchase.iloc[i]['@shop']
        cash = df_purchase.iloc[i]['@cash']
        shift = df_purchase.iloc[i]['@shift']
        number = df_purchase.iloc[i]['@number']

        # STEEL от 24.04.2025 ВМЕСТО fiscalnum должен браться factorynum - "Заводской номер"
        factorynum = df_purchase.iloc[i]['@factorynum']

        #fiscalnum = df_purchase.iloc[i]['@fiscalnum']
        #! print(shop, cash, shift, number, factorynum)

        # Свойства - взять ФИО кассира
        CASHIER_NAME = None
        try:
            json_property = json.loads(json.dumps(row['plugin-property'], indent=4))
            df_property = pd.DataFrame(json_property)
            idx = df_property[df_property['@key'] == 'CASHIER_NAME'].index
            CASHIER_NAME = df_property.iloc[idx]['@value']
            df_purchase['CASHIER_NAME'].iloc[i] = CASHIER_NAME.at[idx[0]]
        except:
            pass

        # Позиции
        # Если есть позиции больше 1, то в df_purchase будет колонка 'positions.position'
        # Если позиция одна, то колонки 'positions.position' не будет
        if 'positions.position' in df_purchase.columns:
            json_positions = json.loads(json.dumps(row['positions.position'], indent=4))
            #! print('json_positions==',json_positions)
            #! print(type(json_positions))
            # Когда одновременно есть продажи с одной позицией и с несколькими:
            # 1) Если только одна позиция - то значения в отдельных полях (при этом колонка 'payments.payment' float)
            # 2) Если несколько - то в колнке positions.position есть данные
            if type(json_positions) == float:
                df_positions = pd.DataFrame({"@order": [row['positions.position.@order']],
                                           "@departNumber": [row['positions.position.@departNumber']],
                                           "@goodsCode": [row['positions.position.@goodsCode']],
                                           "@barCode": [row['positions.position.@barCode']],
                                           "@count": [row['positions.position.@count']],
                                           "@cost": [row['positions.position.@cost']],
                                           "@nds": [row['positions.position.@nds']],
                                           "@ndsSum": [row['positions.position.@ndsSum']],
                                           "@discountValue": [row['positions.position.@discountValue']],
                                           "@costWithDiscount": [row['positions.position.@costWithDiscount']],
                                           "@amount": [row['positions.position.@amount']],
                                           "@dateCommit": [row['positions.position.@dateCommit']],
                                           "@insertType": [row['positions.position.@insertType']]
                                            })
            else:
                df_positions = pd.DataFrame(json_positions)
        else:
            df_positions = pd.DataFrame({"@order": [row['positions.position.@order']],
                                         "@departNumber": [row['positions.position.@departNumber']],
                                         "@goodsCode": [row['positions.position.@goodsCode']],
                                         "@barCode": [row['positions.position.@barCode']],
                                         "@count": [row['positions.position.@count']],
                                         "@cost": [row['positions.position.@cost']],
                                         "@nds": [row['positions.position.@nds']],
                                         "@ndsSum": [row['positions.position.@ndsSum']],
                                         "@discountValue": [row['positions.position.@discountValue']],
                                         "@costWithDiscount": [row['positions.position.@costWithDiscount']],
                                         "@amount": [row['positions.position.@amount']],
                                         "@dateCommit": [row['positions.position.@dateCommit']],
                                         "@insertType": [row['positions.position.@insertType']]
                                         })

        # !!! Взять только определённые колонки. Поле plugin-property не нужно, т.к. бывает, что оно размножает строки
        df_positions = df_positions.loc[:, df_positions.columns.intersection(col_position)]

        df_positions['shop'] = shop
        df_positions['cash'] = cash
        df_positions['shift'] = shift
        df_positions['number'] = number
        df_positions['factorynum'] = factorynum
        # !!! Убрать дубликаты
        df_positions = df_positions.drop_duplicates()

        # Переименовать колонки у позиций
        df_positions.rename(columns={'@order': 'order', '@departNumber': 'departNumber',
                                     '@goodsCode': 'goodsCode', '@barCode': 'barCode',
                                     '@count': 'count', '@cost': 'cost', '@nds': 'nds', '@ndsSum': 'ndsSum',
                                     '@discountValue': 'discountValue',
                                     '@costWithDiscount': 'costWithDiscount',
                                     '@amount': 'amount', '@dateCommit': 'dateCommit',
                                     '@insertType': 'insertType'},
                            inplace=True)

        # Собрать в один df (чтобы потом 1 раз делать вставку в базу)
        df_position_all = pd.concat([df_position_all, df_positions])

        # Оплаты
        # Если есть оплаты больше 1, то в df_purchase будет колонка 'payments.payment'
        # Если оплата только одна, то колонки 'payments.payment' не будет
        if 'payments.payment' in df_purchase.columns:
            json_payment = json.loads(json.dumps(row['payments.payment'], indent=4))
            #! print('json_payment==', json_payment)
            #! print(type(json_payment))
            # Когда одновременно есть продажи с одной оплатой и с несколькими:
            # 1) Если только одна оплата - то значения в отдельных полях (при этом колонка 'payments.payment' float)
            # 2) Если несколько - то в колнке payments.payment есть данные
            if type(json_payment) == float:
                # print("row['payments.payment.@order']===", row['payments.payment.@order'])
                # print("row['payments.payment.@typeClass']===", row['payments.payment.@typeClass'])
                # print("row['payments.payment.@amount']===", row['payments.payment.@amount'])
                # print("row['payments.payment.@description']===", row['payments.payment.@description'])
                # Если только 1 свойство, то будут payments.payment.plugin-property.@key, payments.payment.plugin-property.@value
                try:
                    df_payment = pd.DataFrame({"@order": [row['payments.payment.@order']],
                                              "@typeClass": [row['payments.payment.@typeClass']],
                                              "@amount": [row['payments.payment.@amount']],
                                              "@description": [row['payments.payment.@description']],
                                              "plugin-property": [row['payments.payment.plugin-property']]})
                except:
                    pp_key = row['payments.payment.plugin-property.@key']
                    pp_value = row['payments.payment.plugin-property.@value']
                    #! print(pp_key, pp_value)
                    pp_dict = {'@key': pp_key, '@value': pp_value}
                    #! print(type(pp_dict), pp_dict)
                    if pp_key:
                        df_payment = pd.DataFrame({"@order": [row['payments.payment.@order']],
                                                   "@typeClass": [row['payments.payment.@typeClass']],
                                                   "@amount": [row['payments.payment.@amount']],
                                                   "@description": [row['payments.payment.@description']],
                                                   "plugin-property": [pp_dict]})
                    else:
                        if pp_key:
                            df_payment = pd.DataFrame({"@order": [row['payments.payment.@order']],
                                                       "@typeClass": [row['payments.payment.@typeClass']],
                                                       "@amount": [row['payments.payment.@amount']],
                                                       "@description": [row['payments.payment.@description']]})
            else:
                df_payment = pd.DataFrame(json_payment)
        else:
            try:
                # Если только 1 свойство, то будут payments.payment.plugin-property.@key, payments.payment.plugin-property.@value
                pp_key = row['payments.payment.plugin-property.@key']
                pp_value = row['payments.payment.plugin-property.@value']
                #! print(pp_key, pp_value)
                pp_dict = {'@key': pp_key, '@value': pp_value}
                #! print(type(pp_dict), pp_dict)
            except:
                pp_key = None

            if pp_key:
                df_payment = pd.DataFrame({"@order": [row['payments.payment.@order']],
                                           "@typeClass": [row['payments.payment.@typeClass']],
                                           "@amount": [row['payments.payment.@amount']],
                                           "@description": [row['payments.payment.@description']],
                                           "plugin-property": [pp_dict]})
            else:
                df_payment = pd.DataFrame({"@order": [row['payments.payment.@order']],
                                           "@typeClass": [row['payments.payment.@typeClass']],
                                           "@amount": [row['payments.payment.@amount']],
                                           "@description": [row['payments.payment.@description']],
                                           "plugin-property": [row['payments.payment.plugin-property']]})

        #print('df_payment==', df_payment)
        # df_payment.to_excel('df_payment.xlsx')
        # sys.exit()


        # Забрать свойства plugin-property, чтобы дальше их добавить колонками
        # df_payment_plugin_property = df_payment['plugin-property']
        # # print('df_payment_plugin_property==', df_payment_plugin_property)
        # # print(len(df_payment_plugin_property))
        # # print(type(df_payment_plugin_property))


        # Взять только первое значение, т.к. plugin-property идёт строками в payment и размножает записи в df
        df_payment = df_payment.groupby('@order').nth(0)  # .reset_index()

        df_payment['shop'] = shop
        df_payment['cash'] = cash
        df_payment['shift'] = shift
        df_payment['number'] = number
        df_payment['factorynum'] = factorynum

        # Забираем данные из дополнительных свойств (plugin-property) и записываем в нужные колонки df
        for p, row_p in df_payment.iterrows():
            # print(p, row_p)
            # print('**********************')


            if 'plugin-property' in row_p:
                # print(row_p['plugin-property'])
                if isinstance(row_p['plugin-property'], dict):
                    #print('key=' + j['@key'], '; value=' + str(j['@value']))
                    if row_p['plugin-property']['@key'] in add_column_payment:
                        key = row_p['plugin-property']['@key']
                        value = row_p['plugin-property']['@value']
                        #! print(key + '  ***====***', value)
                        df_payment.loc[p, key] = value
                        #print(df_payment)

                if isinstance(row_p['plugin-property'], list):
                    #print('LIST_j=',row_p['plugin-property'])
                    for l in row_p['plugin-property']:
                        #! print('l=',l)
                        if l['@key'] in add_column_payment:
                            key = l['@key']
                            value = l['@value']
                            #! print(key + '  ***====***', value)
                            df_payment.loc[p, key] = value

        if 'plugin-property' in df_payment:
            del df_payment['plugin-property']

        # Переименовать колонки у оплат
        df_payment.rename(columns={'@order': 'order', '@typeClass': 'typeClass',
                                   '@amount': 'amount', '@description': 'description'},
                          inplace=True)

        # Собрать оплаты в один df (чтобы потом 1 раз делать вставку в базу)
        df_payment_all = pd.concat([df_payment_all, df_payment])


        # Марки
        # Если есть в колонках продаж exciseBottles
        if not df_purchase.filter(regex="^exciseBottles").empty:
            if 'exciseBottles.bottle.@barcode' in row and not pd.isna(row['exciseBottles.bottle.@barcode']):
                    df_exciseBottles = pd.DataFrame({"@barcode": [row['exciseBottles.bottle.@barcode']],
                                                     "@exciseBarcode": [row['exciseBottles.bottle.@exciseBarcode']],
                                                     "@volume": [row['exciseBottles.bottle.@volume']],
                                                     "@price": [row['exciseBottles.bottle.@price']]})
                    df_exciseBottles['shop'] = shop
                    df_exciseBottles['cash'] = cash
                    df_exciseBottles['shift'] = shift
                    df_exciseBottles['number'] = number
                    df_exciseBottles['factorynum'] = factorynum
                    df_exciseBottles_all = pd.concat([df_exciseBottles_all, df_exciseBottles])
            else:
                if 'exciseBottles.bottle' in df_purchase.columns:
                    json_exciseBottles = json.loads(json.dumps(row['exciseBottles.bottle'], indent=4))

                    if type(json_exciseBottles) == float:
                        # Если не пусто (не NaN), то забрать марки
                        if not pd.isna(json_exciseBottles):
                            df_exciseBottles = pd.DataFrame({"@barcode": [row['exciseBottles.bottle.@barcode']],
                                                             "@exciseBarcode": [
                                                                 row['exciseBottles.bottle.@exciseBarcode']],
                                                             "@volume": [row['exciseBottles.bottle.@volume']],
                                                             "@price": [row['exciseBottles.bottle.@price']]})
                            df_exciseBottles['shop'] = shop
                            df_exciseBottles['cash'] = cash
                            df_exciseBottles['shift'] = shift
                            df_exciseBottles['number'] = number
                            df_exciseBottles['factorynum'] = factorynum
                            df_exciseBottles_all = pd.concat([df_exciseBottles_all, df_exciseBottles])
                    else:
                        df_exciseBottles = pd.DataFrame(json_exciseBottles)
                        df_exciseBottles['shop'] = shop
                        df_exciseBottles['cash'] = cash
                        df_exciseBottles['shift'] = shift
                        df_exciseBottles['number'] = number
                        df_exciseBottles['factorynum'] = factorynum
                        df_exciseBottles_all = pd.concat([df_exciseBottles_all, df_exciseBottles])


    # Чеки. Определить необходимые колонки
    col_Purchase = ['@tabNumber', '@userName', '@operationType', '@cashOperation',
                    '@operDay', '@shop', '@cash', '@shift', '@number', '@saletime', '@begintime',
                    '@amount', '@discountAmount', '@inn', '@qrcode',
                    '@fiscalDocNum', '@factorynum', '@fiscalnum', 'CASHIER_NAME'
                    ]

    # !!! Взять только определённые колонки.
    df_purchase = df_purchase.loc[:, df_purchase.columns.intersection(col_Purchase)]

    # Чеки. Переименовать колонки
    df_purchase.rename(columns={'@tabNumber': 'tabNumber', '@userName': 'userName',
                                '@operationType': 'operationType', '@cashOperation': 'cashOperation',
                                '@operDay': 'operDay', '@shop': 'shop', '@cash': 'cash', '@shift': 'shift',
                                '@number': 'number', '@saletime': 'saletime', '@begintime': 'begintime',
                                '@amount': 'amount', '@discountAmount': 'discountAmount',
                                '@inn': 'inn', '@qrcode': 'qrcode', '@fiscalDocNum': 'fiscalDocNum',
                                '@factorynum': 'factorynum', '@fiscalnum': 'fiscalnum'},
                       inplace=True)

    # Если есть марки, то будет заполнено поле price, переименовываем колонки
    if '@price' in df_exciseBottles_all.columns:
        df_exciseBottles_all.rename(columns={'@barcode': 'barcode', '@exciseBarcode': 'exciseBarcode',
                                             '@volume': 'volume', '@price': 'price'},
                           inplace=True)
    else:
        df_exciseBottles_all = df_exciseBottles_all.drop(df_exciseBottles_all.index, inplace=True)


    # df_purchase.to_excel("in_df_purchase_NEW.xlsx")
    # sys.exit()

    # Позиции чеков. Удалить из позиций пустые строки, т.к было объединение
    df_position_all = df_position_all.dropna(subset=['cash'])
    # Удалить ненужные колонки (чтобы вставка в базу прошла корректно)
    del df_position_all['plugin-property']

    # Оплаты. Удалить из оплат пустые строки, т.к было объединение
    df_payment_all = df_payment_all.dropna(subset=['cash'])
    # df_payment_all['plugin-property'] = df_payment_all['plugin-property'].astype(str)

    # Удалить продажи, где нет марок
    if df_exciseBottles_all is not None:
        if not df_exciseBottles_all.empty:
            df_exciseBottles_all = df_exciseBottles_all.dropna(subset=['exciseBarcode'])

    # print(df_purchase)
    # print(df_position_all)
    # print(df_payment_all)
    # print(df_exciseBottles_all)
    #df_exciseBottles_all.to_excel('df_exciseBottles_all.xlsx')
    # sys.exit()

    # Забрать чеки из базы #"inn, qrcode, fiscalDocNum, factorynum, fiscalnum, CASHIER_NAME " \
    purchase_query = "select tabNumber, userName, operationType, cashOperation, operDay, " \
                     "shop, cash, shift, number, saletime, begintime, amount, discountAmount, " \
                     "factorynum, inn, qrcode, fiscalDocNum, fiscalnum, CASHIER_NAME " \
                     "from Crystals_Purchase " \
                     "where saletime between '" + str(date_begin) + "' and '" + str(date_end) + "'"
    # "where saletime between '2025-03-10T00:00:00' and '2025-03-10T17:58:40'"
    # "where convert(datetime,replace(operday,'+03:00','')) = '" + str(operday) + "'"
    # "where convert(varchar,convert(datetime,replace(saletime,'+03:00','')),20) between '" \
    df_purchase_base = api_utils.select_query(purchase_query, login_sql, password_sql, server_sql, driver_sql, database)



    #api_utils.InsertLog('Забрали из базы продажи')

    # print(df_purchase_base)
    # sys.exit()

    # Убрать из даты +03:00  (2024-09-03+03:00)
    # Взять первые 10 символов из строки с датой
    df_purchase["operDay"] = df_purchase['operDay'].str[:10]

    # Удалить из df чеки, которые уже есть в базе
    if df_purchase_base is not None:
        if not df_purchase_base.empty:
            mask = df_purchase.isin(df_purchase_base.to_dict(orient='list')).all(axis=1)
            df_purchase_new = df_purchase[~mask]
        else:
            df_purchase_new = df_purchase

    df_purchase_new_key = df_purchase_new[["shop", "cash", "shift", "number", "factorynum"]]

    # df_purchase_base.to_excel("df_purchase_base.xlsx")
    # df_purchase_new.to_excel("df_purchase_new.xlsx")
    # df_purchase.to_excel("df_purchase_all.xlsx")

    api_utils.InsertLog('Всего продаж c ' + str(date_begin) + ' по ' + str(date_end) +
                        ' из Crystals: ' + str(len(df_purchase)))
    api_utils.InsertLog('Всего продаж c ' + str(date_begin) + ' по ' + str(date_end) +
                        ' в базе: ' + str(len(df_purchase_base)))
    api_utils.InsertLog('Всего продаж необходимо загрузить в базу: ' + str(len(df_purchase_new)))

    # df_position_new.to_excel("in_df_position_new5.xlsx")
    # df_position_all.to_excel("in_df_position_new5_all.xlsx")
    # print(df_position_new)
    # print(df_purchase_new_key)
    # print(df_purchase_new)
    # df_position_all.to_excel("df_purchase_new_25042025.xlsx")
    # sys.exit()

    # Загрузить отсутствующие чеки в бд
    if df_purchase_new is not None:
        if not df_purchase_new.empty:
            # print(df_purchase_new)

            # Позиции. Оставить только те, которых нет в базе.
            # df_position_new = df_position_all.loc[df_position_all["shop"].isin(df_purchase_new['shop']) &
            #                                       df_position_all["cash"].isin(df_purchase_new['cash']) &
            #                                       df_position_all["shift"].isin(df_purchase_new['shift']) &
            #                                       df_position_all["number"].isin(df_purchase_new['number']) &
            #                                       df_position_all["factorynum"].isin(df_purchase_new['factorynum'])]
            df_position_new = df_position_all.merge(df_purchase_new_key, on=["shop", "cash", "shift", "number", "factorynum"])
            api_utils.InsertLog('Всего позиций необходимо загрузить в базу: ' + str(len(df_position_new)))

            # Оплаты. Оставить только те, которых нет в базе.
            # df_payment_new = df_payment_all.loc[df_payment_all["shop"].isin(df_purchase_new['shop']) &
            #                                     df_payment_all["cash"].isin(df_purchase_new['cash']) &
            #                                     df_payment_all["shift"].isin(df_purchase_new['shift']) &
            #                                     df_payment_all["number"].isin(df_purchase_new['number']) &
            #                                     df_payment_all["factorynum"].isin(df_purchase_new['factorynum'])]
            df_payment_new = df_payment_all.merge(df_purchase_new_key, on=["shop", "cash", "shift", "number", "factorynum"])
            api_utils.InsertLog('Всего оплат необходимо загрузить в базу: ' + str(len(df_payment_new)))

            # df_position_new.to_excel("in_df_position_new5.xlsx")
            # df_payment_new.to_excel("in_df_payment_new5.xlsx")
            # sys.exit()

            # Вставка чеков
            params = urllib.parse.quote_plus("DRIVER=" + odbc_driver + ";"
                                             "SERVER=" + server_sql + ";"
                                             "DATABASE=" + database + ";"
                                             "UID=" + login_sql + ";"
                                             "PWD=" + password_sql + ";")
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df_purchase_new.to_sql("Crystals_Purchase", engine, index=False, chunksize=10000, if_exists="append")
            api_utils.InsertLog('Вставка чеков: Успешно')

            # Вставка позиций чеков
            df_position_new.to_sql("Crystals_Purchase_Position", engine, index=False, chunksize=10000,
                                   if_exists="append")
            api_utils.InsertLog('Вставка позиций: Успешно')

            # Вставка оплат
            df_payment_new.to_sql("Crystals_Purchase_Payment", engine, index=False, chunksize=10000,
                                  if_exists="append")
            api_utils.InsertLog('Вставка оплат: Успешно')

            # Марки. Оставить только те, которых нет в базе.
            if df_exciseBottles_all is not None:
            #if not df_exciseBottles_all.empty:
                # df_exciseBottles_new = df_exciseBottles_all.loc[df_exciseBottles_all["shop"].isin(df_purchase_new['shop']) &
                #                                                 df_exciseBottles_all["cash"].isin(df_purchase_new['cash']) &
                #                                                 df_exciseBottles_all["shift"].isin(
                #                                                     df_purchase_new['shift']) &
                #                                                 df_exciseBottles_all["number"].isin(
                #                                                     df_purchase_new['number']) &
                #                                                 df_exciseBottles_all["factorynum"].isin(
                #                                                     df_purchase_new['factorynum'])]

                df_exciseBottles_new = df_exciseBottles_all.merge(df_purchase_new_key, on=["shop", "cash", "shift", "number", "factorynum"])
                api_utils.InsertLog('Всего марок необходимо загрузить в базу: ' + str(len(df_exciseBottles_new)))


                # Вставка марок
                if not df_exciseBottles_new.empty:
                    df_exciseBottles_new.to_sql("Crystals_Purchase_ExciseBottles", engine, index=False, chunksize=10000,
                                                if_exists="append")
                    api_utils.InsertLog('Вставка марок: Успешно')
                else:
                    api_utils.InsertLog('Вставка марок: Успешно (марок нет)')
            else:
                api_utils.InsertLog('Марок нет')


# IN. Получить продажи за операционный день
def getPurchasesByOperDay(url_srv, operday):
    # Получить данные за операционный день (getPurchasesByPeriod) и сверить с базой (все ли чеки есть)
    url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    client = zeep.Client(url_c)
    result = client.service.getPurchasesByOperDay(operday)
    #print(result)
    file_xml = result.decode("utf-8")
    #print(file_xml)

    # ??? Можно использовать с подобъектом: По заданным параметрам (getPurchasesByParams)
    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByParams('2024-09-03', 29591)
    # file_xml = result.decode("utf-8")

    # Чеки. Преобразование XML в словарь
    dict_data = xmltodict.parse(file_xml)

    # Количество чеков
    perchases_count = dict_data.get('purchases')["@count"]

    api_utils.InsertLog('Всего продаж: ' + str(perchases_count))

    if perchases_count == '0':
        return

    # Преобразование словаря в JSON
    json_data = json.loads(json.dumps(dict_data, indent=4))
    print(json_data['purchases']['purchase'])
    df_purchase = pd.DataFrame(json_data['purchases']['purchase'])

    if df_purchase.empty:
        return

    # Чтобы забрать ФИО кассира
    df_purchase['CASHIER_NAME'] = None

    # Оплаты в одной таблице (чтобы делать вставку в базу 1 раз)
    # df_payment_all = pd.read_xml(file_xml, xpath='(.//payments/*)')

    df_payment_all = pd.DataFrame(columns=['order', 'typeClass', 'amount', 'description',  # 4
                                           'pay.frcode', 'code', 'bank.id', 'ref.number', 'card.type', 'bank.type',
                                           'card.number',  # 11
                                           'terminal.number', 'cash.transaction.date', 'subclass', 'merchant.id'])  # 15

    # Позиции со всех чеков в одной таблице (чтобы делать вставку в базу 1 раз)
    # Сначала загрузим всё из xml (чтобы создались колонки)
    # А потом ниже удалим пустые (df_position_all.dropna(subset=['cash']))
    df_position_all = pd.read_xml(file_xml, xpath="/purchases/purchase/positions/position")

    # Колонки для позиций
    col_position = ['@order', '@departNumber', '@goodsCode', '@barCode',
                    '@count', '@cost', '@nds', '@ndsSum', '@discountValue', '@costWithDiscount',
                    '@amount', '@dateCommit', '@insertType'
                    ]

    # Дополнительные колонки для оплат
    add_column_payment = ['pay.frcode', 'code', 'bank.id', 'ref.number', 'card.type', 'bank.type', 'card.number',  # 7
                          'terminal.number', 'cash.transaction.date', 'subclass', 'merchant.id']  # 11
    df_payment_all[add_column_payment] = None, None, None, None, None, None, None, None, None, None, None  # 11

    # Соберём данные чеков, позиций и оплат в разные df
    for i, row in df_purchase.iterrows():
        # print(f"Index: {i}")
        # print(f"{row}\n")

        # Взять идентификаторы смены
        shop = df_purchase.iloc[i]['@shop']
        cash = df_purchase.iloc[i]['@cash']
        shift = df_purchase.iloc[i]['@shift']
        number = df_purchase.iloc[i]['@number']

        # STEEL от 24.04.2025 ВМЕСТО fiscalnum должен браться factorynum - "Заводской номер"
        factorynum = df_purchase.iloc[i]['@factorynum']

        #fiscalnum = df_purchase.iloc[i]['@fiscalnum']
        print(shop, cash, shift, number, factorynum)

        # Свойства - взять ФИО кассира
        CASHIER_NAME = None
        try:
            json_property = json.loads(json.dumps(row['plugin-property'], indent=4))
            df_property = pd.DataFrame(json_property)
            idx = df_property[df_property['@key'] == 'CASHIER_NAME'].index
            CASHIER_NAME = df_property.iloc[idx]['@value']
            df_purchase['CASHIER_NAME'].iloc[i] = CASHIER_NAME.at[idx[0]]
        except:
            pass

        # Позиции
        json_positions = json.loads(json.dumps(row['positions'], indent=4))
        df_positions = pd.DataFrame(json_positions['position'])
        # !!! Взять только определённые колонки. Поле plugin-property не нужно, т.к. бывает, что оно размножает строки
        df_positions = df_positions.loc[:, df_positions.columns.intersection(col_position)]

        df_positions['shop'] = shop
        df_positions['cash'] = cash
        df_positions['shift'] = shift
        df_positions['number'] = number
        df_positions['factorynum'] = factorynum
        # !!! Убрать дубликаты
        df_positions = df_positions.drop_duplicates()

        # Переименвоать колонки у позиций
        df_positions.rename(columns={'@order': 'order', '@departNumber': 'departNumber',
                                     '@goodsCode': 'goodsCode', '@barCode': 'barCode',
                                     '@count': 'count', '@cost': 'cost', '@nds': 'nds', '@ndsSum': 'ndsSum',
                                     '@discountValue': 'discountValue',
                                     '@costWithDiscount': 'costWithDiscount',
                                     '@amount': 'amount', '@dateCommit': 'dateCommit',
                                     '@insertType': 'insertType'},
                            inplace=True)

        # Собрать в один df (чтобы потом 1 раз делать вставку в базу)
        df_position_all = pd.concat([df_position_all, df_positions])

        # Оплаты
        # print('row_payments**********', row['payments']['payment'])
        json_payment = json.loads(json.dumps(row['payments']['payment'], indent=4))
        df_payment = pd.DataFrame(json_payment)

        # Забрать свойства plugin-property, чтобы дальше их добавить колонками
        df_payment_plugin_property = df_payment['plugin-property']

        # Взять только первое значение, т.к. plugin-property идёт строками в payment и размножает записи в df
        df_payment = df_payment.groupby('@order').nth(0) #.reset_index()

        df_payment['shop'] = shop
        df_payment['cash'] = cash
        df_payment['shift'] = shift
        df_payment['number'] = number
        df_payment['factorynum'] = factorynum

        # print('df_payment*******',df_payment)
        # print('df_payment_FIRST*******', df_payment.groupby('@order').nth(0).reset_index())
        # print('df_payment_plugin-property*******', df_payment['plugin-property'])

        # Забираем данные из дополнительных свойств (plugin-property) и записываем в нужные колонки df
        for j in df_payment_plugin_property:
            if isinstance(j, dict):
                #print('key=' + j['@key'], '; value=' + str(j['@value']))
                if j['@key'] in add_column_payment:
                    print(j['@key'] + '  ***====***', j['@value'])
                    df_payment[j['@key']] = j['@value']

        del df_payment['plugin-property']


        # Переименовать колонки у оплат
        df_payment.rename(columns={'@order': 'order', '@typeClass': 'typeClass',
                                   '@amount': 'amount', '@description': 'description'},
                          inplace=True)

        # Собрать оплаты в один df (чтобы потом 1 раз делать вставку в базу)
        df_payment_all = pd.concat([df_payment_all, df_payment])

        #print('df_payment_all********',df_payment_all)


    #sys.exit()

    # Чеки. Определить необходимые колонки
    col_Purchase = ['@tabNumber', '@userName', '@operationType', '@cashOperation',
                    '@operDay', '@shop', '@cash', '@shift', '@number', '@saletime', '@begintime',
                    '@amount', '@discountAmount', '@inn', '@qrcode',
                    '@fiscalDocNum', '@factorynum', '@fiscalnum', 'CASHIER_NAME'
                    ]

    # !!! Взять только определённые колонки.
    df_purchase = df_purchase.loc[:, df_purchase.columns.intersection(col_Purchase)]

    # Чеки. Переименовать колонки
    df_purchase.rename(columns={'@tabNumber': 'tabNumber', '@userName': 'userName',
                                '@operationType': 'operationType', '@cashOperation': 'cashOperation',
                                '@operDay': 'operDay', '@shop': 'shop', '@cash': 'cash', '@shift': 'shift',
                                '@number': 'number', '@saletime': 'saletime', '@begintime': 'begintime',
                                '@amount': 'amount', '@discountAmount': 'discountAmount',
                                '@inn': 'inn', '@qrcode': 'qrcode', '@fiscalDocNum': 'fiscalDocNum',
                                '@factorynum': 'factorynum', '@fiscalnum': 'fiscalnum'},
                       inplace=True)

    # Позиции чеков. Удалить из позиций пустые строки, т.к было объединение
    df_position_all = df_position_all.dropna(subset=['cash'])
    # Удалить ненужные колонки (чтобы вставка в базу прошла корректно)
    del df_position_all['plugin-property']

    # Оплаты. Удалить из оплат пустые строки, т.к было объединение
    df_payment_all = df_payment_all.dropna(subset=['cash'])
    # df_payment_all['plugin-property'] = df_payment_all['plugin-property'].astype(str)

    # Забрать чеки из базы
    purchase_query = "select tabNumber, userName, operationType, cashOperation, operDay, " \
                     "shop, cash, shift, number, saletime, begintime, amount, discountAmount, " \
                     "inn, qrcode, fiscalDocNum, factorynum, fiscalnum, CASHIER_NAME " \
                     "from Crystals_Purchase " \
                     "where operday = '" + str(operday) + "'"
                     #"where convert(datetime,replace(operday,'+03:00','')) = '" + str(operday) + "'"
    df_purchase_base = api_utils.select_query(purchase_query, login_sql, password_sql, server_sql, driver_sql, database)

    # Убрать из даты +03:00  (2024-09-03+03:00)
    # Взять первые 10 символов из строки с датой
    df_purchase["operDay"] = df_purchase['operDay'].str[:10]

    # Удалить из df чеки, которые уже есть в базе
    if not df_purchase_base.empty:
        mask = df_purchase.isin(df_purchase_base.to_dict(orient='list')).all(axis=1)
        df_purchase_new = df_purchase[~mask]
    else:
        df_purchase_new = df_purchase
    # df_purchase_new.to_excel("in_df_purchase_new_NEW5.xlsx")

    api_utils.InsertLog('Всего продаж за ' + str(operday) + ' из Crystals: ' + str(len(df_purchase)))
    api_utils.InsertLog('Всего продаж за ' + str(operday) + ' в базе: ' + str(len(df_purchase_base)))
    api_utils.InsertLog('Всего продаж за ' + str(operday) + ' необходимо загрузить в базу: ' + str(len(df_purchase_new)))

    #sys.exit()

    # Загрузить отсутствующие чеки в бд
    if not df_purchase_new.empty:
        #print(df_purchase_new)

        # Позиции. Оставить только те, которых нет в базе.
        df_position_new = df_position_all.loc[df_position_all["shop"].isin(df_purchase_new['shop']) &
                                              df_position_all["cash"].isin(df_purchase_new['cash']) &
                                              df_position_all["shift"].isin(df_purchase_new['shift']) &
                                              df_position_all["number"].isin(df_purchase_new['number']) &
                                              df_position_all["factorynum"].isin(df_purchase_new['factorynum'])]
        api_utils.InsertLog('Всего позиций необходимо загрузить в базу: ' + str(len(df_position_new)))

        # Оплаты. Оставить только те, которых нет в базе.
        df_payment_new = df_payment_all.loc[df_payment_all["shop"].isin(df_purchase_new['shop']) &
                                            df_payment_all["cash"].isin(df_purchase_new['cash']) &
                                            df_payment_all["shift"].isin(df_purchase_new['shift']) &
                                            df_payment_all["number"].isin(df_purchase_new['number']) &
                                            df_payment_all["factorynum"].isin(df_purchase_new['factorynum'])]
        api_utils.InsertLog('Всего Оплат необходимо загрузить в базу: ' + str(len(df_payment_new)))
        #sys.exit()

        # Вставка чеков
        params = urllib.parse.quote_plus("DRIVER=" + odbc_driver + ";"
                                         "SERVER=" + server_sql + ";"
                                         "DATABASE=" + database + ";"
                                         "UID=" + login_sql + ";"
                                         "PWD=" + password_sql + ";")
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        df_purchase_new.to_sql("Crystals_Purchase", engine, index=False, chunksize=10000, if_exists="append")
        api_utils.InsertLog('Вставка чеков: Успешно')

        # Вставка позиций чеков
        params = urllib.parse.quote_plus("DRIVER=" + odbc_driver + ";"
                                         "SERVER=" + server_sql + ";"
                                         "DATABASE=" + database + ";"
                                         "UID=" + login_sql + ";"
                                         "PWD=" + password_sql + ";")
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        df_position_new.to_sql("Crystals_Purchase_Position", engine, index=False, chunksize=10000,
                               if_exists="append")
        api_utils.InsertLog('Вставка позиций: Успешно')

        # Вставка оплат
        params = urllib.parse.quote_plus("DRIVER=" + odbc_driver + ";"
                                         "SERVER=" + server_sql + ";"
                                         "DATABASE=" + database + ";"
                                         "UID=" + login_sql + ";"
                                         "PWD=" + password_sql + ";")
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        df_payment_new.to_sql("Crystals_Purchase_Payment", engine, index=False, chunksize=10000,
                              if_exists="append")
        api_utils.InsertLog('Вставка оплат: Успешно')


# IN. Экспорт Z-отчетов из SetRetail10 в ERP (веб-сервис на стороне SetRetail10)
def getZReportsByOperDay (url_srv, operday):
    # За заданный операционный день (getZReportsByOperDay)
    # Описание: https://crystals.atlassian.net/wiki/spaces/INT/pages/1646877/Z-+SetRetail10+ERP+-+SetRetail10
    url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    client = zeep.Client(url_c)
    result = client.service.getZReportsByOperDay(operday)
    file_xml = result.decode("utf-8")
    # print(file_xml)

    # Сохранить в файл
    # with open('z_xml', 'w', encoding='utf-8') as f:
    #     f.write(file_xml)
    #     f.close()
    #
    # sys.exit()

    # Преобразование XML в словарь
    dict_data = xmltodict.parse(file_xml)
    # print(dict_data)

    # Количество z-отчётов
    zreport_count = dict_data.get('reports')["@count"]
    api_utils.InsertLog('Файл получен. Всего z-отчётов: ' + str(zreport_count))

    if zreport_count == '0':
        return

    # Делаем из словаря df
    df_zreport = pd.json_normalize(dict_data['reports']['zreport'])

    # print(df_zreport)
    # df_zreport.to_excel('df_zreport.xlsx')
    # sys.exit()

    if df_zreport.empty:
        return

    # Добавить нужные (дополнительные) колонки в df. Значения которых будут браться из 'plugin-property'
    df_zreport[[zreport_addcolumns]] = None

    # Соберём данные z-отчётов в один df
    for i, row in df_zreport.iterrows():
        # print(f"Index: {i}")
        # print(f"{row}\n")

        # Забираем данные из дополнительных свойств (plugin-property) и добавляем в df
        if 'plugin-property' in row: #df_zreport.columns: # STEEL от 22.05.2025 for j in row['plugin-property']: TypeError: 'float' object is not iterable
            # print('/*/*/*/*/*/*/*/*/*/')
            # print(row['plugin-property'])
            # print(type(row['plugin-property']))

            if isinstance(row['plugin-property'], float): # STEEL от 22.05.2025 Если колонка float и не пуская (не nan)
                if not math.isnan(row['plugin-property']):
                    for j in row['plugin-property']:
                        #! print('key=' + j['@key'], '; value=' + j['@value'])

                        if j['@key'] in zreport_addcolumns:
                            #! print(j['@key'] + '  ***====***', j['@value'])
                            df_zreport.loc[i, j['@key']] = j['@value']
            else:
                for j in row['plugin-property']:
                    if j['@key'] in zreport_addcolumns:
                        df_zreport.loc[i, j['@key']] = j['@value']

        # Забираем данные платежей
        # Если есть оплаты больше 1, то в df_purchase будет колонка 'payments.payment'
        # Если оплата только одна, то колонки 'payments.payment' не будет
        if 'payments.payment' in row:
            json_payment = json.loads(json.dumps(row['payments.payment'], indent=4))
            #! print('json_payment==', json_payment)
            #! print(type(json_payment))
            # Когда одновременно есть продажи с одной оплатой и с несколькими:
            # 1) Если только одна оплата - то значения в отдельных полях (при этом колонка 'payments.payment' float)
            # 2) Если несколько - то в колнке payments.payment есть данные
            if type(json_payment) == list:
                for k in json_payment:
                    #! print('k=', k)
                    # Наличные
                    if k['@typeClass'] == 'CashPaymentEntity':
                        if '@amountPurchase' in k:
                            #! print('CashPaymentEntity_amountPurchase====', k['@amountPurchase'])
                            df_zreport.loc[i,'CashPaymentEntity_amountPurchase'] = k['@amountPurchase']
                        if '@amountReturn' in k:
                            #! print('CashPaymentEntity_amountReturn====', k['@amountReturn'])
                            df_zreport.loc[i, 'CashPaymentEntity_amountReturn'] = k['@amountReturn']

                    # Оплата картой
                    if k['@typeClass'] == 'BankCardPaymentEntity':
                        if '@amountPurchase' in k:
                            #! print('BankCardPaymentEntity_amountPurchase====', k['@amountPurchase'])
                            df_zreport.loc[i,'BankCardPaymentEntity_amountPurchase'] = k['@amountPurchase']
                        if '@amountReturn' in k:
                            #! print('BankCardPaymentEntity_amountReturn====', k['@amountReturn'])
                            df_zreport.loc[i, 'BankCardPaymentEntity_amountReturn'] = k['@amountReturn']
        # Может быть только 1 payment
        # Тогда не будет колонки 'payments.payment', но будут:
        # taxes.tax.@nds,taxes.tax.@ndsSumSale,taxes.tax.@ndsSumReturn,taxes.tax.@sumPosition
        else:
            if 'payments.payment.@typeClass' in row:
                # Наличные
                if row['payments.payment.@typeClass'] == 'CashPaymentEntity':
                    if 'payments.payment.@amountPurchase' in row:
                        #! print('CashPaymentEntity_amountPurchase====', row['payments.payment.@amountPurchase'])
                        df_zreport.loc[i, 'CashPaymentEntity_amountPurchase'] = row['payments.payment.@amountPurchase']
                    if 'payments.payment.@amountReturn' in row:
                        #! print('CashPaymentEntity_amountReturn====', row['payments.payment.@amountReturn'])
                        df_zreport.loc[i, 'CashPaymentEntity_amountReturn'] = row['payments.payment.@amountReturn']

                # Оплата картой
                if row['payments.payment.@typeClass'] == 'BankCardPaymentEntity':
                    if 'payments.payment.@amountPurchase' in row:
                        #! print('BankCardPaymentEntity_amountPurchase====', row['payments.payment.@amountPurchase'])
                        df_zreport.loc[i, 'BankCardPaymentEntity_amountPurchase'] = row['payments.payment.@amountPurchase']
                    if 'payments.payment.@amountReturn' in row:
                        #! print('BankCardPaymentEntity_amountReturn====', row['payments.payment.@amountReturn'])
                        df_zreport.loc[i, 'BankCardPaymentEntity_amountReturn'] = row['payments.payment.@amountReturn']

        # Забираем данные сумм НДС
        if 'taxes.tax' in row:
            json_tax = json.loads(json.dumps(row['taxes.tax'], indent=4))
            #! print('json_tax==', json_tax)
            #! print(type(json_tax))
            if type(json_tax) == list:
                for t in json_tax:
                    #! print('t=',t)
                    if '@nds' in t:
                        #! print('nds====', t['@nds'])
                        # Забирать только по актуальным НДС (tax = ('10', '20'))
                        if t['@nds'] in tax:
                            if '@ndsSumSale' in t:
                                #! print(str(t['@nds']) + ', ndsSumSale====', t['@ndsSumSale'])
                                df_zreport.loc[i, 'nds' + str(t['@nds']) + '_ndsSumSale'] = t['@ndsSumSale']
                            if '@ndsSumReturn' in t:
                                #! print(str(t['@nds']) + ', ndsSumReturn====', t['@ndsSumReturn'])
                                df_zreport.loc[i, 'nds' + str(t['@nds']) + '_ndsSumReturn'] = t['@ndsSumReturn']
                            if '@sumPosition' in t:
                                #! print(str(t['@nds']) + ', sumPosition====', t['@sumPosition'])
                                df_zreport.loc[i, 'nds' + str(t['@nds']) + '_sumPosition'] = t['@sumPosition']
        # Может быть только 1 tax
        # Тогда не будет колонки 'taxes.tax', но будут:
        # taxes.tax.@nds,taxes.tax.@ndsSumSale,taxes.tax.@ndsSumReturn,taxes.tax.@sumPosition
        else:
            if 'taxes.tax.@nds' in row:
                nds = row['taxes.tax.@nds']
                #! print('nds=', nds)
                #! print(type(nds))
                # Забирать только по актуальным НДС (tax = ('10', '20'))
                if nds in tax:
                    if 'taxes.tax.@ndsSumSale' in row:
                        #! print(str(nds) + ', ndsSumSale====', row['taxes.tax.@ndsSumSale'])
                        df_zreport.loc[i, 'nds' + str(nds) + '_ndsSumSale'] = row['taxes.tax.@ndsSumSale']
                    if 'taxes.tax.@ndsSumReturn' in row:
                        #! print(str(nds) + ', ndsSumReturn====', row['taxes.tax.@ndsSumReturn'])
                        df_zreport.loc[i, 'nds' + str(nds) + '_ndsSumReturn'] = row['taxes.tax.@ndsSumReturn']
                    if 'taxes.tax.@sumPosition' in row:
                        #! print(str(nds) + ', sumPosition====', row['taxes.tax.@sumPosition'])
                        df_zreport.loc[i, 'nds' + str(nds) + '_sumPosition'] = row['taxes.tax.@sumPosition']

    # Оставить в df только нужные колонки
    df_zreport = df_zreport[zreport_columns]

    api_utils.InsertLog('Всего за ' + str(operday) + ' z-отчётов в Crystals: ' + str(len(df_zreport)))

    if not df_zreport.empty:
        # Забрать Z-отчёты из базы
        zreport_query = "select convert(varchar(50),shiftNumber) as shiftNumber, " \
                        "convert(varchar(50),shopNumber) as shopNumber," \
                        "convert(varchar(50),docNumber) as docNumber," \
                        "convert(varchar(50),cashNumber) as cashNumber," \
                        "factoryCashNumber " \
                        "from Crystals_ZReports " \
                        "where dateOperDay = '" + str(operday) + "'"
                        # "where convert(datetime,replace(dateOperDay,'+03:00','')) = '" + str(operday) + "'"
        df_zreport_base = api_utils.select_query(zreport_query, login_sql, password_sql, server_sql, driver_sql, database)

        api_utils.InsertLog('Всего за ' + str(operday) + ' z-отчётов в базе: ' + str(len(df_zreport_base)))

        # Удалить Z-отчёты, которые уже есть в базе
        if not df_zreport_base.empty:
            # Соединяем даныне из Crystals с данными из базы
            df_zreport_all = pd.merge(df_zreport, df_zreport_base,
                                      on=['shiftNumber', 'shopNumber', 'docNumber', 'cashNumber', 'factoryCashNumber'],
                                      how="outer", indicator=True)
            #df_zreport_all.to_excel("df_zreport_all.xlsx")

            # Убираем то, что есть в базе
            df_zreport_new = df_zreport_all.loc[df_zreport_all["_merge"] == "left_only"].drop("_merge", axis=1)
        else:
            df_zreport_new = df_zreport

        # df_zreport_new.to_excel("df_zreport_all.xlsx")
        api_utils.InsertLog('Всего за ' + str(operday) + ' z-отчётов необходимо загрузить в базу: ' + str(len(df_zreport_new)))

        # Вставка z-отчётов
        if df_zreport_new is not None:
            if not df_zreport_new.empty:

                # Убрать из даты +03:00  (2024-09-03+03:00)
                # df_zreport["dateOperDay"] = pd.to_datetime(df_zreport['dateOperDay'], format='%Y-%m-%d+03:00')
                # Взять первые 10 символов из строки с датой
                df_zreport_new["dateOperDay"] = df_zreport_new['dateOperDay'].str[:10]

                # Бывает null в кассире: "Игнатьева А.Н null"
                df_zreport_new['userName'] = df_zreport_new['userName'].str.replace('null', '').str.strip()

                params = urllib.parse.quote_plus("DRIVER=" + odbc_driver + ";"
                                                 "SERVER=" + server_sql + ";"
                                                 "DATABASE=" + database + ";"
                                                 "UID=" + login_sql + ";"
                                                 "PWD=" + password_sql + ";")
                engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
                df_zreport_new.to_sql("Crystals_ZReports", engine, index=False, chunksize=10000, if_exists="append")
                api_utils.InsertLog('Вставка z-отчётов: Успешно')

# IN. Методы веб-сервиса для экспорта отчетов по сторно - Отбор по фильтру (<storno-events>). КИС: Удалённые позиции
def get_stornoevents (operday):
    url_c = url_srv + '/SET-ERPIntegration/StornoExportServiceBean?wsdl'
    client = zeep.Client(url_c)
    #filter = [{'shop': '29457', 'shift': '236'}]
    filter_day = [{'operday': operday}]
    result = client.service.getByFilter(filter_day)
    file_xml = result.decode("utf-8")
    # print(file_xml)

    if '<storno-events>' not in file_xml:
        return

    df_storno = pd.read_xml(file_xml)
    api_utils.InsertLog('Storno-events. Operday: ' + str(operday) +'. В Crystals всего: ' + str(len(df_storno)))
    # df_storno.to_excel("storno-events.xlsx")

    # sys.exit()

    # Формируем ключ для данных из Crystals
    df_storno_key = df_storno[["shop", "cash", "shift", "receipt-number", "event-time", "event-type", "marking"]]

    # Забираем Storno-events из базы
    purchase_query = "select shop, cash, shift, [receipt-number], [event-time], [event-type], marking " \
                     "from Crystals_StornoEvent " \
                     "where [event-time] between '" + str(operday) + "T00:00:00' and '" + str(operday) + "T23:59:59'"
    df_storno_base = api_utils.select_query(purchase_query, login_sql, password_sql, server_sql, driver_sql, database)

    api_utils.InsertLog('Storno-events. Operday: ' + str(operday) + '. В базе всего: ' + str(len(df_storno_base)))

    # Для сравнения столбцы в обоих dataframe должны иметь один и тот же набор и порядок.
    # https://sky.pro/wiki/python/sravnenie-pandas-data-frame-poluchenie-unikalnykh-strok/
    # exclusive_df1 = df1[~df1['key_column'].isin(df2['key_column'])] - Совпадение по индексу
    comparison_df = df_storno_key.merge(df_storno_base, how='left', indicator=True)
    comparison_df_new = comparison_df[comparison_df['_merge'] == 'left_only'].drop('_merge', axis=1)
    api_utils.InsertLog('Storno-events. Operday: ' + str(operday) + '. Необходимо вставить в базу: ' + str(len(comparison_df_new)))

    # Получили ключ для новых данных
    df_storno_new_key = comparison_df_new[["shop", "cash", "shift", "receipt-number", "event-time", "event-type", "marking"]]
    # Забираем только новые данные (по новому ключу df_storno_new_key)
    df_storno_new = df_storno.merge(df_storno_new_key, on=["shop", "cash", "shift", "receipt-number", "event-time", "event-type", "marking"])

    if not df_storno_new.empty:
        params = urllib.parse.quote_plus("DRIVER=" + odbc_driver + ";"
                                         "SERVER=" + server_sql + ";"
                                         "DATABASE=" + database + ";"
                                         "UID=" + login_sql + ";"
                                         "PWD=" + password_sql + ";")

        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        df_storno_new.to_sql("Crystals_StornoEvent", engine, index=False, chunksize=10000, if_exists="append")
        api_utils.InsertLog('Storno-events. Operday: ' + str(operday) + '. Вставка в базу: Успешно')

    return


if __name__ == '__main__':
    try:
        # Получение настроек
        try:
            settings = api_utils.GetSettings()
            login_sql = settings["login_sql"]
            password_sql = settings["password_sql"]
            server_sql = settings["server_sql"]
            odbc_driver = settings["odbc_driver"]
            driver_sql = settings["driver_sql"]
            database = settings["database"]
            mails = settings["mails_to_send"].split()
            shared_folder = settings["shared_folder"]
            url_srv = settings["url_srv"]
            #url_srv_setmark = settings["url_srv_setmark"] # настройки сервера set mark в подобъекте

            # ИНН организации
            inn = settings["inn"]

            # Список колонок z-отчётов
            zreport_columns = settings["zreport_columns"].split(",")

            # Список дополнительных колонок для z-отчётов
            zreport_addcolumns = settings["zreport_addcolumns"].split(",")

            # Актуальные значения НДС. Пока '10', '20'
            tax = settings["tax"].split(",")

            # Как часто забирать продажи (по-умолчанию 1 раз в час)
            every_hours_getPurchasesByPeriod = settings["every_hours_getPurchasesByPeriod"]

            # За сколько часов забирать продажи (по-умолчанию за 1 час)
            max_hours_getPurchasesByPeriod = settings["max_hours_getPurchasesByPeriod"]

            # Когда снимать Z-отчёты (по-умолчанию в 23:40:00)
            last_hours_getZReportsByOperDay = settings["last_hours_getZReportsByOperDay"]

            # Как часто проверять задания на прогрузку кас (по-умолчанию каждые 30 сек.)
            delay_sec_check_action = settings["delay_sec_check_action"]

            # Простой приложения. Сколько секунд ждать, когда наступит новый день. Ночных продаж нет
            # if NowTime_HMS >= "00:00:00" and NowTime_HMS < '00:10:00'
            delay_sec_new_day = settings["delay_sec_new_day"]

        except:
            api_utils.InsertLog('Ошибка при получении настроек')
            time.sleep(3)
            sys.exit()

        # Создание дирректорий логов
        api_utils.CreateLogDir()

        startTime = datetime.datetime.now()
        MLog = startTime.strftime('%m')
        YLog = startTime.strftime('%Y')

        # STEEL от 04.03.2025 продажи забираются отдельной джобой каждый час. Для ждобы с продажами:
        # NowDT = datetime.datetime.now()
        # dateend = (NowDT - datetime.timedelta(hours=max_hours_getPurchasesByPeriod)).strftime("%Y-%m-%dT%H:59:59.997")
        # datebeg = (NowDT - datetime.timedelta(hours=max_hours_getPurchasesByPeriod)).strftime("%Y-%m-%dT%H:00:00")
        # api_utils.InsertLog('Получить из Crystals продажи за период: ' + str(datebeg) + ' - ' + str(dateend))
        # getPurchasesByPeriod(url_srv, datebeg, dateend)
        # sys.exit()



        # При первом запуске получить продажи, z-отчёты и удалённые позиции за текущий день, далее работать в цикле
        api_utils.InsertLog('START at ' + str(startTime))
        date_begin = startTime.strftime("%Y-%m-%dT00:00:00")
        date_end = startTime.strftime("%Y-%m-%dT23:59:59")
        api_utils.InsertLog('Импорт. Получаем данные о продажах из Crystals за период: ' + str(date_begin) + ' - ' + str(date_end))
        getPurchasesByPeriod(url_srv, date_begin, date_end)
        # #getPurchasesByPeriod(url_srv, '2025-05-21T00:00:00', '2025-05-21T23:59:59')
        # # sys.exit()

        time.sleep(1)
        api_utils.InsertLog('Импорт. Получаем данные z-отчётов из Crystals за операционный день: ' + str(date_begin))
        getZReportsByOperDay(url_srv, date_begin)


        time.sleep(1)
        api_utils.InsertLog('Импорт. Получаем данные stornoevents из Crystals за операционный день: ' + str(date_begin))
        get_stornoevents(date_begin)




        # sys.exit()


        # Если использовать schedule:
        # # Забрать задания из КИС (прогрузка касс, кассиров, марок)
        # schedule.every(delay_sec_check_action).seconds.do(do_getKISaction)
        # # Получить данные о продажах из Crystals за период
        # schedule.every(every_hours_getPurchasesByPeriod).hours.do(do_getPurchasesByPeriod)
        # #schedule.every(every_hours_getPurchasesByPeriod).seconds.do(do_getPurchasesByPeriod)
        # # Получить данные о z-отчётах из Crystals
        # schedule.every().day.at(last_hours_getZReportsByOperDay).do(do_getZReportsByOperDay)


        # ВРУЧНУЮ Z-отчёты
        # date_begin = datetime.datetime.strptime('2025-05-20', "%Y-%m-%d").date()
        # date_end = datetime.datetime.strptime('2025-05-21', "%Y-%m-%d").date()
        # print(date_begin, date_end)
        #
        # while date_begin <= date_end:
        #     print(date_begin)
        #     api_utils.InsertLog('Импорт. Получаем данные z-отчётов из Crystals за операционный день: ' + str(date_begin))
        #     getZReportsByOperDay(url_srv, date_begin)
        #     time.sleep(1)
        #     date_begin = date_begin + datetime.timedelta(days=1)

        # ВРУЧНУЮ отчёты о продажах за период!!!!
        # api_utils.InsertLog('START at ' + str(startTime))
        # date_begin = '2025-05-29T11:00:00'
        # date_end = '2025-05-29T11:59:59.997'
        # api_utils.InsertLog(
        #     'Импорт. Получаем данные о продажах из Crystals за период: ' + str(date_begin) + ' - ' + str(date_end))
        # getPurchasesByPeriod(url_srv, date_begin, date_end)
        # #

        # # ВРУЧНУЮ Забрать удалённые позиции за текущий день
        # operday = datetime.datetime.now().strftime("%Y-%m-%d")
        # get_stornoevents(operday)
        # sys.exit()


        # sys.exit()


        start_getPurchases = 0
        start_getZReports = 0
        count_getZReports = 0

        StartTime_Purchases = datetime.datetime.now()

        api_utils.InsertLog('Цикл запущен')
        while True:
            # Если использовать schedule:
            # schedule.run_pending()
            # time.sleep(1)

            time.sleep(delay_sec_check_action)

            # Продажи
            if start_getPurchases == 0:
                NowTime = datetime.datetime.now()
                elapsed_time = NowTime - StartTime_Purchases
                if elapsed_time.seconds >= every_hours_getPurchasesByPeriod * 60 * 60:
                    start_getPurchases = 1
                    StartTime_Purchases = datetime.datetime.now()

            # Если настал новый день, то сделать count_getZReports=0, чтобы в "23:40:00" вновь запросить z-отчёты
            if count_getZReports >= 2:
                NowTime_HMS = datetime.datetime.now().strftime("%H:%M:%S")
                if NowTime_HMS >= "00:00:00" and NowTime_HMS < '00:10:00':
                    count_getZReports = 0
                    start_getZReports = 0
                    # Простой программы. Ждём (обычно 5,5ч.), а потом работаем дальше (ночью никаких заданий нет)
                    time.sleep(delay_sec_new_day)

            # Z-отчёты запросить 2 раза (count_getZReports)
            if start_getZReports == 0 and count_getZReports < 2:
                NowTime_HMS = datetime.datetime.now().strftime("%H:%M:%S")
                if NowTime_HMS >= last_hours_getZReportsByOperDay:
                    start_getZReports = 1
                    count_getZReports += 1
                    if count_getZReports > 0:
                        time.sleep(delay_sec_check_action*10)

            # OUT. Задания из КИС (прогрузка касс)
            try:
                do_getKISaction()
            except:
                traceback.print_exc()
                startTime = datetime.datetime.now()
                t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
                log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+', encoding="utf-8")
                log.write(t_log + ' ## Ошибка do_getKISaction\n')
                traceback.print_exc(file=log)
                log.close()


            # STEEL от 04.03.2025 продажи забираются отдельной джобой каждый час
            # IN. Получить данные о продажах
            if start_getPurchases == 1:
                # do_getPurchasesByPeriod()
                # start_getPurchases = 0

                NowDT = datetime.datetime.now()
                dateend = (NowDT - datetime.timedelta(hours=max_hours_getPurchasesByPeriod)).strftime("%Y-%m-%dT%H:59:59.997")
                datebeg = (NowDT - datetime.timedelta(hours=max_hours_getPurchasesByPeriod)).strftime("%Y-%m-%dT%H:00:00")
                api_utils.InsertLog('Получить из Crystals продажи за период: ' + str(datebeg) + ' - ' + str(dateend))
                getPurchasesByPeriod(url_srv, datebeg, dateend)
                start_getPurchases = 0

            # IN. Получить данные о z-отчётах
            if start_getZReports == 1:
                do_getZReportsByOperDay()
                start_getZReports = 0

                # IN. Получить удалённые позиции (<storno-events>)
                operday_storno = datetime.datetime.now().strftime("%Y-%m-%d")
                get_stornoevents(operday_storno)


            # !!!!!!!! - Разовый запуск
            # # Задания из КИС (прогрузка касс)
            # do_getKISaction()
            #
            # # # IN. Получить данные о продажах из Crystals за период
            # date_begin = '2024-11-10'
            # date_end = '2024-11-11T23:59:00.000'
            # api_utils.InsertLog('Импорт. Получаем данные о продажах из Crystals за период: ' + str(date_begin) + ' - ' + str(date_end))
            # getPurchasesByPeriod(url_srv, date_begin, date_end)
            #
            # time.sleep(15)
            #
            # # IN. Получить данные о z-отчётах из Crystals - Цикл по датам
            # date_begin = datetime.datetime.strptime('2024-12-11', "%Y-%m-%d").date()
            # date_end = datetime.datetime.strptime('2024-12-12', "%Y-%m-%d").date()
            # print(date_begin, date_end)
            #
            # while date_begin <= date_end:
            #     print(date_begin)
            #     api_utils.InsertLog('Импорт. Получаем данные z-отчётов из Crystals за операционный день: ' + str(date_begin))
            #     getZReportsByOperDay(url_srv, date_begin)
            #     time.sleep(1)
            #     date_begin = date_begin + datetime.timedelta(days=1)





            # TESTS:
            # !!!
            # OUT. Обновить кассиров  ++
            # file_name = 'updateCashiers_29591.xml'
            # file = 'C:/Python/MyPrj/Norman/Crystals/OutFiles/updateCashiers_29591.xml'
            # tabNum = '2696'
            # api_utils.InsertLog('Экспорт. Обновление кассиров. tabNum=' + str(tabNum))
            # update_сashiers(url_srv, file_name, file, tabNum)
            # api_utils.InsertLog('Успешно')

            # !!!
            # OUT. Отпавить файлы из расшаренной папки \\piter-sql\Export\Crystals в Crystals
            # api_utils.InsertLog('Экспорт. Отправка файлов в Crystals')
            # send_files_to_crystals()
            # api_utils.InsertLog('Успешно')

            # !!!
            # OUT. Отпавить марки в set mark
            # api_utils.InsertLog('Экспорт. Отправка марок в set mark')
            # SubObjectID = 29591
            # #mark = '193402170508920322001RGHE5KPZZGEU6Z3YVL6LUYL64EYW34UCISBAERN2D7HC7E2A6OXKW4N7HWFB4HNO6RXVGTM44Z7DP274KROBBOXBUHN2AFXSIBYJMKBMKCPXTGUXTWT3R23CQY43FFW3I'
            # send_files_to_setmark(SubObjectID)

            # !!!
            # # IN. Получить данные о продажах из Crystals за операционный день (для сверки - всё ли есть в базе)
            # OperDay = '2024-09-03'
            # api_utils.InsertLog('Импорт. Получаем данные о продажах из Crystals за операционный день: ' + str(OperDay))
            # getPurchasesByOperDay(url_srv, OperDay)
            #
            # # !!!
            # # IN. Получить данные о z-отчётах из Crystals за операционный день
            # OperDay = '2024-10-01'  #06
            # api_utils.InsertLog('Импорт. Получаем данные z-отчётов из Crystals за операционный день: ' + str(OperDay))
            # getZReportsByOperDay(url_srv, OperDay)

            # sys.exit()

    except:
        traceback.print_exc()
        startTime = datetime.datetime.now()
        t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
        log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+', encoding="utf-8")
        log.write(t_log + ' ## Ошибка в процедуре Parser_main.py\n')
        traceback.print_exc(file=log)
        log.close()
        time.sleep(10)
        sys.exit()

