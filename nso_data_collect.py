import sys
import requests
import json
import psycopg2
import psycopg2.extras
import datetime
import pandas as pd
import db_env as db


def get_date():

    today = datetime.date.today()

    date_1 = pd.to_datetime(str(today), format="%Y-%m-%d") - pd.DateOffset(months=2)

    fm_for_nso = date_1.strftime("%Y%m")

    return fm_for_nso


def conn_postgis(postgres_db, postgres_user, postgres_server, postgres_password, data_layer, port=None):
    try:
        connection_str = "dbname={db} user={user} host={host} password={password}{port_section}".format(
            db=postgres_db,
            user=postgres_user,
            host=postgres_server,
            password=postgres_password,
            port_section="" if port is None else " port={}".format(port),
        )
        return psycopg2.connect(connection_str)
    except Exception as e:
        data_layer['status'] = 'Error: ' + str(e)
        print(json.dumps(data_layer, ensure_ascii=False))
        sys.exit(1)


def get_haystack_price(Period):
    value = {
        "tbl_id": "DT_NSO_1002_001V5",
        "Period": [Period]
    }

    header = {"Content-Type": "application/json"}
    data = json.dumps(value).encode("utf-8")

    response = requests.post("http://opendata.1212.mn/api/data", data=data, headers=header)

    if (response.status_code == 200):
        print(response.content)
        print("\n")

    d = json.loads(response.content)

    for resp in d["DataList"]:
        for obj in resp:
            haystack_list.append(HayStack(tbl_id=resp['TBL_ID'],period=resp['Period'],scr_mn=resp['SCR_MN'], code1=resp['CODE1'],scr_mn1=resp['SCR_MN1'] ,dtval_co=resp['DTVAL_CO']))
            # print("response=", resp["CODE1"])
    # select_haystack()

    return haystack_list


data_file = []


# get hay_stack table from postgis database
def select_haystack():
    data_layer = {'data': {}}
    try:

        con_pg_ext = conn_postgis(db.database, db.username, db.hostname, db.password, 'hay_price',
                                  port=None)
        cur_pg_ext = con_pg_ext.cursor(cursor_factory=psycopg2.extras.DictCursor)
        #template sort of thing please check for the latest table
        cur_pg_ext.execute("select ids,ST_AsText(geom),id0,name_lmn,name_l1,aimgiin_co,soum_code,code3,date,hay_pric_1,hay_pric_2,hay_pric_3,hay_pric_4,hay_pric_5,hay_pric_6 from hay_price where date like '202001'")
        data_file = cur_pg_ext.fetchall()
        # data_file = [row for row in cur_pg_ext]
        cur_pg_ext.close()

    except Exception as e:
        print(str(e))

    return data_file


# later for some occassion need to be developed
haystack_list = []


def replace_str():
    for i in haystack_list:
        print(i)

    return None

# join to class list & postgis hay_stack table
def join_field_calc(new_data_file, data_file):
    # cannot use pandas merge function use manual process
    try:

        for row in data_file:
            for new_row in new_data_file:
                if str(row['code3']) == str(new_row.code1) and (str(row['date']) != str(new_row.period)):

                    # should also state that date is equal do not need to add to the database just leave it
                        insert_haystack_table(new_row, row['geom'])
    except Exception as e:
        print(str(e))

    return None


# update postgis database also with the id each table

def insert_haystack_table(table_data,geom):
    try:
        con_pg_ext = conn_postgis(db.database, db.username, db.hostname, db.password, 'hay_price',
                                  port=None)
        cursor = con_pg_ext.cursor()
        # have to check  for null number
        cursor.execute("INSERT INTO hay_price (hay_price,hay_pric_1, code3, hay_pric_4,hay_pric_6,geom) VALUES(%s, %s, %s, %s, %s,ST_AsText(%s))", (table_data.period,table_data.scr_mn,table_data.code1,table_data.scr_mn1,table_data.dtval_co,geom))
        con_pg_ext.commit()
        cursor.close()
        con_pg_ext.close()

    except Exception as e:
        print(str(e))

    return None


class HayStack:
    def __init__(self, tbl_id, period, scr_mn, code1,scr_mn1, dtval_co):
        self.tbl_id = tbl_id
        self.period = period
        self.scr_mn=scr_mn
        self.code1 = code1
        self.scr_mn1 = scr_mn1
        self.dtval_co = dtval_co


if __name__ == "__main__":

    data_layer = {'data': {}}

    try:

        period = get_date()

        print(period)

        new_data = get_haystack_price(period)

        data = select_haystack()

        join_field_calc(new_data, data)

    except Exception as e:
        print(str(e))

