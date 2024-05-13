import db_data
import datetime
import make_xls
import mail_sender
import configparser
import logging

#Чтение конфигов
config = configparser.ConfigParser()
config.read('config.ini')
config.sections()
smpt_server = config.get('Settings',"MailServer")
smtp_port = config.get('Settings',"MailServerPort")
from_address = config.get("Settings", "AddressFrom")
paswd = config.get('Settings',"PasswordFrom")
count_days = config.get('Settings',"CountDays")
dbname = config.get('Settings', 'dbName')
dbhost = config.get('Settings', 'dbHost')
dblogin = config.get('Settings', 'dblogin')
dbpaswd = config.get('Settings', 'dbpassword')
dbtype = config.get('Settings', 'subdtype')
enable_send = config.get('Settings', 'Enable_Mail_send')
enable_ape = config.get('Settings','Enable_APE')


now = datetime.datetime.now()

log = logging.basicConfig(filename=f'log-{now.strftime("%Y-%m-%d")}.log', level=logging.DEBUG, encoding='ansi', datefmt='%Y-%m-%d %H:%M:%S')
new_date = now - datetime.timedelta(days=int(count_days))



query = db_data.query(new_date=new_date, now=now)
query_svod = db_data.query_svod(new_date=new_date,now=now)
count_query = db_data.count_query(new_date=new_date, now=now)
error_packets = db_data.select_error_packets()
get_receivers_query = """select * from grk_pso_report_receiver"""
insert_table_error_packets = db_data.error_packets(new_date, now)

# общий отчет
logging.info('start message')
if dbtype == 'pgsql':
    logging.info('db type pgsql')
    receivers_list = db_data.get_query(db_name=dbname, username=dblogin,password=dbpaswd, host=dbhost,query=get_receivers_query)
    for receiver in receivers_list:
        if receiver[0] == 1:
            export = db_data.get_query(db_name=dbname, username=dblogin,password=dbpaswd, host=dbhost,query=query)
            rows = export.fetchall()
            export.close()
            error_packets_insert = db_data.get_query(db_name=dbname, username=dblogin,password=dbpaswd, host=dbhost,query=insert_table_error_packets)
            error_packets_insert.connection.commit()
            error_packets_insert.close()
            export_svod = db_data.get_query(db_name=dbname, username=dblogin,password=dbpaswd, host=dbhost,query=query_svod)
            export_svod_rows = export_svod.fetchall()
            export_svod.close()
            error_packets_fetch = db_data.get_query(db_name=dbname, username=dblogin,password=dbpaswd, host=dbhost,query=error_packets)
            error_packets_rows= error_packets_fetch.fetchall()
            error_packets_fetch.close()
            count_packets = db_data.get_query(db_name=dbname, username=dblogin,password=dbpaswd, host=dbhost,query=count_query)
            count_rows = count_packets.fetchall()
            count_packets.close()
            if rows == []:
                print('Выгрузка пустая')
                logging.info("Выгрузка пустая")
            else:
                make_xls.exportDataxls(rows, now, newdate=new_date, rows1=count_rows, svod_rows=export_svod_rows, error_rows=error_packets_rows)
                files = [
                    f".\\reports\\Отчет ПСО отправленные док-ты-(с {new_date.strftime('%Y-%m-%d')} по {now.strftime('%Y-%m-%d')}).xls"]
                msg_subj = f"Отчет о доставке писем по ПСО {receiver[3]}"
                msg_text = "Добрый день!\nВам направляется выгрузка по отправленным пакетам, во вложении"
                if enable_send == 'True':
                    mail_sender.send_email(receiver[1],
                                       msg_subj,
                                       msg_text,
                                       files,
                                       receiver[2],
                                       smpt_server,
                                       smtp_port,
                                       from_address,
                                       paswd)
                else:
                    print('Отправка выключена,поменяйте в конфиге Enable_Mail_send ')
                    logging.info('Отправка выключена,поменяйте в конфиге Enable_Mail_send ')
                for error_row in error_packets_rows:

                    if error_row[0] == 'Выгружен' and error_row[7] == 'Зарегистрирован':
                        delete_query = db_data.delete_error_packet(error_row[9])
                        delete_cur = db_data.get_query(db_name=dbname, username=dblogin,password=dbpaswd, host=dbhost,query=delete_query)
                        delete_cur.connection.commit()
                        delete_cur.close()

        elif receiver[0] == 2:
            if enable_ape == 'True':
                ape_query = db_data.docs_WO_resolution_APE()
                docs_wo_resolution = db_data.get_query(db_name=dbname, username=dblogin, password=dbpaswd, host=dbhost, query=ape_query)
                rows = docs_wo_resolution.fetchall()
                if rows == []:
                    print('Выгрузка  по резолюциям Артюхина Р.Е. пустая')
                    logging.info("Выгрузка по резолюциям Артюхина Р.Е. пустая")
                else:
                    make_xls.create_report_APE(rows,now)
                    files = [f".\\reports\\Список документов без резолюции Артюхин Р.Е. {now.strftime('%Y-%m-%d')}.xls"]
                    msg_subj = f"Отчет о документах без резолюции Артюхин Р.Е."
                    msg_text = "Добрый день!\nВам направляется выгрузка по документам без резолюции Артюхин Р.Е."
                    if enable_send == 'True':
                        mail_sender.send_email(receiver[1],
                                       msg_subj,
                                       msg_text,
                                       files,
                                       receiver[2],
                                       smpt_server,
                                       smtp_port,
                                       from_address,
                                       paswd)
            else :
                print('Выгрузка АРЕ выключена')
                logging.info("Выгрузка АРЕ выключена")
        else:
            query_by_dep_id = db_data.query_by_department(new_date,now,receiver[0])
            count_query_by_dep_id = db_data.count_query_by_department(new_date,now,receiver[0])
            export = db_data.get_query(db_name=dbname, username=dblogin, password=dbpaswd, host=dbhost, query=query_by_dep_id)

            rows = export.fetchall()
            count_packets = db_data.get_query(db_name=dbname, username=dblogin, password=dbpaswd, host=dbhost,
                                              query=count_query_by_dep_id)
            count_rows = count_packets.fetchall()
            if rows == []:
                print('Выгрузка отправленных пакетов пустая')
                logging.info("Выгрузка отправленных пакетов пустая")
            else:
                make_xls.exportDataxls(rows, now, newdate=new_date, rows1=count_rows)
                files = [
                    f".\\reports\\Отчет ПСО отправленные док-ты-(с {new_date.strftime('%Y-%m-%d')} по {now.strftime('%Y-%m-%d')}).xls"]
                msg_subj = f"Отчет о доставке писем по ПСО : {receiver[3]}"
                msg_text = "Добрый день!\nВам направляется выгрузка по отправленным пакетам, во вложении"
                if enable_send == 'True':
                    mail_sender.send_email(receiver[1],
                                       msg_subj,
                                       msg_text,
                                       files,
                                       receiver[2],
                                       smpt_server,
                                       smtp_port,
                                       from_address,
                                       paswd)
                else:
                    print('Отправка выключена, поменяйте в конфиге Enable_Mail_send ')
                    logging.info('Отправка выключена,поменяйте в конфиге Enable_Mail_send ')
elif dbtype == 'mssql':
    receivers_list = db_data.get_query_mssql(db_name=dbname, username=dblogin, password=dbpaswd, host=dbhost, query=get_receivers_query)
    for receiver in receivers_list:
        if receiver[0] == 1:
            export = db_data.get_query_mssql(db_name=dbname, username=dblogin, password=dbpaswd, host=dbhost, query=query)
            rows = export.fetchall()
            count_packets = db_data.get_query_mssql(db_name=dbname, username=dblogin, password=dbpaswd, host=dbhost,
                                              query=count_query)
            count_rows = count_packets.fetchall()
            if rows == []:
                print('Выгрузка пустая')
            else:
                make_xls.exportDataxls(rows, now, newdate=new_date, rows1=count_rows)
                files = [
                    f".\\reports\\Отчет ПСО отправленные док-ты-(с {new_date.strftime('%Y-%m-%d')} по {now.strftime('%Y-%m-%d')}).xls"]
                msg_subj = f"Отчет о доставке писем по ПСО : {receiver[3]}"
                msg_text = "Добрый день!\nВам направляется выгрузка по отправленным пакетам, во вложении"
                if enable_send == 'True':
                    mail_sender.send_email(receiver[1],
                                       msg_subj,
                                       msg_text,
                                       files,
                                       receiver[2],
                                       smpt_server,
                                       smtp_port,
                                       from_address,
                                       paswd)
                else:
                    print('Отправка выключена, поменяйте в конфиге Enable_Mail_send ')
                    logging.info('Отправка выключена,поменяйте в конфиге Enable_Mail_send ')
        else:
            query_by_dep_id = db_data.query_by_department(new_date, now, receiver[0])
            count_query_by_dep_id = db_data.count_query_by_department(new_date, now, receiver[0])
            export = db_data.get_query_mssql(db_name=dbname, username=dblogin, password=dbpaswd, host=dbhost,
                                       query=query_by_dep_id)

            rows = export.fetchall()
            count_packets = db_data.get_query_mssql(db_name=dbname, username=dblogin, password=dbpaswd, host=dbhost,
                                              query=count_query_by_dep_id)
            count_rows = count_packets.fetchall()
            if rows == []:
                print('Выгрузка пустая')
                logging.info('Выгрузка пустая')
            else:
                make_xls.exportDataxls(rows, now, newdate=new_date, rows1=count_rows)
                files = [
                    f".\\reports\\Отчет ПСО отправленные док-ты-(с {new_date.strftime('%Y-%m-%d')} по {now.strftime('%Y-%m-%d')}).xls"]
                msg_subj = f"Отчет о доставке писем по ПСО : {receiver[3]}"
                msg_text = "Добрый день!\nВам направляется выгрузка по отправленным пакетам, во вложении"
                if enable_send == 'True':
                    mail_sender.send_email(receiver[1],
                                       msg_subj,
                                       msg_text,
                                       files,
                                       receiver[2],
                                       smpt_server,
                                       smtp_port,
                                       from_address,
                                       paswd)
                else:
                    print('Отправка выключена, поменяйте в конфиге Enable_Mail_send ')
                    logging.info('Отправка выключена,поменяйте в конфиге Enable_Mail_send ')



