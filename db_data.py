import psycopg2
import pyodbc as sql

def query(new_date, now):
    QUERY = (f"""select 
    qs."Name" ,
    v2."Name" \"Отправитель\",
    v3."Name" "Департамент отправителя",
     l2.\"Name\"  \"получатель\",
      v4."Name" "Департамент получателя",
      l.\"DocN\" \"Номер документа\",
      j.\"Name\" \"Наименование журнала регистрации\", 
    case when ps."Name" ='Отказ в регистрации' then 'Ошибка на стороне получателя' else ps."Name" end,
     q.\"InsertDateTime\" 
from ldexchangequeue q 
join ldexchangestate qs on q."StateID" = qs."ID"
join ldexchangeobject eo on eo.\"ExportItemID\" =q.\"ID\" 
left join ldexchangereceiver r on r.\"ExportItemID\" =q.\"ID\" 
left join ldvocabulary l2 on l2.\"ID\" =r.\"MemberID\" 
left join lduser u1 on l2."ID" = u1."ID"
left join ldvocabulary v4 on v4."ID"=u1."DepartmentID" 
join ldmail m on m.\"ID\" =eo.\"ObjectID\" 
join lderc l on l.\"ID\" =m.\"BaseERCID\" 
join ldjournal j on j.\"ID\" = l.\"JournalID\"
join ldvocabulary v2 on v2."ID" = m."CreatorID" 
left join lduser u on v2."ID" = u."ID"
left join ldvocabulary v3 on v3."ID"=u."DepartmentID" 
join ldobject ob on ob."ID" =l."ID" 
left join cls_esexportpacket ce on ce."PacketUid" = r."PacketUid"
left join cls_espacketstate ps on ps."Id" = ce."StateId" 
where q."InsertDateTime" > '{str(new_date.strftime('%Y%m%d'))}' and q."InsertDateTime" < '{str(now.strftime('%Y%m%d'))}' and
 q."Tag" ='Outgoing docs' and (qs."Name" !='Выгружен' or(qs."Name" ='Выгружен' and r."PacketUid"  is not null))
 order by q.\"ID\";
""" )
    return QUERY
def error_packets(new_date, now):
    QUERY = f"""INSERT INTO ldexchange_packet_error("ID", "QPacketState", "EPacketState" )
    select q."ID", q."StateID", ce."StateId"
from ldexchangequeue q 
left join ldexchangereceiver r on r.\"ExportItemID\" =q.\"ID\" 
left join cls_esexportpacket ce on ce."PacketUid" = r."PacketUid"
left join cls_espacketstate ps on ps."Id" = ce."StateId" 
where not exists(
Select null
from ldexchange_packet_error
where ldexchange_packet_error."ID" = q."ID"
) and q."InsertDateTime" > '{str(new_date.strftime('%Y%m%d'))}' and q."InsertDateTime" < '{str(now.strftime('%Y%m%d'))}' and
 q."Tag" ='Outgoing docs' and (q."StateID" = 4 or ce."StateId" in(4,5,8,12))
 order by q.\"ID\""""
    return QUERY

def select_error_packets():
    sql = """select qs."Name" ,v2."Name" \"Отправитель\",v3."Name" "Департамент отправителя",
     l2.\"Name\"  \"получатель\" , v4."Name" "Департамент получателя" ,l.\"DocN\" \"Номер документа\",j.\"Name\" \"Наименование журнала регистрации\",
     case when ps."Name" ='Отказ в регистрации' then 'Ошибка на стороне получателя' else ps."Name" end,
      q.\"InsertDateTime\", q."ID"     
from ldexchangequeue q 
join ldexchange_packet_error pe on pe."ID" = q."ID"
join ldexchangestate qs on q."StateID" = qs."ID"
join ldexchangeobject eo on eo.\"ExportItemID\" =q.\"ID\" 
left join ldexchangereceiver r on r.\"ExportItemID\" =q.\"ID\" 
left join ldvocabulary l2 on l2.\"ID\" =r.\"MemberID\" 
left join lduser u1 on l2."ID" = u1."ID"
left join ldvocabulary v4 on v4."ID"=u1."DepartmentID" 
join ldmail m on m.\"ID\" =eo.\"ObjectID\" 
join lderc l on l.\"ID\" =m.\"BaseERCID\" 
join ldjournal j on j.\"ID\" = l.\"JournalID\"
join ldvocabulary v2 on v2."ID" = m."CreatorID"
left join lduser u on v2."ID" = u."ID"
left join ldvocabulary v3 on v3."ID"=u."DepartmentID" 
join ldobject ob on ob."ID" =l."ID" 
left join cls_esexportpacket ce on ce."PacketUid" = r."PacketUid"
left join cls_espacketstate ps on ps."Id" = ce."StateId" 
where (qs."Name" !='Выгружен' or(qs."Name" ='Выгружен' and r."PacketUid"  is not null))
 order by q.\"ID\""""

    return sql
def delete_error_packet(packet_id):
    query =f"""delete from ldexchange_packet_error where "ID" = {str(packet_id)}"""
    return query
def query_svod(new_date, now):
    QUERY = f"""select t."Номер документа", t."Наименование журнала регистрации",t."Отправитель", t."Департамент отправителя",
"InsertDateTime" as "Дата и время отправки",
		count(t."Номер документа") as "Кол-во отправленных пакетов", 
		sum(case when t."PacketState" ='Зарегистрирован' then 1 else 0 end) "Зарегистрирован",
		sum(case when t."PacketState" ='Отправлен' then 1 else 0 end) "Отправлен",
		sum(case when t."PacketState" ='Доставлен' then 1 else 0 end) "Доставлен",
		sum(case when t."PacketState" ='Отказ в регистрации' then 1 else 0 end) "Ошибка на стороне получателя",
		sum(case when t."PacketState" ='Ошибка отправки' then 1 else 0 end) "Ошибка отправки",
		sum(case when t."ExportState" ='Ошибка выгрузки' then 1 else 0 end) "Ошибка выгрузки",
		sum(case when t."ExportState" ='Отменён' then 1 else 0 end) "Отменён"
from 
(select qs."Name" as "ExportState",v2."Name" "Отправитель", v3."Name"  "Департамент отправителя"  ,l."DocN" "Номер документа",j."Name" "Наименование журнала регистрации", ps."Name" as "PacketState", 
to_char(q."InsertDateTime",'dd.MM.yyyy HH:MI') "InsertDateTime"
from ldexchangequeue q 
join ldexchangestate qs on q."StateID" = qs."ID"
join ldexchangeobject eo on eo."ExportItemID" =q."ID" 
left join ldexchangereceiver r on r."ExportItemID" =q."ID" 
left join ldvocabulary l2 on l2."ID" =r."MemberID" 
join ldmail m on m."ID" =eo."ObjectID" 
join lderc l on l."ID" =m."BaseERCID" 
join ldjournal j on j."ID" = l."JournalID"
join ldvocabulary v1 on v1."ID" = j."DepartmentID"
join ldvocabulary v2 on v2."ID" = m."CreatorID"
left join lduser u on v2."ID" = u."ID"
left join ldvocabulary v3 on v3."ID"=u."DepartmentID" 
join ldobject ob on ob."ID" =l."ID" 
left join cls_esexportpacket ce on ce."PacketUid" = r."PacketUid"
left join cls_espacketstate ps on ps."Id" = ce."StateId" 
where q."InsertDateTime" > '{str(new_date.strftime('%Y%m%d'))}' and q."InsertDateTime" < '{str(now.strftime('%Y%m%d'))}' and
 q."Tag" ='Outgoing docs' and (qs."Name" !='Выгружен' or(qs."Name" ='Выгружен' and r."PacketUid"  is not null))
 order by q."InsertDateTime") as t
 group by t."Номер документа", t."Наименование журнала регистрации",t."Отправитель", t."Департамент отправителя",
"InsertDateTime" order by "InsertDateTime" asc """
    return QUERY
def count_query(new_date, now):
    COUNT_QUERY = f"""select
count(q."ID"), case when ps."Name" is null then qs."Name"
                when ps."Name" = 'Отказ в регистрации' then 'Ошибка на стороне получателя'
                else ps."Name" end
from ldexchangequeue q
join ldexchangestate qs on q."StateID" = qs."ID"
join ldexchangeobject eo on eo."ExportItemID" =q."ID"
left join ldexchangereceiver r on r."ExportItemID" =q."ID"
left join ldvocabulary l2 on l2."ID" =r."MemberID"
join ldmail m on m."ID" =eo."ObjectID"
join lderc l on l."ID" =m."BaseERCID"
join ldobject ob on ob."ID" =l."ID"
left join cls_esexportpacket ce on ce."PacketUid" = r."PacketUid"
left join cls_espacketstate ps on ps."Id" = ce."StateId"
where q."InsertDateTime" > '{str(new_date.strftime('%Y%m%d'))}' and q."InsertDateTime" < '{str(now.strftime('%Y%m%d'))}' and
 q."Tag" ='Outgoing docs'  and (r."PacketUid" is null or ce."Id" is not null)
 group by case when ps."Name" is null then qs."Name"
                when ps."Name" = 'Отказ в регистрации' then 'Ошибка на стороне получателя'
                else ps."Name" end """
    return COUNT_QUERY

def query_by_department(new_date, now, dep_id):
    QUERY_BY_department = f"""select qs."Name" ,v1.\"Name\" \"Отправитель\", l2.\"Name\"  \"получатель\"  ,l.\"DocN\" \"Номер документа\",j.\"Name\" \"Наименование журнала регистрации\", ps.\"Name\", q.\"InsertDateTime\"    
from ldexchangequeue q 
join ldexchangestate qs on q."StateID" = qs."ID"
join ldexchangeobject eo on eo.\"ExportItemID\" =q.\"ID\" 
left join ldexchangereceiver r on r.\"ExportItemID\" =q.\"ID\" 
left join ldvocabulary l2 on l2.\"ID\" =r.\"MemberID\" 
join ldmail m on m.\"ID\" =eo.\"ObjectID\" 
join lderc l on l.\"ID\" =m.\"BaseERCID\" 
join ldjournal j on j.\"ID\" = l.\"JournalID\"
join ldvocabulary v1 on v1."ID" = j."DepartmentID"
join ldobject ob on ob."ID" =l."ID" 
left join cls_esexportpacket ce on ce."PacketUid" = r."PacketUid"
left join cls_espacketstate ps on ps."Id" = ce."StateId" 
where q."InsertDateTime" > '{str(new_date.strftime('%Y%m%d'))}' and q."InsertDateTime" < '{str(now.strftime('%Y%m%d'))}' and
 q."Tag" ='Outgoing docs'
 and v1."ID" = {dep_id} order by q.\"ID\""""
    return QUERY_BY_department
def count_query_by_department(new_date, now, dep_id):
    COUNT_QUERY_BY_department = f"""select count(q."ID"), ps.\"Name\"
from ldexchangequeue q 
join ldexchangestate qs on q."StateID" = qs."ID"
join ldexchangeobject eo on eo.\"ExportItemID\" =q.\"ID\" 
left join ldexchangereceiver r on r.\"ExportItemID\" =q.\"ID\" 
left join ldvocabulary l2 on l2.\"ID\" =r.\"MemberID\" 
join ldmail m on m.\"ID\" =eo.\"ObjectID\" 
join lderc l on l.\"ID\" =m.\"BaseERCID\" 
join ldjournal j on j.\"ID\" = l.\"JournalID\"
join ldvocabulary v1 on v1."ID" = j."DepartmentID"
join ldobject ob on ob."ID" =l."ID" 
left join cls_esexportpacket ce on ce."PacketUid" = r."PacketUid"
left join cls_espacketstate ps on ps."Id" = ce."StateId" 
where q."InsertDateTime" > '{str(new_date.strftime('%Y%m%d'))}' and q."InsertDateTime" < '{str(now.strftime('%Y%m%d'))}' and
 q."Tag" ='Outgoing docs'
 and v1."ID" = {dep_id} group by ps.\"Name\""""
    return COUNT_QUERY_BY_department
def get_query(db_name, username,password,host,query):
    connect = psycopg2.connect(dbname=db_name, user=username, password=password, host=host,)
    cur = connect.cursor()
    cur.execute(query=query)

    return cur


def docs_WO_resolution_APE():
    QUERY_APE = """
    select j."Name" , doc."RegDate" ,doc."DocN" from lderc doc
    join ldjournal j on j."ID"= doc."JournalID"
         join ldobjecttype ot on ot."ID" = j."CardTypeID"
         where
			 ot."ID" in (2006,2007)
			and doc."StateID"=1
	AND EXISTS(
		SELECT NULL
		FROM dbo.LDMail m
			join dbo.GRK_MailReport mr on
				mr."MailID" = m."ID" and
				mr."DocID" = m."ERCID"
			join dbo.ldmailstate ms on
				ms."ID" = m."MailStateID"
			join dbo.ldmailtype mt on
				mt."ID" = m."MailTypeID"
			join dbo.ldvocabulary vi on
				vi."ID" = m."InitiatorID"
		WHERE
			m."ERCID" = doc."ID"
			AND vi."ID" = 341397
			AND mt."ID" = 927570
			AND ms."ID" IN (1,11)
			AND mr."ARMDocState" = 2
	)
	and not exists(select null from dbo.ldversion ver where ver."DocID" = doc."ID" and ver."FileName" ilike 'Резолюция руководителя')
order by doc."RegDate" desc
    """
    return QUERY_APE
def get_query_mssql(db_name, username,password,host,query): #connect to sql server
    server = 'tcp:'+host
    cnxn = sql.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+db_name+';UID='+username+';PWD='+password)
    cursor = cnxn.cursor()
    cursor.execute(query)
    return cursor