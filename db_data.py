import psycopg2
import pyodbc as sql

def query(new_date, now):
    QUERY = (f"""select qs."Name" ,v1."Name" \"Отправитель\", l2.\"Name\"  \"получатель\"  ,l.\"DocN\" \"Номер документа\", ps.\"Name\", q.\"InsertDateTime\" 
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
 order by q.\"ID\"""" )
    return QUERY

def count_query(new_date, now):
    COUNT_QUERY = f"""select count(q."ID"), ps.\"Name\" 
from ldexchangequeue q 
join ldexchangestate qs on q."StateID" = qs."ID"
join ldexchangeobject eo on eo.\"ExportItemID\" =q.\"ID\" 
left join ldexchangereceiver r on r.\"ExportItemID\" =q.\"ID\" 
left join ldvocabulary l2 on l2.\"ID\" =r.\"MemberID\" 
join ldmail m on m.\"ID\" =eo.\"ObjectID\" 
join lderc l on l.\"ID\" =m.\"BaseERCID\" 
join ldobject ob on ob."ID" =l."ID" 
left join cls_esexportpacket ce on ce."PacketUid" = r."PacketUid"
left join cls_espacketstate ps on ps."Id" = ce."StateId" 
where q."InsertDateTime" > '{str(new_date.strftime('%Y%m%d'))}' and q."InsertDateTime" < '{str(now.strftime('%Y%m%d'))}' and
 q."Tag" ='Outgoing docs'
 group by ps.\"Name\""""
    return COUNT_QUERY

def query_by_department(new_date, now, dep_id):
    QUERY_BY_department = f"""select qs."Name" ,v1.\"Name\" \"Отправитель\", l2.\"Name\"  \"получатель\"  ,l.\"DocN\" \"Номер документа\", ps.\"Name\", q.\"InsertDateTime\"    
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
    select j."Name" , doc."RegDate" ,doc."DocN"  from dbo.lderc doc
join dbo.ldjournal j on  j."ID" =doc."JournalID"
			join dbo.ldobjecttype ot on ot."ID" = j."CardTypeID"
 where
    doc."StateID" =1
    and ot."ID" in(2006,2007)
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
			AND mt."ID" = 927570 --select * from ldmailstate where "ID" in (1,11)
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