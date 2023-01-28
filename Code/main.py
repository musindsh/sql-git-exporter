import pyodbc
import sys
import os
import io
import git
from git import Repo
from git import Git
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import shutil
import config
import schedule
import time


directory = config.Config.directory

fromaddr = config.Config.fromaddr
toaddr = config.Config.toaddr
msg = MIMEMultipart()
msg['From'] = fromaddr
msg['To'] = ', '.join(toaddr)
msg['Subject'] = "Выгрузка кода SQL Server"
server = smtplib.SMTP('10.10.10.10:25')


class ProgressPrinter(git.RemoteProgress):

    def line_dropped(self, line):
        print("line dropped : " + str(line))


def export():
    DB_Login = config.Config.DB_Login
    DB_Pass = config.Config.DB_Pass

    if not os.path.exists(directory):
        os.mkdir(directory)
    else:
        shutil.rmtree(directory)
        os.mkdir(directory)

    try:
        print('Клонирование репозитория...')
        Repo.clone_from(config.Config.remote, directory, branch='master',
                        env={'GIT_SSH_COMMAND': config.Config.git_ssh_cmd})
    except Exception as git_err:
        body = 'Ошибка при клонировании репозитория во временную папку:\n\n' + str(git_err)
        msg.attach(MIMEText(body, 'plain'))
        server.starttls()
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()
        print('Клонирование не удалось' + str(git_err))

    if not os.path.exists(directory + "/Server Security"):
        os.mkdir(directory + "/Server Security")
    else:
        shutil.rmtree(directory + "/Server Security")
        os.mkdir(directory + "/Server Security")
    if not os.path.exists(directory + "/Server Security/Logins"):
        os.mkdir(directory + "/Server Security/Logins")
    if not os.path.exists(directory + "/Server Security/Logins/SQL"):
        os.mkdir(directory + "/Server Security/Logins/SQL")
    if not os.path.exists(directory + "/Server Security/Logins/Windows"):
        os.mkdir(directory + "/Server Security/Logins/Windows")
    if not os.path.exists(directory + "/Server Security/Roles"):
        os.mkdir(directory + "/Server Security/Roles")
    if not os.path.exists(directory + "/Server Security/Triggers"):
        os.mkdir(directory + "/Server Security/Triggers")
    i = 0

    try:
        DBs = config.Config.DBs
        for i in range(len(DBs)):
            DB = DBs[i][0];
            host = DBs[i][1];
            if not os.path.exists(directory + "/" + str(DB)):
                os.mkdir(directory + "/" + str(DB))
            else:
                shutil.rmtree(directory + "/" + str(DB))
                os.mkdir(directory + "/" + str(DB))
            if not os.path.exists(directory + "/" + str(DB) + "/Stored Procedures"):
                os.mkdir(directory + "/" + str(DB) + "/Stored Procedures")
            if not os.path.exists(directory + "/" + str(DB) + "/Tables"):
                os.mkdir(directory + "/" + str(DB) + "/Tables")
            if not os.path.exists(directory + "/" + str(DB) + "/Triggers"):
                os.mkdir(directory + "/" + str(DB) + "/Triggers")
            if not os.path.exists(directory + "/" + str(DB) + "/Database Triggers"):
                os.mkdir(directory + "/" + str(DB) + "/Database Triggers")
            if not os.path.exists(directory + "/" + str(DB) + "/Views"):
                os.mkdir(directory + "/" + str(DB) + "/Views")
            if not os.path.exists(directory + "/" + str(DB) + "/Functions"):
                os.mkdir(directory + "/" + str(DB) + "/Functions")
            if not os.path.exists(directory + "/" + str(DB) + "/Functions/Table-valued Functions"):
                os.mkdir(directory + "/" + str(DB) + "/Functions/Table-valued Functions")
            if not os.path.exists(directory + "/" + str(DB) + "/Functions/Scalar-valued Functions"):
                os.mkdir(directory + "/" + str(DB) + "/Functions/Scalar-valued Functions")
            if not os.path.exists(directory + "/" + str(DB) + "/Security"):
                os.mkdir(directory + "/" + str(DB) + "/Security")
            if not os.path.exists(directory + "/" + str(DB) + "/Security/Roles"):
                os.mkdir(directory + "/" + str(DB) + "/Security/Roles")
            if not os.path.exists(directory + "/" + str(DB) + "/Security/Users"):
                os.mkdir(directory + "/" + str(DB) + "/Security/Users")
            if not os.path.exists(directory + "/" + str(DB) + "/Security/Users/Windows"):
                os.mkdir(directory + "/" + str(DB) + "/Security/Users/Windows")
            if not os.path.exists(directory + "/" + str(DB) + "/Security/Users/SQL"):
                os.mkdir(directory + "/" + str(DB) + "/Security/Users/SQL")
            if not os.path.exists(directory + "/" + str(DB) + "/Service Broker"):
                os.mkdir(directory + "/" + str(DB) + "/Service Broker")
            if not os.path.exists(directory + "/" + str(DB) + "/Service Broker/Queues"):
                os.mkdir(directory + "/" + str(DB) + "/Service Broker/Queues")
            if not os.path.exists(directory + "/" + str(DB) + "/Service Broker/XML Schemas"):
                os.mkdir(directory + "/" + str(DB) + "/Service Broker/XML Schemas")
            if not os.path.exists(directory + "/" + str(DB) + "/Service Broker/Contracts"):
                os.mkdir(directory + "/" + str(DB) + "/Service Broker/Contracts")
            if not os.path.exists(directory + "/" + str(DB) + "/Service Broker/Message types"):
                os.mkdir(directory + "/" + str(DB) + "/Service Broker/Message types")
            if not os.path.exists(directory + "/" + str(DB) + "/Service Broker/Services"):
                os.mkdir(directory + "/" + str(DB) + "/Service Broker/Services")
            if not os.path.exists(directory + "/" + str(DB) + "/Sequences"):
                os.mkdir(directory + "/" + str(DB) + "/Sequences")

            cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};TrustServerCertificate=Yes;SERVER=' + str(
                host) + ';DATABASE=' + str(DB) + ';UID=' + str(DB_Login) + ';PWD=' + str(DB_Pass), autocommit=True)
            cursor = cnxn.cursor()

            print('Обработка базы ' + str(DB))

            cursor.execute(
                """SELECT  '""" + str(directory) + "/" + str(DB) + """/Stored Procedures/' + SCHEMA_NAME(o.schema_id) + '.' + o.name + '.sql' as name, TRIM(object_definition(o.object_id)) as code
			FROM sys.objects o
			where o.name not like 'sys%'
			and o.type in ('P')
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'w', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """select distinct '""" + str(directory) + "/" + str(DB) + """/Tables/'+s.name+'.'+tb.name+'.sql' as name, 'CREATE TABLE ['+s.name+'].['+tb.name+'] ('+c.cols+IIF(pk.pk is not null,','+char(13)+char(10)+ pk.pk,'')+IIF(uq.keys is not null,','+char(13)+char(10)+ uq.keys,'')+IIF(const.fks is not null,',' + const.fks,'')+char(13)+char(10)+') ON ['+fg.name+'];' + CHAR(13)+char(10)+'GO'+CHAR(13)+char(10)+ ISNULL(CHAR(13)+char(10)+idx.CreateIndexScript,'') as sql
				from sys.tables tb
				join sys.schemas s on tb.schema_id=s.schema_id

				cross apply (
				select distinct f.name
				from sys.filegroups f 
				join sys.indexes i on i.data_space_id = f.data_space_id
				where i.object_id=tb.object_id and i.index_id in (0,1)
				) fg

				outer apply (
				select t.name, STRING_AGG(char(13)+char(10)+'['+c.name +'] '+IIF(compc.definition IS NULL,UPPER(tt.name)+ISNULL(len.val,''), 'AS '+compc.definition+IIF(compc.is_persisted=1,' PERSISTED',''))+IIF(ISNULL(idc.ident,'')='','', ' '+idc.ident)+ IIF(TRIM(ISNULL(object_definition(c.default_object_id),'')) = '','', ' CONSTRAINT ['+object_name(c.default_object_id)+'] DEFAULT '+ CONVERT(VARCHAR(MAX),TRIM(object_definition(c.default_object_id)))) + IIF(c.is_nullable=0,  IIF(compc.object_id IS NULL, ' NOT NULL',''), ''), ', ')  as cols ---, c.* --STRING_AGG(c.name,',') as cols 
				from sys.tables t 
				join sys.columns c on c.object_id=t.object_id
				left join sys.computed_columns compc on compc.object_id=t.object_id and compc.column_id=c.column_id
				join sys.types tt on c.user_type_id = tt.user_type_id 
				outer apply (
				select IIF(c.system_type_id in (62,106,108),'('+convert(varchar(max),c.precision)+IIF(c.system_type_id=62,'',','+convert(varchar(max),c.scale))+')','('+IIF(convert(varchar(max),c.max_length)='-1','MAX', convert(varchar(max),c.max_length))+')') as val from sys.columns cc
				where cc.column_id=c.column_id and cc.object_id=t.object_id 
				and c.system_type_id in (62,106,108,165,167,173,175,231,239)
				) len
				outer apply (SELECT 'IDENTITY('+ convert(varchar(max),idc.seed_value) + ','+convert(varchar(max),idc.increment_value)+')' as ident
				FROM sys.identity_columns idc 
				where t.object_id=idc.object_id
				and c.column_id=idc.column_id) idc
				where tb.object_id=t.object_id
				group by t.name
				) c

				outer apply (select  'CONSTRAINT ['+i.name+'] PRIMARY KEY '+i.type_desc+' ('+STRING_AGG('['+ix.name+'] '+IIF(ix.is_descending_key = 1,' DESC',' ASC'),', ')+')'+' WITH ('+IIF(i.fill_factor is null, '', 'FILLFACTOR = '+CONVERT(VARCHAR(MAX),i.fill_factor))+ IIF(i.is_padded=0, '', ', PAD_INDEX = ON')+') ON ['+ix.fg+']' COLLATE DATABASE_DEFAULT as pk
				FROM sys.indexes i
				outer apply (
				select distinct cl.name as name, ic.is_descending_key as is_descending_key, f.name as fg 
				from sys.filegroups f 
				join sys.objects ob on i.object_id = ob.parent_object_id 
				inner join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
				inner join sys.columns cl on cl.column_id = ic.column_id and cl.object_id = ob.parent_object_id
				where i.data_space_id = f.data_space_id) ix
				where i.object_id=tb.object_id and i.is_primary_key=1 
				group by i.name, i.type_desc, i.fill_factor, ix.fg, i.is_padded) pk

				outer apply (select uqs.id, STRING_AGG(uqs.uq,CHAR(13)+char(10)) as keys from 
				(select i.object_id as id, 'CONSTRAINT ['+i.name+'] UNIQUE '+i.type_desc+' ('+STRING_AGG('['+ix.name+'] '+IIF(ix.is_descending_key = 1,' DESC',' ASC'),', ')+')'+' WITH ('+IIF(i.fill_factor is null, '', 'FILLFACTOR = '+CONVERT(VARCHAR(MAX),i.fill_factor))+ IIF(i.is_padded=0, ', PAD_INDEX = OFF', ', PAD_INDEX = ON')+') ON ['+ix.fg+']' COLLATE DATABASE_DEFAULT as uq
				FROM sys.indexes i
				outer apply (
				select distinct cl.name as name, ic.is_descending_key as is_descending_key, f.name as fg 
				from sys.filegroups f 
				join sys.objects ob on i.object_id = ob.parent_object_id 
				inner join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
				inner join sys.columns cl on cl.column_id = ic.column_id and cl.object_id = ob.parent_object_id
				where i.data_space_id = f.data_space_id) ix
				where i.object_id=tb.object_id and i.is_unique_constraint = 1
				group by i.object_id, i.name, i.type_desc, i.fill_factor, ix.fg, i.is_padded) uqs
				group by uqs.id) uq

				outer apply (
				select STRING_AGG(char(13)+char(10)+'CONSTRAINT ['+f.name+'] FOREIGN KEY ('+fkc.FKColumn +') REFERENCES '+ t1.name+IIF(f.is_not_for_replication = 1,' NOT FOR REPLICATION',''),',') WITHIN GROUP (ORDER BY t1.name ASC) as fks
				from sys.foreign_keys f 
				join sys.tables t on f.parent_object_id = t.object_id
				outer apply (
				select STRING_AGG('['+c.name+']', ', ') as FKColumn from sys.columns c 
				join sys.foreign_key_columns as fkc on fkc.parent_object_id = c.object_id and  fkc.parent_column_id = c.column_id
				where fkc.constraint_object_id=f.object_id
				group by fkc.constraint_object_id 
				) fkc
				outer apply (
				select top 1 '['+s.name+'].['+t1.name+']' as name from sys.tables t1 
				join sys.schemas s on t1.schema_id=s.schema_id
				where t1.object_id=f.referenced_object_id
				) t1
				where 
				f.parent_object_id = tb.object_id) const

				outer apply(SELECT o.[name] 
				FROM sys.indexes i
				INNER JOIN sys.filegroups f ON i.data_space_id = f.data_space_id
				INNER JOIN sys.all_objects o ON i.[object_id] = o.[object_id] 
				where o.object_id=tb.object_id) t2

				outer apply (
				SELECT t.object_id, STRING_AGG('CREATE '+ IIF(i.is_unique = 1, 'UNIQUE ', '')+i.type_desc COLLATE DATABASE_DEFAULT + ' INDEX [' + i.name + '] ON [' + SCHEMA_NAME(t.schema_id) + '].[' + t.name + '] '+CHAR(13) +char(10)+'(' + 
				KeyColumns.cols + ')' + ISNULL(CHAR(13)+char(10) +'INCLUDE (' + IncludedColumns.cols + ') ', '') + ISNULL(CHAR(13) +char(10)+'WHERE  ' + i.filter_definition, '') + CHAR(13)+char(10) + 'WITH (' +  IIF(i.is_padded = 1,'PAD_INDEX = ON, ','PAD_INDEX = OFF, ') + 
				'FILLFACTOR = ' + CONVERT(VARCHAR(5),IIF(i.fill_factor = 0, 100, i.fill_factor)) + IIF(i.ignore_dup_key = 1, ', IGNORE_DUP_KEY = ON',', IGNORE_DUP_KEY = OFF') + 
				IIF(i.allow_row_locks = 0, ', ALLOW_ROW_LOCKS = OFF',', ALLOW_ROW_LOCKS = ON')+IIF(i.allow_page_locks = 0,', ALLOW_PAGE_LOCKS = OFF',', ALLOW_PAGE_LOCKS = ON')+IIF(i.optimize_for_sequential_key = 1,', OPTIMIZE_FOR_SEQUENTIAL_KEY = ON',', OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF')+CHAR(13) +char(10)+'/* SORT_IN_TEMPDB = ON, ONLINE = ON, DROP_EXISTING = OFF */'+ ') ON [' +DS.name + '];' +  CHAR(13) +char(10)+ 'GO ', CHAR(13)+char(10)+CHAR(13)+char(10))  WITHIN GROUP (ORDER BY i.name ASC)  [CreateIndexScript]
				FROM sys.indexes i 
				JOIN sys.tables t ON  i.object_id = t.object_id
				JOIN sys.sysindexes si ON  i.object_id = si.id AND i.index_id = si.indid
				OUTER APPLY (
				SELECT STRING_AGG('[' + c.name + ']' + iif(ic1.is_descending_key=1,' DESC',' ASC'),', ') as cols
				FROM sys.index_columns ic1
				JOIN sys.columns c ON  c.object_id = ic1.object_id AND c.column_id = ic1.column_id
				where  ic1.is_included_column = 0
				and ic1.object_id=i.object_id 
				and ic1.index_id=i.index_id
				group by ic1.object_id, ic1.index_id) KeyColumns
				JOIN sys.data_spaces DS ON  i.data_space_id = DS.data_space_id
				JOIN sys.filegroups FG  ON  i.data_space_id = FG.data_space_id
				OUTER APPLY (SELECT STRING_AGG('[' + c.name + ']',', ') as cols
				FROM sys.index_columns ic1
				JOIN sys.columns c ON  c.object_id = ic1.object_id AND c.column_id = ic1.column_id AND ic1.is_included_column = 1
				WHERE  ic1.object_id = i.object_id
				AND ic1.index_id = i.index_id
				GROUP BY ic1.object_id, ic1.index_id) IncludedColumns
				WHERE  i.is_primary_key = 0
				AND i.is_unique_constraint = 0
				and t.is_ms_shipped <> 1 
				and i.index_id <> 0
				and tb.object_id=t.object_id
				group by t.object_id
				) idx
				order by 1
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'w', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT  '""" + str(directory) + "/" + str(DB) + """/Functions/Scalar-valued Functions/' + SCHEMA_NAME(o.schema_id) + '.' + o.name + '.sql' as name, TRIM(object_definition(o.object_id)) as code
			FROM sys.objects o
			where o.name not like 'sys%'
			and o.type in ('FN')
			UNION ALL
			SELECT  '""" + str(directory) + "/" + str(DB) + """/Functions/Table-valued Functions/' + SCHEMA_NAME(o.schema_id) + '.' + o.name + '.sql' as name, TRIM(object_definition(o.object_id)) as code
			FROM sys.objects o
			where o.name not like 'sys%'
			and o.type in ('TF','IF')
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'w', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """select '""" + str(directory) + "/" + str(DB) + """/Triggers/'+SCHEMA_NAME(s.schema_id)+'.'+s.name+'.sql' as triggername, STRING_AGG(TRIM(object_definition(tr.object_id)) +CHAR(13)+char(10)+ ' ' + IIF(tr.is_disabled = 1, char(13)+char(10)+'GO' + char(13)+char(10)+ 'DISABLE TRIGGER ' + tr.name +' ON ' + t.name+CHAR(13)+char(10), CHAR(13)+char(10)+'GO' + char(13)+char(10)+ 'ENABLE TRIGGER ' + tr.name +' ON dbo.' + s.name+CHAR(13)+char(10)), char(13)+char(10)+'GO'+char(13)+char(10)) as code from [sys].[triggers] tr
			left join [sys].[tables] t on tr.parent_id = t.object_id
			left join sys.objects s on tr.parent_id=s.object_id
			where s.type='V'
			group by '""" + str(directory) + "/" + str(DB) + """/Triggers/'+SCHEMA_NAME(s.schema_id)+'.'+s.name+'.sql'
			union
			select '""" + str(directory) + "/" + str(DB) + """/Triggers/'+SCHEMA_NAME(s.schema_id)+'.'+t.name+'.sql' as triggername, STRING_AGG(TRIM(object_definition(tr.object_id)) +CHAR(13)+char(10)+ ' ' + IIF(tr.is_disabled = 1, char(13)+char(10)+'GO' + char(13)+char(10)+ 'DISABLE TRIGGER ' + tr.name +' ON ' + t.name +CHAR(13)+char(10), CHAR(13)+char(10)+'GO' + char(13)+char(10)+ 'ENABLE TRIGGER ' + tr.name +' ON ' + t.name +CHAR(13)+char(10)), char(13)+char(10)+'GO'+char(13)+char(10)) as code from [sys].[triggers] tr
			left join [sys].[tables] t on tr.parent_id = t.object_id
			left join sys.objects s on tr.parent_id=s.object_id
			where s.type='U'
			group by '""" + str(directory) + "/" + str(DB) + """/Triggers/'+SCHEMA_NAME(s.schema_id)+'.'+t.name+'.sql'
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'w', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """	select '""" + str(directory) + "/" + str(DB) + """/Database Triggers/'+t.name+'.sql' as triggername, TRIM(OBJECT_DEFINITION(t.object_id))+char(13)+char(10)+char(13)+char(10)+'GO'+char(13)+char(10)+IIF(t.is_disabled = 1, 'DISABLE', 'ENABLE')+' TRIGGER ['+t.name+'] ON ALL SERVER' as code 
			from sys.triggers t where t.parent_class = 0
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'w', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT '""" + str(directory) + "/" + str(DB) + """/Sequences/'+SCHEMA_NAME(s.schema_id)+'.'+s.name+ '.sql' AS name, 
			'CREATE SEQUENCE ['+SCHEMA_NAME(s.schema_id)+'].['+s.name+']'+CHAR(13)+char(10)+ 
			'AS ['+t.name+']'+CHAR(13)+char(10)+'START WITH '+CONVERT(VARCHAR(MAX),s.start_value)+ 
			CHAR(13)+char(10)+'INCREMENT BY '+
			CONVERT(VARCHAR(MAX),s.increment)+CHAR(13)+char(10)+'MINVALUE '+CONVERT(VARCHAR(MAX),s.minimum_value)+CHAR(13)+ 
			char(10)+'MAXVALUE '+CONVERT(VARCHAR(MAX),s.maximum_value)+CHAR(13)+char(10)+ 
			IIF(s.is_cycling = 1, 'CYCLE'+CHAR(13)+char(10),'')+IIF(s.is_cached = 1, 'CACHE'+
			IIF(s.cache_size IS NOT NULL, ' '+CONVERT(VARCHAR(MAX),s.cache_size),''),'NO CACHE') 
			+CHAR(13)+char(10)+'GO'+CHAR(13)+char(10) AS code
			FROM sys.sequences s
			JOIN sys.types t on s.user_type_id = t.user_type_id 
			WHERE s.is_ms_shipped = 0	
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'w', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT  '""" + str(directory) + "/" + str(DB) + """/Views/' + SCHEMA_NAME(o.schema_id) + '.' + o.name + '.sql' as name, TRIM(ISNULL(object_definition(o.object_id),'')) as code
			FROM sys.objects o
			where o.name not like 'sys%'
			and o.type in ('V')
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'w', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT '""" + str(directory) + "/" + str(DB) + """/Security/Roles/'+ pr.name + '.sql', IIF(pr.name is not null,'CREATE ROLE ['+pr.name+'];'+char(13)+char(10)+'GO'+char(13)+char(10),'')+STRING_AGG(CONVERT(NVARCHAR(MAX),r.code),char(13)+char(10)) WITHIN GROUP (ORDER BY r.code ASC) as code
			from sys.database_principals pr
			CROSS APPLY (select pr.name, code =
			CASE pe.class 
			WHEN 0 THEN pe.state_desc + ' ' + pe.permission_name + char(13)+char(10)+  'ON DATABASE::['+DB_NAME()+'] TO [' + pr.name COLLATE DATABASE_DEFAULT+'];'+char(13)+char(10)+'GO'
			WHEN 1 THEN pe.state_desc + ' ' + pe.permission_name + char(13)+char(10)+ 
			'ON OBJECT::['+SCHEMA_NAME(o.schema_id)+'].['+o.name+'] TO [' + pr.name COLLATE DATABASE_DEFAULT+']' 
			 + char(13)+char(10) +'AS ['+SCHEMA_NAME(o.schema_id)+'];'+char(13)+char(10)+'GO'
			WHEN 3 THEN pe.state_desc + ' ' + pe.permission_name + char(13)+char(10)+ 'ON SCHEMA::['+SCHEMA_NAME(pe.major_id)+'] TO [' + pr.name COLLATE DATABASE_DEFAULT+'];'+char(13)+char(10)+'GO'
			END 
				FROM sys.database_permissions AS pe
			left JOIN sys.objects AS o
			ON pe.major_id = o.object_id
			where pe.grantee_principal_id = pr.principal_id
			) r
			where pr.type='R'
			group by pr.name
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'w', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT '""" + str(directory) + "/" + str(DB) + """/Security/Roles/'+ DP1.name + '.sql' AS DatabaseRole,
			char(13)+char(10)+STRING_AGG(CONVERT(NVARCHAR(MAX),'ALTER ROLE ['+ DP1.name+ '] ADD MEMBER ['+ DP2.name+']'+char(13)+char(10)+'GO'), char(13)+char(10))  WITHIN GROUP (ORDER BY DP1.name ASC, DP2.name ASC) AS DatabaseUser
			FROM sys.database_role_members AS DRM
			RIGHT OUTER JOIN sys.database_principals AS DP1
			ON DRM.role_principal_id = DP1.principal_id
			LEFT OUTER JOIN sys.database_principals AS DP2
			ON DRM.member_principal_id = DP2.principal_id
			WHERE DP1.type = 'R' and
			DP2.name not like '%*%'
			and DP2.name is not NULL
			GROUP BY DP1.name
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'a', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT IIF(pr.name like '%DOMAIN%', '""" + str(directory) + "/" + str(
                    DB) + """/Security/Users/Windows/'+ REPLACE(pr.name,'DOMAIN','DOMAIN'),'""" + str(
                    directory) + "/" + str(DB) + """/Security/Users/SQL/'+ pr.name )  + '.sql', 'USE """ + str(DB) + """;'+char(13)+char(10)+'GO'+char(13)+char(10)+STRING_AGG(CONVERT(NVARCHAR(MAX),r.code),char(13)+char(10)) WITHIN GROUP (ORDER BY r.code ASC) as code
			FROM sys.database_principals AS pr
			JOIN sys.database_permissions AS pe
			ON pe.grantee_principal_id = pr.principal_id
			JOIN sys.objects AS o
			ON pe.major_id = o.object_id
			OUTER APPLY (select pe.state_desc + ' ' + pe.permission_name + char(13)+char(10)+ 'ON OBJECT::[dbo].['+o.name+'] TO [' + IIF (pr.name LIKE '%DOMAIN%', REPLACE(pr.name,'DOMAIN','DOMAIN'),pr.name)  COLLATE DATABASE_DEFAULT+']'  + char(13)+char(10) +'AS [dbo];'+char(13)+char(10)+'GO'  as code from sys.database_principals pr1 where pr.sid=pr1.sid) r
			group by pr.name
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0].replace('DOMAIN\\', '')
                with io.open(file_name, 'w', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT '""" + str(directory) + """/Server Security/Logins/'+IIF(pr.name like '%DOMAIN%','Windows','SQL')+'/'+IIF(pr.name like '%*%','MD5_'+CONVERT(VARCHAR(32),HashBytes('MD5', pr.name), 2),pr.name) +'.sql' as name, 'USE '+ DB_NAME()+ CHAR(13)+char(10)+'GO'+char(13)+char(10)+STRING_AGG(CONVERT(NVARCHAR(MAX),r.code),char(13)+char(10)) WITHIN GROUP (ORDER BY r.code ASC) as code
			FROM sys.server_principals AS pr 
			JOIN sys.server_permissions AS pe  
			ON pe.grantee_principal_id = pr.principal_id  
			CROSS APPLY (select pe.state_desc + ' ' + pe.permission_name +' TO [' + pr.name COLLATE DATABASE_DEFAULT +']'+char(13)+char(10)+'GO'  as code from sys.database_principals pr1 where pr.sid=pr1.sid) r
			group by pr.name
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0].replace('DOMAIN\\', '')
                with io.open(file_name, 'a', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT '""" + str(directory) + "/" + str(DB) + """/Service Broker/Queues/'+SCHEMA_NAME(sq.[schema_id])+'.'+sq.[name]+'.sql' AS 'Name', 'CREATE QUEUE ['+SCHEMA_NAME(sq.[schema_id])+'].['+sq.[name]+'] WITH STATUS = '+IIF(sq.is_enqueue_enabled = 1,'ON','OFF') +', RETENTION = '+IIF(sq.is_retention_enabled = 1,'ON','OFF')+', ACTIVATION (STATUS = '+IIF(sq.is_activation_enabled = 1,'ON, PROCEDURE_NAME = '+sq.activation_procedure,'OFF')	+', MAX_QUEUE_READERS = '+CONVERT(VARCHAR(MAX),sq.max_readers)+IIF(sq.execute_as_principal_id IS NULL,')',', EXECUTE AS '+CASE sq.execute_as_principal_id WHEN -2 THEN 'OWNER' ELSE '['+pr.name+']' END+')')+', POISON_MESSAGE_HANDLING (STATUS = '+IIF(sq.is_poison_message_handling_enabled = 1,'ON','OFF')+')  ON ['+f.[name]+']' COLLATE DATABASE_DEFAULT AS Code
			FROM sys.service_queues sq
			LEFT JOIN sys.server_principals pr ON pr.principal_id = sq.execute_as_principal_id
			JOIN sys.objects o ON o.parent_object_id = sq.object_id
			JOIN sys.indexes i ON i.object_id = o.object_id AND i.index_id = 1
			JOIN sys.filegroups f  ON i.data_space_id = f.data_space_id
			WHERE sq.is_ms_shipped = 0
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'a', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT '""" + str(directory) + "/" + str(DB) + """/Service Broker/XML Schemas/'+ SCHEMA_NAME(XSC.[schema_id])+'.'+XSC.[name]+'.sql' AS 'Name', 'CREATE XML SCHEMA COLLECTION ['+SCHEMA_NAME(XSC.[schema_id])+'].['+XSC.[name]+'] AS N'''+CONVERT(VARCHAR(MAX),XML_SCHEMA_NAMESPACE (SCHEMA_NAME(XSC.[schema_id]),XSC.[name]))+'''' AS 'Code'
			FROM    sys.xml_schema_collections XSC JOIN sys.xml_schema_namespaces XSN
			ON (XSC.xml_collection_id = XSN.xml_collection_id)
			WHERE    XSC.xml_collection_id > 1
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'a', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT '""" + str(directory) + "/" + str(DB) + """/Service Broker/Message types/'+smt.[name]+'.sql' AS 'Name', 'CREATE MESSAGE TYPE ['+smt.[name]+'] VALIDATION = '+CASE smt.[validation] WHEN 'E' THEN 'EMPTY' WHEN 'N' THEN 'NONE' WHEN 'X' THEN IIF(smt.xml_collection_id IS NULL, 'WELL_FORMED_XML', 'VALID_XML WITH SCHEMA COLLECTION ['+SCHEMA_NAME(xsc.[schema_id])+'].['+xsc.[name]+']') END COLLATE DATABASE_DEFAULT AS 'Code'
			FROM sys.service_message_types smt
			LEFT JOIN sys.xml_schema_collections xsc ON smt.xml_collection_id = xsc.xml_collection_id
			WHERE smt.message_type_id > 14
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'a', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT '""" + str(directory) + "/" + str(DB) + """/Service Broker/Contracts/'+sc.[name]+'.sql' AS 'Name', 'CREATE CONTRACT ['+sc.[name]+'] (['+smt.[name]+'] SENT BY '+CASE WHEN scmu.is_sent_by_initiator = 1 AND scmu.is_sent_by_target = 1 THEN 'ANY' WHEN scmu.is_sent_by_initiator = 1 AND scmu.is_sent_by_target = 0 THEN 'INITIATOR' WHEN scmu.is_sent_by_initiator = 0 AND scmu.is_sent_by_target = 1 THEN 'TARGET' END +')' COLLATE DATABASE_DEFAULT AS 'Code'
			FROM sys.service_contracts sc
			JOIN sys.service_contract_message_usages scmu ON sc.service_contract_id=scmu.service_contract_id
			JOIN sys.service_message_types smt ON scmu.message_type_id=smt.message_type_id AND smt.message_type_id > 14
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'a', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            cursor.execute(
                """SELECT '""" + str(directory) + "/" + str(DB) + """/Service Broker/Services/'+s.[name]+'.sql' AS 'Name', 'CREATE SERVICE ['+s.[name]+']  ON QUEUE ['+SCHEMA_NAME(q.[schema_id])+'].['+q.[name]+'] (['+sc.[name]+'])'  COLLATE DATABASE_DEFAULT AS Code
			FROM sys.services s
			JOIN sys.service_queues q ON s.service_queue_id=q.object_id
			JOIN sys.service_contract_usages scu ON scu.service_id=s.service_id 
			JOIN sys.service_contracts sc ON sc.service_contract_id=scu.service_contract_id AND sc.service_contract_id>5
			""")
            results = [row for row in cursor.fetchall()]
            for result in results:
                file_name = result[0]
                with io.open(file_name, 'a', encoding="utf-8") as f:
                    f.write(result[1])
                print("Записан файл: " + file_name)

            if DB == 'APT':
                if not os.path.exists(directory + "/SQL Jobs"):
                    os.mkdir(directory + "/SQL Jobs")
                else:
                    shutil.rmtree(directory + "/SQL Jobs")
                    os.mkdir(directory + "/SQL Jobs")

                if not os.path.exists(directory + "/SQL Jobs/SQL Server Agent"):
                    os.mkdir(directory + "/SQL Jobs/SQL Server Agent")


                jobpath = directory + "/SQL Jobs/SQL Server Agent"

                cursor.execute(
                    """ select sj.name name, 'step_'+convert(varchar,sjs.step_id)+'.'+sjs.step_name+'.sql' [step], IIF(sj.enabled=0,'/*** disabled ***/'+char(13)+char(10),'')+sjs.command as code
                        from msdb.dbo.sysjobs sj
                        join msdb.dbo.sysjobsteps sjs on  sjs.job_id=sj.job_id
                        order by name, step""")
                results = [row for row in cursor.fetchall()]
                for result in results:
                    folder = jobpath + '/'+ result[0].replace('\\','-').replace('/','-').replace(':','-').replace('*','-').replace('?','-').replace('"','-').replace('<','-').replace('>','-').replace('|','-').replace('+','-').replace('%','-').replace('@','-')

                    if not os.path.exists(folder):
                        os.mkdir(folder)
                    file_name = folder + '/' + result[1].replace('\\','-').replace('/','-').replace(':','-').replace('*','-').replace('?','-').replace('"','-').replace('<','-').replace('>','-').replace('|','-').replace('+','-').replace('%','-').replace('@','-')
                    with io.open(file_name, 'w', encoding="utf-8") as f:
                        f.write(result[2])
                    print("Записан файл: " + file_name)

        print('Выгрузка безопасности сервера')
        cursor.execute(
            """select '""" + str(directory) + """/Server Security/Roles/'+ SUSER_NAME(rm.role_principal_id)+'.sql', STRING_AGG(CONVERT(NVARCHAR(MAX),'ALTER SERVER ROLE '+SUSER_NAME(rm.role_principal_id)+' ADD MEMBER [' + lgn.name +']'+char(13)+char(10)+'GO'),char(13)+char(10)) WITHIN GROUP (ORDER BY lgn.name ASC) code
		from sys.server_role_members rm, sys.server_principals lgn
		where rm.role_principal_id >=3 AND rm.role_principal_id <=10 AND
		rm.member_principal_id = lgn.principal_id
		group by rm.role_principal_id
		""")
        results = [row for row in cursor.fetchall()]
        for result in results:
            file_name = result[0]
            with io.open(file_name, 'w', encoding="utf-8") as f:
                f.write(result[1])
            print("Записан файл: " + file_name)

        cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};TrustServerCertificate=Yes;SERVER=' + str(
            host) + ';DATABASE=master;UID=' + str(DB_Login) + ';PWD=' + str(DB_Pass), autocommit=True)
        cursor = cnxn.cursor()

        cursor.execute(
            """select  '""" + str(directory) + """/Server Security/Triggers/'+'dbo.'+t.name+'.sql' as triggername, TRIM(OBJECT_DEFINITION(t.object_id))+char(13)+char(10)+char(13)+char(10)+'GO'+char(13)+char(10)+IIF(t.is_disabled = 1, 'DISABLE', 'ENABLE')+' TRIGGER ['+t.name+'] ON ALL SERVER' as code 
		from master.sys.server_triggers t""")
        results = [row for row in cursor.fetchall()]
        for result in results:
            file_name = result[0]
            with io.open(file_name, 'w', encoding="utf-8") as f:
                f.write(result[1])
            print("Записан файл: " + file_name)

        cursor.close()
    except Exception as git_err:
        body = 'Ошибка при выполнении скрипта для БД ' + str(DB) + ' :\n\n' + str(git_err)
        msg.attach(MIMEText(body, 'plain'))
        server.starttls()
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()
        print('Выгрузка кода базы ' + str(DB) + ' не удалась: ' + str(git_err))


    print('Отправка в TFS...')
    PATH_OF_GIT_REPO = directory  # make sure .git folder is properly configured
    COMMIT_MESSAGE = 'Autocommit from SQL Server'
    try:
        repo = Repo(PATH_OF_GIT_REPO)
        repo.config_writer().set_value("user", "name", "git_sql").release()
        repo.git.add(all=True)
        if len(repo.index.diff('HEAD'))>0:
            repo.index.commit(COMMIT_MESSAGE)
            origin = repo.remote(name='origin')
            origin.push(progress=ProgressPrinter()).raise_if_error()
            print('Успешная фиксация')
        else:
            print('Изменения отсутствуют')
    except Exception as git_err:
        body = 'Ошибка при отправке кода БД в GIT:\n\n' + str(git_err)
        msg.attach(MIMEText(body, 'plain'))
        server.starttls()
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()
        print('Фиксация не удалась' + str(git_err))

    shutil.rmtree(directory)
    cursor.close()

schedule.every().hour.at(":14").do(export)
schedule.every().hour.at(":29").do(export)
schedule.every().hour.at(":44").do(export)
schedule.every().hour.at(":59").do(export)

while True:
    try:
        schedule.run_pending()
    except Exception as git_err:
        body = 'Ошибка робота GIT:\n\n' + str(git_err)
        msg.attach(MIMEText(body, 'plain'))
        server.starttls()
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()
        print('Работа прервана с ошибкой \n' + str(git_err))
    time.sleep(1)