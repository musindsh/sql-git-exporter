import os
class Config:
    DB_Login = 'git_user'
    DB_Pass = 'password'
    directory = os.path.join(os.getcwd(),'SQL_Code')
    DBs = [['Files','sqlsrv.domain.lcl'], ['Bills','sqlsrv.domain.lcl'], ['Confluence','sqlsrv.domain.lcl'],  ['dbms','sqlsrv.domain.lcl'],  ['Logs','sqlsrv.domain.lcl'], ['Monitoring','sqlsrv.domain.lcl'], ['master','sqlsrv.domain.lcl']]
    git_ssh_identity_file = os.path.join(os.getcwd(),'id_rsa')
    git_ssh_cmd = 'ssh -i %s' % git_ssh_identity_file
    remote = "ssh://git.domain.lcl:22/"
    fromaddr = "sql_git@domain.ru"
    toaddr = ["user@domain.ru"] # формат ["1@1.ru","2@1.ru"]
