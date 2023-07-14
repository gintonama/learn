import ftplib

url = 'localhost'
username = 'avas'
password = 'keamanan'
local_path_file = '/opt/inforex/example.csv'
ftp_path_file = '/anomaly/example.csv'

ftp = ftplib.FTP(url)
ftp.login(username, password)

with open(local_path_file, "rb") as file:
    # use FTP's STOR command to upload the file
    ftp.storbinary(f"STOR {ftp_path_file}", file)