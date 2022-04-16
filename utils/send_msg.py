import smtplib
from email.mime.text import MIMEText

from common.config import mail_user, mail_pass, mail_host, mail_receivers, mail_sender

# 设置服务器所需信息
# 163邮箱服务器地址


class MSGSender:
    def __init__(self):
        self.mail_host = mail_host
        # 163用户名
        self.mail_user = mail_user
        # 密码(部分邮箱为授权码)
        self.mail_pass = mail_pass
        # 邮件发送方邮箱地址
        self.sender = mail_sender
        # 邮件接受方邮箱地址，注意需要[]包裹，这意味着你可以写多个邮件地址群发
        self.receivers = mail_receivers

        # 设置email信息
        # 邮件内容设置
        self.message = MIMEText('content', 'plain', 'utf-8')
        # 邮件主题
        self.message['Subject'] = 'title'
        # 发送方信息
        self.message['From'] = self.sender
        # 接受方信息
        self.message['To'] = self.receivers[0]

    def send(self, text):
        self.message = MIMEText(text, 'plain', 'utf-8')
        # 登录并发送邮件
        try:
            smtpObj = smtplib.SMTP_SSL(self.mail_host)
            # 登录到服务器
            smtpObj.login(self.mail_user, self.mail_pass)
            # 发送
            smtpObj.sendmail(
                self.sender, self.receivers, self.message.as_string())
            # 退出
            smtpObj.quit()
            print('success')
        except smtplib.SMTPException as e:
            print('error', e)  # 打印错误
