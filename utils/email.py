#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: email.py
@Ide: PyCharm
@Time: 2021-06-13 14:14:18
@Desc: 
"""
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP_SSL

from init import ConfigAnalysis
from utils.logger import logger


class Email(ConfigAnalysis):

    def __init__(self, title, content, error=None):
        super(Email, self).__init__()
        self.title = f'[启动时间:{self.obj_start_time}] {title}'
        _error = f"<p>Twitter爬虫异常，错误信息如下：</p>\n\t"
        _content = f"<p>Twitter爬虫正常，当前进度：</p>\n\t"
        self.content =  f"{_error}{error}" if error else f"{_content}{content}"
        self.header = "TwitterSpider"

        self.send_email()


    def send_email(self):

        msg = MIMEMultipart()
        msg["Subject"] = Header(self.title, 'utf-8')
        msg["From"] = self.sender_qq
        msg["To"] = Header(self.header, "utf-8")

        msg.attach(MIMEText(self.content, 'html'))

        try:
            smtp = SMTP_SSL(self.host_server)  # ssl登录连接到邮件服务器
            smtp.set_debuglevel(0)  # 0是关闭，1是开启debug
            smtp.ehlo(self.host_server)  # 跟服务器打招呼，告诉它我们准备连接，最好加上这行代码
            smtp.login(self.sender_qq, self.pwd)
            smtp.sendmail(self.sender_qq, self.receiver, msg.as_string())
            smtp.quit()
            logger.info("邮件发送成功")
        except smtplib.SMTPException:
            logger.error("无法发送邮件")
