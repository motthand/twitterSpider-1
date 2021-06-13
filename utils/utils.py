#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: utils.py
@Ide: PyCharm
@Time: 2021-05-30 12:00:19
@Desc:
"""
import datetime
import hashlib
import sys
from _md5 import md5
from concurrent import futures
from datetime import datetime
from datetime import timezone, timedelta

import psutil
import tqdm


def get_md5_text(text):
    hashlib.md5(text.encode('utf8')).hexdigest()


def get_md5(data):
    md5_ = md5()
    md5_.update(data)
    return md5_.hexdigest()


def order_set_list(data: list):
    """
    列表去重 且顺序
    :param data:
    :return:
    """
    return list({}.fromkeys(data).keys())


def twitter_conversion(times: list or str):
    """
    Input ['Wed May 12 05:54:51 +0000 2021'] or 'Wed May 12 05:54:51 +0000 2021'
    Output ['2021-05-12 13:54:51'] or '2021-05-12 13:54:51'
    :param times:
    :return:
    """

    utc = timezone.utc  # 获取 UTC 的时区对象
    beijing = timezone(timedelta(hours=8))  # 北京时区

    def _twitter_conversion(ctime):
        time_obj = datetime.strptime(ctime, '%a %b %d %H:%M:%S %z %Y')
        utc_time = time_obj.replace(tzinfo=utc)  # 强制转换加上 UTC 时区
        time_beijing = utc_time.astimezone(beijing)  # 转为北京时间
        time_beijing = time_beijing.strftime('%Y-%m-%d %H:%M:%S')
        return time_beijing

    time_list = list()
    if isinstance(times, list):
        for ctime in times:
            time_beijing = _twitter_conversion(ctime)
            time_list.append(time_beijing)
    elif isinstance(times, str):
        return _twitter_conversion(times)
    else:
        return None
        # raise Exception("参数类型错误！")

    return time_list


def thread_pool(method, data, **kwargs):
    """
    多线程任务
    :param method: 函数
    :param data: 可遍历列表数据
    :param kwargs:
    :return:
    """
    if len(kwargs) == 1:
        kwargs = kwargs.get('kwargs')
    if isinstance(kwargs, dict):
        thread_num = kwargs.get('thread_num', 1)
        prompt = kwargs.get('prompt', '多线程任务->[线程数:%s]' % thread_num)
    else:
        sys.exit('kwargs is not dict')

    prompt = '多线程任务 [线程数:%s]-> %s ->' % (thread_num, prompt)
    with futures.ThreadPoolExecutor(max_workers=thread_num) as executor:
        res = tqdm.tqdm(executor.map(method, data), total=len(data))
        res.set_description(prompt)
        return len(list(res))


def process_pool(method, data, **kwargs):
    """
    多进程任务
    :param method: 函数
    :param data: 可遍历列表数据
    :param kwargs: :return:
    """
    if len(kwargs) == 1:
        kwargs = kwargs.get('kwargs')
    if isinstance(kwargs, dict):
        thread_num = kwargs.get('thread_num', 1)
        prompt = kwargs.get('prompt', '多进程任务->[线程数:%s]' % thread_num)
    else:
        sys.exit('kwargs is not dict')

    prompt = '多进程任务->[%s]->[线程数:%s]' % (prompt, thread_num)
    with futures.ProcessPoolExecutor(max_workers=thread_num) as executor:
        res = tqdm.tqdm(executor.map(method, data), total=len(data))
        res.set_description(prompt)
        return len(list(res))


def MonitorSystem():
    # 获取cpu使用情况
    cpuper = psutil.cpu_percent()
    # 获取内存使用情况：系统内存大小，使用内存，有效内存，内存使用率
    mem = psutil.virtual_memory()
    # 内存使用率
    memper = mem.percent
    # 获取当前时间
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    line = f'{ts} cpu:{cpuper}%, mem:{memper}%'
    return line


def MonitorNetWork():
    # 获取网络收信息
    netinfo = psutil.net_io_counters()
    # 获取当前时间
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    line = f'{ts} bytessent={StrOfSize(netinfo.bytes_sent)}, bytesrecv={StrOfSize(netinfo.bytes_recv)}'
    return line


def StrOfSize(size):
    '''
    auth: wangshengke@kedacom.com ；科达柯大侠
    递归实现，精确为最大单位值 + 小数点后三位
    '''

    def strofsize(integer, remainder, level):
        if integer >= 1024:
            remainder = integer % 1024
            integer //= 1024
            level += 1
            return strofsize(integer, remainder, level)
        else:
            return integer, remainder, level

    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    integer, remainder, level = strofsize(size, 0, 0)
    if level + 1 > len(units):
        level = -1
    return ('{}.{:>03d} {}'.format(integer, remainder, units[level]))


def send_email(error):
    import smtplib
    from smtplib import SMTP_SSL
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.header import Header
    # from email.mime.application import MIMEApplication  # 用于添加附件

    host_server = 'smtp.qq.com'  # qq邮箱smtp服务器
    sender_qq = 'geektalk@qq.com'  # 发件人邮箱
    pwd = 'fbsbergtirfebdfd'
    receiver = '601872868@qq.com'
    ctime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    mail_title = f'[{ctime}]Twitter爬虫异常'  # 邮件标题

    # 邮件正文内容
    mail_content = f"<p>Twitter爬虫异常，错误信息如下：</p>\n\t{error}"

    msg = MIMEMultipart()
    msg["Subject"] = Header(mail_title, 'utf-8')
    msg["From"] = sender_qq
    msg["To"] = Header("Twitter爬虫", "utf-8")

    msg.attach(MIMEText(mail_content, 'html'))

    try:
        smtp = SMTP_SSL(host_server)  # ssl登录连接到邮件服务器
        smtp.set_debuglevel(0)  # 0是关闭，1是开启debug
        smtp.ehlo(host_server)  # 跟服务器打招呼，告诉它我们准备连接，最好加上这行代码
        smtp.login(sender_qq, pwd)
        smtp.sendmail(sender_qq, receiver, msg.as_string())
        smtp.quit()
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("无法发送邮件")
