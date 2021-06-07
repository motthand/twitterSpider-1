#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: run.py
@Ide: PyCharm
@Time: 2021-06-07 18:03:50
@Desc: 
"""
from analysis.thread_pool import DataExtractPool
from utils.logger import logger


class DataAnalysis(DataExtractPool):

    def __init__(self):
        super(DataAnalysis, self).__init__()

    def get_allTwitterInfo(self):
        """获取所有推特用户信息"""

        for spider, analysis in self.redis_name_item.get('spider_analysis_info').items():
            logger.info(f"[用户信息]正在处理redis数据库并获取:{spider} {analysis}")
            self.get_UserInfoPool(name=spider, new_name=analysis)

        for num, (spider, analysis) in enumerate(self.redis_name_item.get('spider_analysis').items()):
            logger.info(f"[用户数据]正在处理redis数据库并获取:{spider} {analysis}")
            self.get_UserDataInfoPool(name=spider, new_name=analysis)

    def init_downloadInfo(self):
        analysis = self._analysis_Info
        analysis_download = self._analysis_downloadInfo
        logger.info(f"[用户数据]正在处理redis数据库并获取:{analysis} {analysis_download}")
        self.get_DownloadInfoPool(name=analysis, new_name=analysis_download)

if __name__ == '__main__':
    d = DataAnalysis()
    # d.get_allTwitterInfo()
    d.init_downloadInfo()