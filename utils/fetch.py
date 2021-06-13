#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: fetch.py
@Ide: PyCharm
@Time: 2021-05-29 13:53:43
@Desc: request请求
"""
import json

import requests
from requests import Response
from retrying import retry

from init import ConfigAnalysis
from utils.logger import logger
from utils.utils import url_md5

__info = ConfigAnalysis().load_fetch()
retry_max_number = __info.get('retry_max_number')
retry_min_random_wait = __info.get('retry_min_random_wait')
retry_max_random_wait = __info.get('retry_max_random_wait')
fetch_timeout = __info.get('fetch_timeout')
proxies = __info.get('proxies')
User_Agent = __info.get('User_Agent')


def need_retry(exception):
    result = isinstance(exception, (requests.ConnectionError, requests.ReadTimeout))
    if result:
        logger.warning(f'Exception: {type(exception)} occurred, retrying...')
    return result


def init_request_headers(session, **kwargs):
    session.proxies.update(proxies)
    session.headers.update(User_Agent)
    # 删除无值的params
    _params = kwargs.get('params')
    if _params is None:
        return session, kwargs

    params = _params.copy()
    for key, value in _params.items():
        if value is None:
            del params[key]

    if kwargs.get('headers'):
        kwargs['headers'].update({'timeout': str(fetch_timeout)})

    return session, kwargs


def fetch(session, url, method='get', check_code=True, **kwargs):
    @retry(stop_max_attempt_number=retry_max_number, wait_random_min=retry_min_random_wait,
           wait_random_max=retry_max_random_wait, retry_on_exception=need_retry)
    def _fetch(session, url, check_code, **kwargs) -> Response:
        response = session.post(url, **kwargs) if method == 'post' else session.get(url, **kwargs)
        if check_code:
            if response.status_code != 200:
                error_info = f'Expected status code 200, but got {response.status_code}.'
                raise requests.ConnectionError(error_info)
        return response

    try:
        session, kwargs = init_request_headers(session, **kwargs)
        resp = _fetch(session, url, check_code, **kwargs)
        return resp
    except Exception as e:
        error_info = 'Something got wrong, error msg:{}'.format(e)
        info = {
            "url": url,
            "check_code": check_code,
            "kwargs": kwargs,
            "headers": session.headers,
        }
        raise Exception(error_info + "\n\n请求参数：" + str(info))


def fetch_json(session, url, method='get', **kwargs):
    response = fetch(session, url, method, **kwargs)
    response_dict = json.loads(response.text)
    response_dict['url_md5'] = url_md5(url, kwargs.get('params'))
    return json.dumps(response_dict, sort_keys=True, ensure_ascii=False)


def fetch_dict(session, url, method='get', **kwargs):
    return json.loads(fetch_json(session, url, method, **kwargs))
