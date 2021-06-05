# 推特爬虫

声明：本项目仅供学习和参考，非商用，欢迎大家fork

### 项目树

```
.
├── README.md
├── analysis
│   ├── __init__.py
│   └── data_analysis.py	# 数据解析
├── api
│   ├── __init__.py
│   ├── login.py	# 登录API
│   ├── spider.py	# 爬虫API
│   └── twitter.py	# 推特API
├── config.ini	# 配置信息
├── config.py
├── download
│   ├── __init__.py
│   └── download.py	# 下载
├── extractor
│   ├── __init__.py
│   ├── extractor.py	# 提取器
│   └── json_path_finder.py	# json提取器
├── spider
│   ├── __init__.py
│   └── spider.py	# 爬虫
└── utils
    ├── __init__.py
    ├── fetch.py	# 请求处理
    ├── logger.py	# 日志
    ├── twitter_redis.py	# 推特redis
    └── utils.py	# 工具
```

使用方法：

1. 安装依赖 `pip install -r requirements.txt`
2. 在`config.ini`中配置你的代理以及账号
3. 开启reids-server
4. 自定义方法 运行spider.py爬取数据

爬取之后的数据可再次提取，也可下载，目前项目为半成品...喜欢的话麻烦给个Star



参考链接：

https://github.com/CharlesPikachu/DecryptLogin

https://github.com/kingname/JsonPathFinder