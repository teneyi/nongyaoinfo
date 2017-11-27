#!/usr/bin/env python
# -*- coding:utf-8 -*-

#---python3环境
#爬取网站：http://www.chinapesticide.gov.cn

import urllib.request
import urllib.parse
import http.cookiejar,re
from lxml import etree
import pymysql
import time
#from json import dumps
headers={
'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Accept-Encoding':'gzip, deflate',
'Accept-Language':'zh-CN,zh;q=0.9',
'Cache-Control':'max-age=0',
'Connection':'keep-alive',
'Content-Type':'application/x-www-form-urlencoded',
'Origin:http':'//www.chinapesticide.gov.cn',
'Referer':'http://www.chinapesticide.gov.cn/myquery/queryselect',
'Upgrade-Insecure-Requests':'1',
'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',

}
#设置cookie
cjar = http.cookiejar.CookieJar() #得到cookie对象
cookie = urllib.request.HTTPCookieProcessor(cjar)#实例化对象
opener = urllib.request.build_opener(cookie) #绑定cookie

opener.headers=headers
urllib.request.install_opener(opener) #载入opener

#-----------------------------
def clean(listdata=[],table_data_zang=[]):
    for i in table_data_zang:
        i=str(i).replace("['","").replace("']","")
        i = i.replace('\\r', '')
        i = i.replace('\r', '')
        i = i.replace('\\t', '')
        i = i.replace('\t', '')
        i = i.replace('\\n', '')
        i = i.replace('\n', '')
        i = i.replace(' ', '')
        listdata.append(i)
    return listdata
##1.查询数据
def nongyao(pageNo):
    url='http://www.chinapesticide.gov.cn/myquery/queryselect'
    req=urllib.request.Request(url)
    data={
        'pageNo': pageNo,#页数
        'pageSize': '20',#每页显示多少个
        'djzh':'',#登记证号
        'cjmc':'',#厂家名称
        'sf':'',#省份
        'nylb':'',#农药类别
        'zhl':'',
        'jx':'',
        'zwmc': '',#作物名称
        'fzdx':'',#防治对象
        'syff':'',#施用方法
        'dx':'',
        'yxcf':'',
        'yxcf_en':'',
        'yxcfhl':'',
        'yxcf2':'',
        'yxcf2_en':'',
        'yxcf2hl':'',
        'yxcf3':'',
        'yxcf3_en':'',
        'yxcf3hl':'',
        'yxqs_start':'',
        'yxqs_end':'',
        'yxjz_start':'',
        'yxjz_end':'',
        'dj': '1',
        'hj': '1',
        'ygq': '1'
    }
    data = urllib.parse.urlencode(data).encode('utf-8')
    html = opener.open(req,data=data).read().decode('utf-8')
    selector = etree.HTML(html)
    table_title = selector.xpath('//table[@id="tab"]/tr//td/p/text()')
    #print(table_title)#表头字段
    if pageNo==1:
        totalpage=selector.xpath('/html/body/div/div/ul/li/a/text()')
        totalpage=totalpage[-5]
        print(totalpage)
        selec_data={'totalpage':totalpage,'pageNo':pageNo,'data':[]}
    else:
        selec_data = {'pageNo':pageNo,'data':[]}
    for i in range(2,22):
        i_str="tr["+str(i)+"]"
        table_data_zang=["","","","","","",""]
        table_data_a = selector.xpath('//table[@id="tab"]/'+i_str+'/td/span/a/text()')#有登记证号、生产企业 等需要合并
        table_data_zang[0] = selector.xpath('//*[@id="tab"]/' + i_str + '/td[1]/span/a/text()')  # 登记证号
        table_data_zang[1] = selector.xpath('//*[@id="tab"]/'+i_str+'/td[2]/span/text()')#登记名称
        table_data_zang[2] = selector.xpath('//*[@id="tab"]/'+i_str+'/td[3]/span/text()')#农药类别
        table_data_zang[3] = selector.xpath('//*[@id="tab"]/'+i_str+'/td[4]/span/text()')#剂型
        table_data_zang[4] = selector.xpath('//*[@id="tab"]/'+i_str+'/td[5]/span/text()')#总含量
        table_data_zang[5] = selector.xpath('//*[@id="tab"]/'+i_str+'/td[6]/span/text()')#有效期至
        table_data_zang[6] = selector.xpath('//*[@id="tab"]/'+i_str+'/td[7]/span/a/text()')#生产企业
        #print(table_data_zang)
        #登记证号点击查看，需要的post 信息
        djzh_post1,djzh_post2=selector.xpath('//table[@id="tab"]/'+i_str+'/td[1]/span/a/@href')[0].replace('javascript:open(','').replace(')','').replace("'",'').split(',')
        #点击企业信息查看详细，需要的post数据
        scqy_post1, scqy_post2=selector.xpath('//table[@id="tab"]/'+i_str+'/td[7]/span/a/@href')[0].replace('javascript:opencompany(','').replace(')','').replace("'",'').split(',')
        table_data_clean=clean([],table_data_zang)#清洗往后保存这列表
        ##最后拼接成字典
        #print(table_data_clean)
        data_dict={table_title[0]:table_data_clean[0],
                   table_title[1]: table_data_clean[1],
                   table_title[2]: table_data_clean[2],
                   table_title[3]: table_data_clean[3],
                   table_title[4]: table_data_clean[4],
                   table_title[5]: table_data_clean[5],
                   table_title[6]: table_data_clean[6],
                   'pdno':djzh_post1,
                   'pdrgno':djzh_post2,
                   'cid':scqy_post1,
                   'c_id':scqy_post2
                   }
        selec_data['data'].append(data_dict)
        #print(selec_data)
    return (selec_data)#数据形式select_data["pageNo":pageNo,'data',data] #其中data是字典形式
#2.点击登记证号 查看详情
def dengjiInfo(pdno,pdrgno):
    url='http://www.chinapesticide.gov.cn/myquery/querydetail?pdno=%s&pdrgno=%s'%(pdno,pdrgno)
    html = opener.open(url).read().decode('utf-8')
    dengjiinfo={}
    selector=etree.HTML(html)
    d_1=selector.xpath('//table[1]/tr[2]/td[2][@class="tab_lef_bot"]/text()')#登记证号
    d_2=selector.xpath('//table[1]/tr[2]/td[4][@class="tab_lef_bot"]/text()')#有效起始日
    d_3=selector.xpath('//table[1]/tr[2]/td[6][@class="tab_lef_bot_rig"]/text()')#有效截止日
    d_4=selector.xpath('//table[1]/tr[3]/td[2][@class="tab_lef_bot"]/text()')#登记名称
    d_5=selector.xpath('//table[1]/tr[3]/td[4][@class="tab_lef_bot"]/text()')#毒性
    d_6=selector.xpath('//table[1]/tr[3]/td[6][@class="tab_lef_bot_rig"]/text()')#剂型
    d_7=selector.xpath('//table[1][@id="reg"]/tr[4]/td[2][@class="tab_lef_bot"]/a/text()')#生产厂家
    d_8=selector.xpath('//table[1][@id="reg"]/tr[5]/td[2][@class="tab_lef_bot_rig"]/text()')#总含量
    d_9=selector.xpath('//table[1][@id="reg"]/tr[6]/td[2][@class="tab_lef_bot_rig"]/text()')#备注
    #d_99=selector.xpath('//table[2][@id="reg"]/tr[2]/td[1][@class="tab_lef_bot"]/a/@href')#标签信息(查看的连接)
    d_10=selector.xpath('//table[3][@id="reg"]/tr[3]/td[1][@class="tab_lef_bot"]/text()')#有效成分
    d_11=selector.xpath('//table[3][@id="reg"]/tr[3]/td[2][@class="tab_lef_bot_rig"]/text()')#有效成分含量
    d_12=selector.xpath('//table[4][@id="reg"]/tr[3]/td[1][@class="tab_lef_bot"]/text()')#作物
    d_13=selector.xpath('//table[4][@id="reg"]/tr[3]/td[2][@class="tab_lef_bot"]/text()')#防治对象
    d_14=selector.xpath('//table[4][@id="reg"]/tr[3]/td[3][@class="tab_lef_bot"]/text()')#有效成分用药量
    d_15=selector.xpath('//table[4][@id="reg"]/tr[3]/td[4][@class="tab_lef_bot_rig"]/text()')#施用方法
    dengjiinfo["登记证号"]=clean([],d_1)
    dengjiinfo["有效起始日"]=clean([],d_2)
    dengjiinfo["有效截止日"]=clean([],d_3)
    dengjiinfo["登记名称"]=clean([],d_4)
    dengjiinfo["毒性"]=clean([],d_5)
    dengjiinfo["剂型"]=clean([],d_6)
    dengjiinfo["生产厂家"]=clean([],d_7)
    dengjiinfo["总含量"]=clean([],d_8)
    dengjiinfo["备注"]=clean([],d_9)
    dengjiinfo["有效成分"]=clean([],d_10)
    dengjiinfo["有效成分含量"]=clean([],d_11)
    dengjiinfo["作物"]=clean([],d_12)
    dengjiinfo["防治对象"]=clean([],d_13)
    dengjiinfo["有效成分用药量"]=clean([],d_14)
    dengjiinfo["施用方法"]=clean([],d_15)
    return (dengjiinfo)
def qiyeInfo(cid,c_id):
    url="http://www.chinapesticide.gov.cn/myquery/companydetail?cid=%s&c_id=%s"%(cid,c_id)
    html = opener.open(url).read().decode('utf-8')
    qiyeinfo={}
    selector=etree.HTML(html)
    qiyeinfo["企业的所有登记证"]=selector.xpath('//table[1][@id="reg"]/tr[1]/td/a/@href')#企业的所有登记证
    qiyeinfo["单位名称"]=clean([],selector.xpath('//table[1][@id="reg"]/tr[2]/td[2]/a/text()'))#单位名称
    qiyeinfo["省份"]=selector.xpath('//table[1][@id="reg"]/tr[2]/td[4]/text()')#省份
    qiyeinfo["国家"]=selector.xpath('//table[1][@id="reg"]/tr[2]/td[6]/text()')#国家
    qiyeinfo["县"]=selector.xpath('//table[1][@id="reg"]/tr[3]/td[2]/text()')#县
    qiyeinfo["乡镇街道"]=selector.xpath('//table[1][@id="reg"]/tr[3]/td[4]/text()')#乡镇街道
    qiyeinfo["邮编"]=selector.xpath('//table[1][@id="reg"]/tr[3]/td[6]/text()')#邮编
    qiyeinfo["电话"]=selector.xpath('//table[1][@id="reg"]/tr[4]/td[2]/text()')#电话
    qiyeinfo["传真"]=selector.xpath('//table[1][@id="reg"]/tr[4]/td[4]/text()')#传真
    qiyeinfo["联系人"]=selector.xpath('//table[1][@id="reg"]/tr[4]/td[6]/text()')#联系人
    qiyeinfo["单位地址"]=selector.xpath('//table[1][@id="reg"]/tr[5]/td[2]/text()')#单位地址
    qiyeinfo["E-Mail"]=selector.xpath('//table[1][@id="reg"]/tr[5]/td[4]/text()')#E-Mail
    qiyeinfo["单位类型"]=selector.xpath('//table[1][@id="reg"]/tr[5]/td[6]/text()')#单位类型
    qiyeinfo["企业简介"]=selector.xpath('//table[2][@id="reg"]/tr[2]/td/p/text()')#企业简介
    return (qiyeinfo)

def tagDetail(pdrgno):
    url="http://www.chinapesticide.gov.cn/myquery/tagdetail?pdno=%s"%pdrgno
    html = opener.open(url).read().decode('utf-8')
    return (html)

# nongyao1=nongyao(name='水稻',pageNo=1)['data']
# pdno,pdrgno,cid,c_id=nongyao1[1]['pdno'],nongyao1[1]['pdrgno'],nongyao1[1]['cid'],nongyao1[1]['c_id']
# print('--->',pdno,pdrgno,cid,c_id)
# # dengjiinfo=dengjiInfo(pdno,pdrgno)
# # qiyeinfo=qiyeInfo(cid,c_id)
# tagDetail(pdrgno)

#http://www.chinapesticide.gov.cn/myquery/queryselect 查询地址 method=post
#http://www.chinapesticide.gov.cn/myquery/querydetail?pdno=3EBE8A0B55124701A8D2D127937D8EA4&pdrgno=PD20080895 登记信息method=get
#http://www.chinapesticide.gov.cn/myquery/tagdetail?pdno=PD20080895 详细信息method=get
#http://www.chinapesticide.gov.cn/myquery/companydetail?cid=98A3FE29319E4FB6AF8CA10CC8041E90&c_id=F33012601 企业信息

#-----------保存文件形式---------------------
# data=nongyao(name='水稻',pageNo=3)
# data=dumps(data,ensure_ascii=False)
# with open('水稻农药登记数据.json','a',encoding='utf-8',newline='\n') as f:
#     f.write(data)
#连接数据库，保存到数据库

conn=pymysql.connect(host="192.168.30.161",user='root',passwd='',db='nongyao',port=3306,charset='utf8')#查数据时出现问号需要charset=utf8
cursor = conn.cursor()#创建游标
# datas=nongyao(name='水稻',pageNo=10)['data']
###《-------------------------第一层爬取：：——————————--》
totalpage=int(nongyao(pageNo=1)['totalpage'])-1 ##获取总页码
for page in range(1,totalpage):#range(1,397)
    datas=nongyao(pageNo=page)['data']
    if page==1:
        print("总页码：",totalpage)
    print('爬取页码：----->',page)
    for data in datas:
        sql="insert into nongyaoinfo(id,djzh ,djmc ,nylb,jx ,zhl ,yxqz ,scqy,pdno,pdrgno,cid,c_id ) VALUE(uuid(),'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(data['登记证号'],data['登记名称'],data['农药类别'],data['剂型'],data['总含量'],data['有效期至'],data['生产企业'],data['pdno'],data['pdrgno'],data['cid'],data['c_id'])
        cursor.execute(sql)
    conn.commit()
    time.sleep(0.1)

# datas=datas(name='水稻',pageNo=1)['data']
# pdno,pdrgno,cid,c_id=datas[1]['pdno'],datas[1]['pdrgno'],datas[1]['cid'],datas[1]['c_id']
# print('--->',pdno,pdrgno,cid,c_id)
# # dengjiinfo=dengjiInfo(pdno,pdrgno)
# # qiyeinfo=qiyeInfo(cid,c_id)
# tagDetail(pdrgno)

#创建表：输入表名，id是自动的 dict={"name":"varchar(50)","code":"varchar(30)"}
# def createtable(table,**fields):
#     sql='''CREATE TABLE IF NOT EXISTS %s(
#         id varchar(100)
#     '''%table
#     for key,value in flieds.items():
#         sql=sql+','+key+" "+value
#     sql=sql+')'
#     cursor.execute(sql)
#     conn.commit()
##表nongyaoinfo创建
# fields={'djzh':"varchar(50)" ,'djmc':"varchar(50)" ,'nylb':"varchar(50)",'jx':"varchar(50)" ,'zhl':"varchar(50)" ,'yxqz':"varchar(50)" ,'scqy':"varchar(50)",'pdno':"varchar(50)",'pdrgno':"varchar(50)",'cid':"varchar(50)",'c_id':"varchar(50)" }
# createtable(table='nongyaoinfo',**fields)#使用方法
###表dengjiinfo创建
# fields={ 'd_1':'varchar(50)',#登记证号
#          'd_2':'varchar(50)',#有效起始日
#          'd_3':'varchar(50)',#有效截止日
#          'd_4':'varchar(50)',#登记名称
#          'd_5':'varchar(50)',#毒性
#          'd_6':'varchar(50)',#剂型
#          'd_7':'varchar(50)',#生产厂家
#          'd_8':'varchar(50)',#总含量
#          'd_9':'varchar(50)',#备注
#          'd_10':'varchar(50)',#有效成分
#          'd_11':'varchar(50)',#有效成分含量
#          'd_12':'varchar(50)',#作物
#          'd_13':'varchar(50)',#防治对象
#          'd_14':'varchar(50)',#有效成分用药量
#          'd_15':'varchar(50)',#施用方法
# }
# createtable(table='degnjiinfo',**fields)#使用方法
#fields=
# 关闭游标
cursor.close()
# 关闭连接
conn.close()
