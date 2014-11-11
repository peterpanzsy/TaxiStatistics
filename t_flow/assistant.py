# -*- coding: utf-8 -*-  
'''
Created on 2014年10月26日

@author: zsy
'''
import csv
import codecs
import datetime
def deal_file():
    f=open("track\\trackAllTaxi20141021.txt")
    fw=open("track\\trackAllTaxi20141021temp.txt","a")
    line=f.readline()
    i=0
    while line!="\n" and line:
        i+=1
        if i<=274415:
            fw.writelines(line)
            line=f.readline()
            print(i)
        else:
            break
    f.close()
    fw.close()

def split_gps_log_by_taxi():
    '''
    将15:30到17:00之间的数据按车牌分割文件
    '''
    i=0
    with codecs.open("F:\\TaxiData\\gps_log1530_1700.csv",'rb',encoding='gbk') as f : # 使用with读文件
        for line in f:
            i+=1
            if i==1:
                continue
#             taxino=line.replace('陕','')
            
            line=line.encode('utf-8').replace("陕",'').replace("\"",'')
            taxino=line.split(",")[0]
            print(taxino)
            writer=csv.writer(file('F:\\TaxiData\\gps_log_1530_1700\\gps_log_1530_1700_'+taxino+'.csv','ab'))
            writer.writerow(line.split(","))
            print(line)
            
if __name__ == '__main__':
    split_gps_log_by_taxi()