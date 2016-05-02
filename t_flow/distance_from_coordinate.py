# -*- coding: utf-8 -*-  
'''
Created on 2014年10月26日

@author: zsy

请求百度API得到单子起点终点之间的距离

'''
import urllib2
from urllib2 import urlopen
import json
import time
from threading import Lock
def trans_coordinate_to_distance_one(sourceLon,sourceLat,targetLon,targetLat):
        url = "http://api.map.baidu.com/direction/v1?mode=driving&origin="+sourceLat+","+sourceLon+"&destination="+targetLat+","+targetLon+"&origin_region=西安&destination_region=西安&output=json&ak=53fb5a6c9ddba227f1af1992d3476d6d"
        req = urllib2.Request(url)
        try:
            print("req begin")
            res_data = urllib2.urlopen(req,timeout=5)
            print("req over")
            res = json.loads(res_data.read(),encoding='UTF-8')
            print("res.read")
            result=res['result']
        except Exception as e:
            print(e)
        try:
            day_price=result['taxi']['detail'][0]['total_price']
            night_price=result['taxi']['detail'][1]['total_price']
            duration=result['taxi']['duration']
            distance=result['taxi']['distance']
            eDistance=distance 
#         time.sleep(0.1)
        except Exception as e:
            print(e)
        print(duration,distance)
        return (duration,distance)
def trans_coordinate_to_distance():#请求百度API得到单子起点终点之间的距离
    '''
    输入：出租车单子记录，没有计算起点到终点距离的
    输出：出租车单子记录，计算起点终点估算距离
     处理过程：
     从文件1中读取每一行记录，通过字符串分割获取各字段值。
     请求百度地图API计算距离信息，返回信息。
     重新赋值字段信息，将新的字段信息写入文件2：
    '''
    f=open("track\\trackAllTaxi20141021.txt")
    fw=open("track\\trackAllTaxi20141027_edistance.txt","a")
    line=f.readline()
    j=0
    while line!="\n" and line:
        j+=1
        if j<=624272:#过滤掉已经计算过的
            line=f.readline()
            continue
        (i,taxiNum,sourceLon,sourceLat,targetLon,targetLat,beginTime,endTime,lastTime,rDistance,eDistance)=line.split(",")
#         sourceLat="34.329559"
#         sourceLon="108.940990"
#         targetLat="34.306566"
#         targetLon="108.937715"
        url = "http://api.map.baidu.com/direction/v1?mode=driving&origin="+sourceLat+","+sourceLon+"&destination="+targetLat+","+targetLon+"&origin_region=西安&destination_region=西安&output=json&ak=53fb5a6c9ddba227f1af1992d3476d6d"
        req = urllib2.Request(url)
        try:
            print("req begin")
            res_data = urllib2.urlopen(req,timeout=5)
            print("req over")
            res = json.loads(res_data.read(),encoding='UTF-8')
            print("res.read")
            result=res['result']
        except Exception as e:
            print(e)
            print(i,taxiNum,sourceLon,sourceLat,targetLon,targetLat,beginTime,endTime,lastTime,rDistance,eDistance)
            line=f.readline()
            continue
        try:
            day_price=result['taxi']['detail'][0]['total_price']
            night_price=result['taxi']['detail'][1]['total_price']
            duration=result['taxi']['duration']
            distance=result['taxi']['distance']
            eDistance=distance 
            res=str(i)+","+taxiNum+","+sourceLon+","+sourceLat+","+targetLon+","+targetLat+","+beginTime+","+endTime+","+lastTime+","+rDistance+","+str(duration)+","+str(eDistance)+","+str(day_price)+","+str(night_price)+'\n'         
            print("print res")
            print(res)
            fw.writelines(res) 
            print("print res over")
            
#         time.sleep(0.1)
        except Exception as e:
            print(e)
            print(result)
            line=f.readline()
            continue
        
        line=f.readline()
    f.close()
    fw.close()
    
def select_distance():
    lon_min = 108.7186431885
    lon_max = 109.1677093506
    lat_min = 34.0950320079
    lat_max = 34.4711842426
    f=open("D:\\myeclipseworkspace\\TaxiStatistics\\t_flow\\track\\trackAllTaxi20141027_edistance.txt")
    fw=open("D:\\myeclipseworkspace\\TaxiStatistics\\t_flow\\track\\trackAllTaxi20141027_edistance_filtered.txt","a")
    line=f.readline()
    i=0
    while line!="\n" and line:    
        i+=1
        print(i)
        linelist=line.split(",") 
        (sourceLon,sourceLat,targetLon,targetLat)=(float(linelist[2]),float(linelist[3]),float(linelist[4]),float(linelist[5]))  
        if sourceLon>lon_min and sourceLon<lon_max and sourceLat>lat_min and sourceLat<lat_max and targetLon>lon_min and targetLon<lon_max and targetLat>lat_min and targetLat<lat_max :   
            fw.writelines(line) 
        line=f.readline()
    fw.close()
    f.close()
def select_duration():
    max=7200
    f=open("D:\\myeclipseworkspace\\TaxiStatistics\\t_flow\\track\\trackAllTaxi20141027_edistance_filtered.txt")
    fw=open("D:\\myeclipseworkspace\\TaxiStatistics\\t_flow\\track\\trackAllTaxi20141027_edistance_filtered_duration.txt","a")
    line=f.readline()
    i=0
    while line!="\n" and line:    
        i+=1
        print(i)
        linelist=line.split(",") 
        dr=int(linelist[13])
        if dr<=7200:
            fw.writelines(line) 
        line=f.readline()
    fw.close()
    f.close()
if __name__ == '__main__':
#     trans_coordinate_to_distance()
#     trans_coordinate_to_distance_one('108.954196','34.263022','108.931826','34.224762')
#     trans_coordinate_to_distance_one('123.019343','26.069988','108.945017','34.269968')
#     select_distance()
    select_duration()