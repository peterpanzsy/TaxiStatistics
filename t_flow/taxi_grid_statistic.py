# -*- coding: utf-8 -*-  
'''
@author: zsy

1、轨迹流：
追踪所有车一天的单子，输出源坐标、目的坐标、开始时间、结束时间、持续时间、估算路程、实际路程
2、供需分析：
需：上车率
供：下车率
追踪一辆车在一天内的上传、下车行为，每出现一次，在相应区域格子的上车或者下车计数字段加1
追踪一辆车在一天内的在每个格子的空车车次，在相应格子的空车车次上加1
追踪一辆车在一天内的在每个格子的实车车次，在相应格子的实车车次上加1  
'''
import cx_Oracle  
import datetime
import sys
import fileinput  
import thread
from operator import itemgetter, attrgetter  
import csv
import codecs
class OraConn():
    def __init__(self,connstr="manage_taxi/taxixjtu@traffic" ):
        self.connstr =connstr # "manage_taxi/taxixjtu@traffic" 
      
    def open(self):
        self.conn = cx_Oracle.Connection(self.connstr)  
        self.cur = self.conn.cursor()  
    def close(self):
        self.conn.commit();  
        self.cur.close();  
        self.conn.close(); 

          
class AreaGrid():
    '''
    按经纬度将区域划分为n个格子
    '''
    def __init__(self,lon_min,lon_max,lat_min,lat_max,each_lon_len,each_lat_len,each_grid_len):
        self.lon_min=lon_min#最小经度
        self.lon_max=lon_max#最大经度
        self.lat_min=lat_min#最小纬度
        self.lat_max=lat_max#最大纬度
        self.each_lon_len=each_lon_len#每单位经度的长度
        self.each_lat_len=each_lat_len#每单位纬度的长度
        self.each_grid_len=each_grid_len#划分的每个格子的边长
        if ((self.lon_max-self.lon_min)*self.each_lon_len)%self.each_grid_len==0:    
            self.col_num_sum=int(((self.lon_max-self.lon_min)*self.each_lon_len)//self.each_grid_len)
        else:
            self.col_num_sum=int(((self.lon_max-self.lon_min)*self.each_lon_len)//self.each_grid_len)+1#总列数，最后一个格子可能不足 each_grid_len的长度
        if ((self.lat_max-self.lat_min)*self.each_lat_len)%self.each_grid_len==0:
            self.row_num_sum=int(((self.lat_max-self.lat_min)*self.each_lat_len)//self.each_grid_len)
        else:
            self.row_num_sum=int(((self.lat_max-self.lat_min)*self.each_lat_len)//self.each_grid_len)+1#总行数，最后一个格子可能不足 each_grid_len的长度
        self.grid_num_sum=self.row_num_sum*self.col_num_sum #838*767=642746
    def getPosition(self,lon_v,lat_v):#根据经纬度，返回该地点在第几个格子
        if lon_v>self.lon_min and lon_v<self.lon_max and lat_v>self.lat_min and lat_v<self.lat_max:            
            mod_res=(lon_v-self.lon_min)*self.each_lon_len%self.each_grid_len
            if mod_res==0:
                col_num=int((lon_v-self.lon_min)*self.each_lon_len//self.each_grid_len)
            else:
                col_num=int((lon_v-self.lon_min)*self.each_lon_len//self.each_grid_len)+1
            mod_res=(lat_v-self.lat_min)*self.each_lat_len%self.each_grid_len
            if mod_res==0:
                row_num=int((lat_v-self.lat_min)*self.each_lat_len//self.each_grid_len)
            else:
                row_num=int((lat_v-self.lat_min)*self.each_lat_len//self.each_grid_len)+1
            ser_num=self.col_num_sum*(row_num-1)+col_num
            return (row_num,col_num,ser_num)
        else:
            return (0,0,0)
    def getGPS(self,row_num,col_num,ser_num):
        lon_mi=self.lon_min+(col_num-1)*(self.each_grid_len*1.0/self.each_lon_len)
        lon_ma=self.lon_min+col_num*(self.each_grid_len*1.0/self.each_lon_len)
        lat_mi=self.lat_min+(row_num-1)*(self.each_grid_len*1.0/self.each_lat_len)
        lat_ma=self.lat_min+(row_num)*(self.each_grid_len*1.0/self.each_lat_len)
        return (lon_mi,lat_mi,lon_ma,lat_ma)
    
class TaxiStatistics():
    def __init__(self,connstr,lon_min,lon_max,lat_min,lat_max,each_lon_len,each_lat_len,each_grid_len):
        self.connstr = connstr 
        self.lon_min=lon_min#最小经度
        self.lon_max=lon_max#最大经度
        self.lat_min=lat_min#最小纬度
        self.lat_max=lat_max#最大纬度
        self.each_lon_len=each_lon_len#每单位经度的长度
        self.each_lat_len=each_lat_len#每单位纬度的长度
        self.each_grid_len=each_grid_len#划分的每个格子的边长
        self.area_grid=AreaGrid(self.lon_min,self.lon_max,self.lat_min,self.lat_max,self.each_lon_len,self.each_lat_len,self.each_grid_len)
        self.grid_stat_list=[]#(ser_num,row_num,col_num,on_cou,off_cou,empty_cou,full_cou) 格子序号、行号、列号、上车次数、下车次数、空车车次、实车车次
        for row_n in range(1,self.area_grid.row_num_sum+1):#初始化grid_stat_list
            for col_n in range(1,self.area_grid.col_num_sum+1):  
                ser_n=(row_n-1)*self.area_grid.col_num_sum+col_n            
                self.grid_stat_list.append((ser_n,row_n,col_n,0,0,0,0))
        self.grid_shift_list=[]#(ser_num,row_num,col_num,empty_cou,flameout_cou) 格子序号、行号、列号、空车车次、熄火车次
        for row_n in range(1,self.area_grid.row_num_sum+1):#初始化grid_shift_list
            for col_n in range(1,self.area_grid.col_num_sum+1):  
                ser_n=(row_n-1)*self.area_grid.col_num_sum+col_n            
                self.grid_shift_list.append((ser_n,row_n,col_n,0,0))
#         self.conn = cx_Oracle.Connection(self.connstr)  
#         self.cur = self.conn.cursor()  
     
  
    def track_one_taxi(self,taxiNum):#追踪一辆车一天的单子，包括起点、终点，出发时间、到达时间、持续时间
        proBeginTime=datetime.datetime.now()
        print(proBeginTime)
        pr={'taxiNo':taxiNum}
        #查询出该车的所有记录（一天）
        conn = cx_Oracle.Connection(self.connstr)  
        cur = conn.cursor() 
        #sql="select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG0101 t where t.licenseplateno like :taxiNo order by t.gps_time"
        sql="select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG20140111 t where t.licenseplateno like :taxiNo order by t.gps_time"
        result=cur.execute(sql,pr) 
        row=cur.fetchone() 
#         file_object = open('track100Taxi20141016.txt', 'a')
#         file_object = open('track\\trackAllTaxi20141021.txt', 'a')
        file_object = open('F:\\TaxiData\\track\\trackAllTaxi20140111at20160504.txt', 'a')
#         file_object.write("\n"+taxiNum+"的轨迹:\n")             
 
        rDistance=""
        eDistance=""
        statusNow="4"
        i=0
        while row:
            (gps_time,longitude,latitude,car_stat1)=(row[0],row[1],row[2],row[3])
            if car_stat1!=statusNow:
                if statusNow=="4" and car_stat1=="5":#上车，出发
                    sourceLon=longitude
                    sourceLat=latitude
                    beginTime=gps_time
                    statusNow="5"
                elif statusNow=="5" and car_stat1=="4":#下车，到达目的地
                    targetLon=longitude
                    targetLat=latitude
                    endTime=gps_time
                    statusNow="4"
                    lastTime=endTime-beginTime
                    #traceTuple=(sourceLon,sourceLat,targetLon,targetLat,beginTime,endTime,lastTime,"","")
                    i+=1
                    res=str(i)+","+taxiNum+","+sourceLon+","+sourceLat+","+targetLon+","+targetLat+","+beginTime.strftime("%Y-%m-%d %H:%M:%S")+","+endTime.strftime("%Y-%m-%d %H:%M:%S")+","+lastTime.__str__()+","+rDistance+","+eDistance+'\n'
                    print (res)
                    file_object.writelines(res) 
                 
            row=cur.fetchone ()  
        conn.close()     
        proEndTime=datetime.datetime.now()
        print(proEndTime)
        file_object.close()   
    def track_all_taxi(self):#追踪所有的车一天的单子，包括车牌、起点、终点，出发时间、到达时间、持续时间
#         f=open("track\\trackAllTaxi20141021.txt")#获取最后一条已经跟踪的记录
        f = open("F:\\TaxiData\\track\\trackAllTaxi20140111at20160504.txt");
        linecount=len(f.readlines())
        f.close()
#         f=open("track\\trackAllTaxi20141021.txt")
        f = open("F:\\TaxiData\\track\\trackAllTaxi20140111at20160504.txt");
        targetLine = "";
        track_tuple=""
        lineNo = 0;  
        while 1:
            mLine = f.readline();
            if not mLine:
                break;
            lineNo += 1;
            if linecount==lineNo:
                targetLine = mLine;
                track_tuple=(i,taxiNum,sourceLon,sourceLat,targetLon,targetLat,beginTime,endTime,lastTime,rDistance,eDistance)=targetLine.split(",")
        f.close()
        
        
        f=open("taxi\\result1All_20141017.txt")#过滤掉已经追踪过的车牌
        if track_tuple!="":
            line = f.readline()  
            while line!="\n" and line:
                taxiNo=(line.strip('\n')).replace('陕','%')
                if taxiNo==taxiNum:
                    break
                line=f.readline()
            
        line=f.readline()#从最后一辆车的下一辆开始追踪
        # 调用文件的 readline()方法
        while line!="\n" and line:
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n')).replace('陕','%')
            print (taxiNo)  
            self.track_one_taxi(taxiNo)                     
            line = f.readline()
        f.close()  
   
    def stat_grid_all_taxi_meanwhile(self):#同时追踪所有车在每个格子上的行为次数，基于内存和文件操作        
        '''
                        追踪所有车在一天内的上传、下车行为，每出现一次，在相应区域格子的上车或者下车计数字段加1
                        追踪所有车在一天内的在每个格子的空车车次，在相应格子的空车车次上加1
                        追踪所有车在一天内的在每个格子的实车车次，在相应格子的实车车次上加1
        '''
        taxi_state_dict={}
        f = open("taxi\\result1All_20141017.txt") 
        line = f.readline()  
        i=0          
        while line!="\n" and line:
            i+=1
            taxiNo=(line.strip('\n')).replace('陕','').replace("灯",'').replace("交",'').replace("测",'')
            print (taxiNo) 
            taxi_state_dict[taxiNo]=("0",0)   #车的当前状态 ,车当前所在的格子序号            
            line = f.readline()
        f.close()  
        with codecs.open("F:\\TaxiData\\gps_log0101.csv",'rb',encoding='gbk') as f : # 使用with读文件
#         reader = csv.reader(file(self.filepath, 'rb'))
            i=0
            for line in f:#遍历一天的所有记录
                line=line.encode('utf-8').replace("陕",'').replace("\"",'').replace("灯",'').replace("交",'').replace("测",'')
                linelist=line.split(",")
                i+=1
                print(i,line)
                if i==1:
                    continue
                (taxiNo,gps_time,longitude,latitude,car_stat)=(linelist[1],linelist[3],float(linelist[4]),float(linelist[5]),linelist[10])         
                (row_num,col_num,ser_num)=self.area_grid.getPosition(longitude,latitude)           
                if (row_num,col_num,ser_num)==(0,0,0):#车辆不在在设定的范围内的格子里,遍历下一条记录
                    continue
                try:
                    if taxi_state_dict[taxiNo][1]==0:#获取当前车的当前格子，如果这是该车第一条记录，更新当前参数值
                        taxi_state_dict[taxiNo]=(car_stat,ser_num)
                        continue
                    stat_now=taxi_state_dict[taxiNo][0]#上一次状态
                    ser_now=taxi_state_dict[taxiNo][1]#上一次所在格子
                    taxi_state_dict[taxiNo]=(car_stat,ser_num)
                except Exception as e:
                    print(e)
                    continue
                #统计格子的空车和实车的车次数量
                if car_stat!=stat_now or ser_num!=ser_now:#如果车的状态发生改变，或者格子发生改变                      
                    if stat_now=="4":
                        (ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou)=self.grid_stat_list[ser_now-1]
                        self.grid_stat_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou+1,full_cou)
                    if stat_now=="5":
                        (ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou)=self.grid_stat_list[ser_now-1]
                        self.grid_stat_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou+1)   
           
                #统计格子的上车次数和下车次数
                if car_stat!=stat_now:
                    if stat_now=="4" and car_stat=="5":#上车行为
                        (ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou)=self.grid_stat_list[ser_now-1]
                        self.grid_stat_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,on_cou+1,off_cou,empty_cou,full_cou)
                    elif stat_now=="5" and car_stat=="4":#下车行为
                        (ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou)=self.grid_stat_list[ser_now-1]
                        self.grid_stat_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,on_cou,off_cou+1,empty_cou,full_cou)
                         
                if i%100000==0:
                    fw=open('F:\\TaxiData\\grid_on_off\\statistic_grid_0101_'+str(i)+'_taxi.csv', 'wb')
                    w=csv.writer(fw)
                    print("write result "+'F:\\TaxiData\\circle_on_off\\statistic_grid_0101_'+str(i)+'_taxi.csv')
                    w.writerows(self.grid_stat_list) 
                    fw.close()   
                continue
        f=open('F:\\TaxiData\\grid_on_off\\statistic_grid_0101_all_taxi.csv', 'wb')
        w=csv.writer(f)
        print("write result F:\\TaxiData\\statistic_grid_0101_all_taxi.csv")
        w.writerows(self.grid_stat_list)
        f.close()

    def stat_grid_1_taxi(self,taxi_no,i):#追踪一辆车在一个格子上的行为次数，基于内存和文件操作        
        '''
                        追踪一辆车在一天内的上传、下车行为，每出现一次，在相应区域格子的上车或者下车计数字段加1
                        追踪一辆车在一天内的在每个格子的空车车次，在相应格子的空车车次上加1
                        追踪一辆车在一天内的在每个格子的实车车次，在相应格子的实车车次上加1
        '''
        stat_now="4"#车的当前状态
        (row_num_now,col_num_now,ser_num_now)=(0,0,0)#车当前所在的格子序号  
        ora_con=OraConn(self.connstr)
        ora_con.open()
        pr={'taxiNo':taxi_no}
        sql="select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG0101 t where t.licenseplateno like :taxiNo order by t.gps_time"
        ora_con.cur.execute(sql,pr) 
        row=ora_con.cur.fetchone() 
        while row:#遍历该车的所有记录
            (gps_time,longitude,latitude,car_stat1)=(row[0],float(row[1]),float(row[2]),row[3])
            (row_num,col_num,ser_num)=self.area_grid.getPosition(longitude,latitude)           
            if (row_num,col_num,ser_num)==(0,0,0):#车辆不在在设定的范围内的格子里,遍历下一条记录
                row=ora_con.cur.fetchone()
                continue
            if ser_num_now==0:#这是第一条记录，更新当前参数值
                stat_now=car_stat1
                (row_num_now,col_num_now,ser_num_now)=(row_num,col_num,ser_num)
                row=ora_con.cur.fetchone()
                continue
            #统计格子的空车和实车的车次数量
            if car_stat1!=stat_now or ser_num!=ser_num_now:#如果车的状态发生改变，或者格子发生改变                      
                if stat_now=="4":
                    (ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou)=self.grid_stat_list[ser_num_now-1]
                    self.grid_stat_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou+1,full_cou)
                if stat_now=="5":
                    (ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou)=self.grid_stat_list[ser_num_now-1]
                    self.grid_stat_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou+1)   
                (row_num_now,col_num_now,ser_num_now)=(row_num,col_num,ser_num)
            #统计格子的上车次数和下车次数
            if car_stat1!=stat_now:
                if stat_now=="4" and car_stat1=="5":#上车行为
                    (ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou)=self.grid_stat_list[ser_num_now-1]
                    self.grid_stat_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,on_cou+1,off_cou,empty_cou,full_cou)
                elif stat_now=="5" and car_stat1=="4":#下车行为
                    (ser_num_now,row_num_now,col_num_now,on_cou,off_cou,empty_cou,full_cou)=self.grid_stat_list[ser_num_now-1]
                    self.grid_stat_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,on_cou,off_cou+1,empty_cou,full_cou)
                stat_now=car_stat1 
            row=ora_con.cur.fetchone()              
            continue
        ora_con.close()
        if i==1 or i%100==0 or i==11728:
            fl=open('grid_cou\\statisticGridAll'+str(i)+'.txt', 'a')
            fl.write(str(i)+"\n")
            for i in self.grid_stat_list:
                fl.write(i.__str__())
                fl.write("\n")
            fl.close()
    def stat_grid_all_taxi(self):#追踪所有的出租车，一共11728辆，将其上车、下车的次数加入相应的区域格子，基于内存和文件
        f=open("grid_cou\\statisticGridAll11000.txt")#将最终统计结果加载进内存
        line=f.readline()
        num=int(line.strip("\n"))
        self.grid_stat_list=[]#(ser_num,row_num,col_num,on_cou,off_cou,empty_cou,full_cou) 格子序号、行号、列号、上车次数、下车次数、空车车次、实车车次
        line=f.readline()
        while line!="\n" and line:
            line=line.strip("(")
            line=line.strip(")\n")
            linelist=line.split(",")
            tupletemp=(int(linelist[0]),int(linelist[1]),int(linelist[2]),int(linelist[3]),int(linelist[4]),int(linelist[5]),int(linelist[6]))
            self.grid_stat_list.append(tupletemp)
            line=f.readline()
        f.close()
            
        f = open("taxi\\result1All_20141017.txt") 
        line = f.readline()  
        i=0           # 调用文件的 readline()方法
        while line!="\n" and line:
            i+=1
            if i<=num:#过滤掉已经统计过的车牌
                line = f.readline()
                continue
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n')).replace('陕','').replace("灯",'').replace("交",'').replace("测",'')
            print (taxiNo)  
            self.stat_grid_1_taxi(taxiNo,i)                     
            line = f.readline()
        f.close()  
    

if __name__ == '__main__':
    
    '''
    整个西安的经纬度范围  642746
    '''
    lon_min = 108.7186431885
    lon_max = 109.1677093506
    lat_min = 34.0950320079
    lat_max = 34.4711842426
    '''
    三环内的经纬度范围估计值115884个格子
    '''
#     lon_min=108.852775#最小经度
#     lon_max=109.056295#最大经度
#     lat_min=34.200207#最小纬度
#     lat_max=34.349599#最大纬度
    each_lon_len=85300#每单位经度的长度
    each_lat_len=111300#每单位纬度的长度
#     93980.35455699515
#     108731.79739859098

    each_grid_len=50#划分的每个格子的边长
    connstr="manage_taxi/taxixjtu@traffic"     
    taxiStatistics=TaxiStatistics(connstr,lon_min,lon_max,lat_min,lat_max,each_lon_len,each_lat_len,each_grid_len)
#     taxiStatistics.stat_grid_all_taxi_meanwhile()
    taxiStatistics.track_all_taxi()
    