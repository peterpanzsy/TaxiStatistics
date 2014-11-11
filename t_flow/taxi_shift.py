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
from operator import itemgetter, attrgetter  
import csv
import codecs
from pyasn1.compat.octets import null

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
        self.grid_num_sum=self.row_num_sum*self.col_num_sum
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

    def stat_shift_1_taxi(self,taxi_no,i):#追踪一辆车在每个格子上的空车、熄火次数，统计15天下午3点半到5点的数据，次数多的更可能为每天交接班地点，基于内存和文件操作        
        '''
                        追踪一辆车在15天内的在每个格子的空车车次，在相应格子的空车车次上加1
                        追踪一辆车在15天内的在每个格子的熄火车次，在相应格子的熄火车次上加1
        '''
        print(datetime.datetime.now())
        stat_now="0"#车的当前状态
        (row_num_now,col_num_now,ser_num_now)=(0,0,0)#车当前所在的格子序号  
        try:
            with codecs.open('F:\\TaxiData\\gps_log_1530_1700\\gps_log_1530_1700_'+taxi_no+'.csv','rb',encoding='gbk') as f : # 使用with读文件 
                for linetemp in f:
                    if linetemp=='\"\r\n' or linetemp==None:
                        continue
                    linetemp=linetemp.encode('utf-8').replace("陕",'').replace("\"",'').replace("灯",'').replace("交",'').replace("测",'')        
                    print(linetemp)
                    linelist=linetemp.split(",")
                    
                    (gps_time,longitude,latitude,car_stat)=(linelist[2],float(linelist[3]),float(linelist[4]),linelist[9])  
                    (row_num,col_num,ser_num)=self.area_grid.getPosition(longitude,latitude)           
                    if (row_num,col_num,ser_num)==(0,0,0):#车辆不在在设定的范围内的格子里,遍历下一条记录
                        continue
                    if ser_num_now==0:#这是第一条记录，更新当前参数值
                        stat_now=car_stat
                        (row_num_now,col_num_now,ser_num_now)=(row_num,col_num,ser_num)
                        continue
                    #统计格子的空车和熄火的车次数量
                    if car_stat!=stat_now or ser_num!=ser_num_now:#如果车的状态发生改变，或者格子发生改变                      
                        if stat_now=="4":
                            (ser_num_now,row_num_now,col_num_now,empty_cou,flameout_cou)=self.grid_shift_list[ser_num_now-1]
                            self.grid_shift_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,empty_cou+1,flameout_cou) 
                        if stat_now=="7":
                            (ser_num_now,row_num_now,col_num_now,empty_cou,flameout_cou)=self.grid_shift_list[ser_num_now-1]
                            self.grid_shift_list[ser_num_now-1]=(ser_num_now,row_num_now,col_num_now,empty_cou,flameout_cou+1)               
                        (row_num_now,col_num_now,ser_num_now)=(row_num,col_num,ser_num)
                        stat_now=car_stat  
        except Exception as e:
            print(e)
            f=open('F:\\TaxiData\\shift_grid\\shift_grid_log.txt','a') 
            f.write(taxi_no+"exception!")
            f.write("\n")
            f.close()            
        print(datetime.datetime.now())
        writer=csv.writer(file('F:\\TaxiData\\shift_grid\\shift_grid_'+taxi_no+'.csv','wb'))
        grid_shift_list=sorted(self.grid_shift_list, key=itemgetter(3,4)) 
        writer.writerows(grid_shift_list) 
      
    
    def stat_shift_all_taxi(self):#追踪所有的出租车，一共11728辆，将其上车、下车的次数加入相应的区域格子，基于内存和文件

        f = open("taxi\\result1All_20141017.txt") 
        line = f.readline()  
        i=0          
        while line!="\n" and line:
            i+=1
            if i<=11734:
                line = f.readline()
                continue
            taxiNo=(line.strip('\n')).replace('陕','').replace("灯",'').replace("交",'').replace("测",'')
            print (taxiNo,i)  
            self.stat_shift_1_taxi(taxiNo,i)                     
            line = f.readline()
        f.close()  
            

if __name__ == '__main__':
    
    '''
    整个西安的经纬度范围
    '''
    lon_min = 108.7186431885
    lon_max = 109.1677093506
    lat_min = 34.0950320079
    lat_max = 34.4711842426
    '''
    三环内的经纬度范围估计值
    '''
#     lon_min=108.852775#最小经度
#     lon_max=109.056295#最大经度
#     lat_min=34.200207#最小纬度
#     lat_max=34.349599#最大纬度
    each_lon_len=85300#每单位经度的长度
    each_lat_len=111300#每单位纬度的长度
    each_grid_len=50#划分的每个格子的边长
    connstr="manage_taxi/taxixjtu@traffic"     
    taxiStatistics=TaxiStatistics(connstr,lon_min,lon_max,lat_min,lat_max,each_lon_len,each_lat_len,each_grid_len)
#     taxiStatistics.stat_shift_all_taxi()
    a=AreaGrid(lon_min,lon_max,lat_min,lat_max,each_lon_len,each_lat_len,each_grid_len)
    (lon_mi,lat_mi,lon_ma,lat_ma)=a.getGPS(203,388,155322)
    print(lon_mi,lat_mi,lon_ma,lat_ma)


    