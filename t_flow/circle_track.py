# -*- coding: utf-8 -*-  
'''
Created on 2014年11月3日

@author: zsy
'''
import random
# import pandas
import csv
import datetime
import codecs
# if __name__ == '__main__':
# rid=random.randint(1,34578916)
# d=pandas.read_csv("gps_log0101.csv")
# print(d[34578916])
import cx_Oracle

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
        
class RandomCircleProduce():
    def __init__(self,filepath,line_total,seed_num,seed_file,radius,each_lon_len,each_lat_len):
        self.filepath=filepath
        self.line_total=line_total
        self.seed_num=seed_num
        self.seed_file=seed_file
        self.radius=radius
        self.each_lon_len=each_lon_len
        self.each_lat_len=each_lat_len
        self.circle_state_list=[]#(ser_num,c_lon,c_lat,on_cou,off_cou,empty_cou,full_cou) 格子序号、上车次数、下车次数、空车车次、实车车次
        self.get_circle_center_list()
    def produce_random_circle_center(self):#产生随机圆的圆心坐标
        time1=datetime.datetime.now()
        print(time1)    
        ridlist=[]
        for i in range(1,10001):
            rid=random.randint(1,34578916)
            ridlist.append(rid)
        reader = csv.reader(file(self.filepath, 'rb'))
        writer=csv.writer(file(self.seed_file,'wb'))
        i=0
        for line in reader:
            i+=1
            if i in ridlist:
                print(line[4],line[5])
                writer.writerow([line[4],line[5]])
                
        time1=datetime.datetime.now()
        print(time1) 
    def get_circle_center_list(self):#初始化圆格子列表
        reader = csv.reader(file(self.seed_file, 'rb'))
        i=0
        for line in reader:
            i+=1
            c_lon=line[0]
            c_lat=line[1]
            self.circle_state_list.append((i,c_lon,c_lat,0,0,0,0))
    def lon_diff_distance(self,lon1,lon2):#根据经度差求两点的水平距离（东西距离）
        return abs(lon1-lon2)*self.each_lon_len
    def lat_diff_distance(self,lat1,lat2):#根据纬度差求两点的垂直距离（南北距离）
        return abs(lat1-lat2)*self.each_lat_len
    def gps_diff_distance(self,lon1,lat1,lon2,lat2):#根据经纬度求两点的直线距离
        lon_distance=self.lon_diff_distance(lon1,lon2)
        lat_distance=self.lat_diff_distance(lat1, lat2)
        distance=(lon_distance**2+lat_distance**2)**0.5
        return distance
    def get_circle_border(self,c_lon,c_lat):#圆的相切正方形边界经纬度
        max_lon=float(self.radius)/self.each_lon_len+c_lon
        min_lon=c_lon-float(self.radius)/self.each_lon_len
        max_lat=float(self.radius)/self.each_lat_len+c_lat
        min_lat=c_lat-float(self.radius)/self.each_lat_len
        return (max_lon,min_lon,max_lat,min_lat)
    def gps_in_circle(self,c_lon,c_lat,lon,lat):#判断经纬度坐标是否属于某个圆
        (max_lon,min_lon,max_lat,min_lat)=self.get_circle_border(c_lon, c_lat)
        if lon>=min_lon and lon<=max_lon and lat>=min_lat and lat<=max_lat:
            distance=self.gps_diff_distance(c_lon,c_lat,lon,lat)
            if distance<=self.radius:
                return True
        return False

    def stat_circle_1_taxi(self,taxi_no,i):#追踪一辆车在一个格子上的行为次数，基于内存和文件操作        
        '''
                        追踪一辆车在一天内的上传、下车行为，每出现一次，在相应区域格子的上车或者下车计数字段加1
                        追踪一辆车在一天内的在每个格子的空车车次，在相应格子的空车车次上加1
                        追踪一辆车在一天内的在每个格子的实车车次，在相应格子的实车车次上加1
        '''
        stat_now="4"#车的当前状态
        ser_now=0 #车当前所在的格子序号  
       
#         ora_con=OraConn()
#         ora_con.open()
#         pr={'taxiNo':taxi_no}
        reader = csv.reader(file(self.filepath, 'rb'))
#         sql="select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG0101 t where t.licenseplateno like :taxiNo order by t.gps_time"
#         ora_con.cur.execute(sql,pr) 
#         row=ora_con.cur.fetchone() 
#         while row:#遍历该车的所有记录
        for line in reader:
            if line[0]!=taxi_no:
                continue
            (gps_time,longitude,latitude,car_stat1)=(line[2],float(line[3]),float(line[4]),line[9]) 
#             (gps_time,longitude,latitude,car_stat1)=(row[0],float(row[1]),float(row[2]),row[3])          
            for line in self.circle_state_list:#ser_num,c_lon,c_lat,on_cou,off_cou,empty_cou,full_cou
                ser=line[0]
                c_lon=float(line[1])  
                c_lat=float(line[2])
                if self.gps_in_circle(c_lon,c_lat,longitude,latitude):   
                    if ser_now==0:#这是第一条记录，更新当前参数值
                        stat_now=car_stat1
                        ser_now=ser
#                         row=ora_con.cur.fetchone()
                        continue
                    #统计格子的空车和实车的车次数量
                    if car_stat1!=stat_now or ser!=ser_now:#如果车的状态发生改变，或者格子发生改变                      
                        if stat_now=="4":
                            (ser_now,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou)=self.circle_state_list[ser_now-1]
                            self.circle_state_list[ser_now-1]=(ser_now,lon_now,lat_now,on_cou,off_cou,empty_cou+1,full_cou)
                        if stat_now=="5":
                            (ser_now,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou)=self.circle_state_list[ser_now-1]
                            self.circle_state_list[ser_now-1]=(ser_now,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou+1)   
                        ser_now=ser
                    #统计格子的上车次数和下车次数
                    if car_stat1!=stat_now:
                        if stat_now=="4" and car_stat1=="5":#上车行为
                            (ser_now,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou)=self.circle_state_list[ser_now-1]
                            self.circle_state_list[ser_now-1]=(ser_now,lon_now,lat_now,on_cou+1,off_cou,empty_cou,full_cou)
                        elif stat_now=="5" and car_stat1=="4":#下车行为
                            (ser_now,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou)=self.circle_state_list[ser_now-1]
                            self.circle_state_list[ser_now-1]=(ser_now,lon_now,lat_now,on_cou,off_cou+1,empty_cou,full_cou)
                        stat_now=car_stat1 
#             row=ora_con.cur.fetchone()              
            continue
#         ora_con.close()
        if i==1 or i%100==0 or i==11728:
            fl=open('circle\\2statisticCircleAll'+str(i)+'.txt', 'a')
            fl.write(str(i)+"\n")
            for i in self.circle_state_list:
                fl.write(i.__str__())
                fl.write("\n")
            fl.close()
    def stat_circle_all_taxi(self):#追踪所有的出租车，一共11728辆，将其上车、下车的次数加入相应的区域格子，基于内存和文件
        num=0
        try:
            f=open("circle\\statisticCircleAll11000.txt")#将最终统计结果加载进内存
            line=f.readline()
            num=int(line.strip("\n"))
            self.circle_state_list=[]#(ser_num,c_lon,c_lat,on_cou,off_cou,empty_cou,full_cou) 格子序号、经度、纬度、上车次数、下车次数、空车车次、实车车次
            line=f.readline()
            while line!="\n" and line:
                line=line.strip("(")
                line=line.strip(")\n")
                linelist=line.split(",")
                tupletemp=(int(linelist[0]),int(linelist[1]),int(linelist[2]),int(linelist[3]),int(linelist[4]),int(linelist[5]),int(linelist[6]))
                self.circle_state_list.append(tupletemp)
                line=f.readline()
            f.close()
        except Exception as e:
            print(e)
            
        f = open("taxi\\result1All_20141017.txt") 
        line = f.readline()  
        i=0           # 调用文件的 readline()方法
        while line!="\n" and line:
            i+=1
            if num>0 and i<=num:#过滤掉已经统计过的车牌
                line = f.readline()
                continue
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n')).replace('陕','%')
            print (taxiNo)  
            self.stat_circle_1_taxi(taxiNo,i)                     
            line = f.readline()
        f.close()  
    def stat_circle_all_taxi_meanwhile(self):#同时追踪所有车在每个格子上的行为次数，基于内存和文件操作        
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
            taxiNo=(line.strip('\n')).replace('陕','').replace("灯",'')
            print (taxiNo) 
            taxi_state_dict[taxiNo]=["-1",[]]   #车的当前状态 ,车当前所在的格子序号 list           
            line = f.readline()
        f.close()  
        with codecs.open(self.filepath,'rb',encoding='gbk') as f : # 使用with读文件
#         reader = csv.reader(file(self.filepath, 'rb'))
            i=0
            for line in f:#遍历一天的所有记录
                line=line.encode('utf-8').replace("陕",'').replace("\"",'').replace("灯",'')
                linelist=line.split(",")
                i+=1
                print(i,line)
                if i==1:
                    continue
                (taxiNo,gps_time,longitude,latitude,car_stat)=(linelist[1],linelist[3],float(linelist[4]),float(linelist[5]),linelist[10])         
                j=0#没获取一条新纪录，初始化所在圆数量
                stat_now=""
                ser_list_now=[]
                if taxi_state_dict[taxiNo][0]=="-1":
                        flag=1
                for line in self.circle_state_list:#遍历圆，找出所在圆，圆格子的记录 ser_num,c_lon,c_lat,on_cou,off_cou,empty_cou,full_cou
                    ser=line[0]
                    c_lon=float(line[1])  
                    c_lat=float(line[2])
                    if self.gps_in_circle(c_lon,c_lat,longitude,latitude):#找到该记录所在圆   
                        j+=1
                        try:
                            if flag==1:#获取当前车的当前状态，如果这是该车第一条记录，更新当前参数值
                                taxi_state_dict[taxiNo][0]=car_stat
                                taxi_state_dict[taxiNo][1].append(ser)
                                continue
                            if j==1:#如果是该记录所在的第一个圆 ，获取记录中车牌所在的上一次信息
                                stat_now=taxi_state_dict[taxiNo][0]#上一次状态
                                ser_list_now=taxi_state_dict[taxiNo][1]#上一次所在格子list
                                taxi_state_dict[taxiNo][1]=[]
                                taxi_state_dict[taxiNo][0]=car_stat#更新状态信息 
                                
                            taxi_state_dict[taxiNo][1].append(ser)
                        except Exception as e:
                            print(e)
                            continue
                        #统计格子的空车和实车的车次数量
                        if car_stat!=stat_now or (ser not in ser_list_now):#如果车的状态发生改变，或者格子发生改变    
                            for ser_i in ser_list_now:#遍历该车牌上一次所在的所有的圆                      
                                if stat_now=="4":
                                    (ser_i,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou)=self.circle_state_list[ser_i-1]
                                    self.circle_state_list[ser_i-1]=(ser_i,lon_now,lat_now,on_cou,off_cou,empty_cou+1,full_cou)
                                if stat_now=="5":
                                    (ser_i,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou)=self.circle_state_list[ser_i-1]
                                    self.circle_state_list[ser_i-1]=(ser_i,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou+1)   
                                #统计格子的上车次数和下车次数
                                if car_stat!=stat_now:
                                    if stat_now=="4" and car_stat=="5":#上车行为
                                        (ser_i,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou)=self.circle_state_list[ser_i-1]
                                        self.circle_state_list[ser_i-1]=(ser_i,lon_now,lat_now,on_cou+1,off_cou,empty_cou,full_cou)
                                    elif stat_now=="5" and car_stat=="4":#下车行为
                                        (ser_i,lon_now,lat_now,on_cou,off_cou,empty_cou,full_cou)=self.circle_state_list[ser_i-1]
                                        self.circle_state_list[ser_i-1]=(ser_i,lon_now,lat_now,on_cou,off_cou+1,empty_cou,full_cou)
                        continue
                flag=0
                if i%1000==0:
                    fw=open('F:\\TaxiData\\circle_on_off\\3statistic_circle_0101_'+str(i)+'_taxi.csv', 'wb')
                    w=csv.writer(fw)
                    print("write result "+'F:\\TaxiData\\circle_on_off\\3statistic_circle_0101_'+str(i)+'_taxi.csv')
                    w.writerows(self.circle_state_list) 
                    fw.close()   
                continue
        fw=open('F:\\TaxiData\\circle_on_off\\3statistic_circle_0101_all_taxi.csv', 'wb')
        w=csv.writer(fw)
        print("write result F:\\TaxiData\\circle_on_off\\3statistic_circle_0101_all_taxi.csv")
        w.writerows(self.circle_state_list)
               
if __name__=="__main__":
    each_lon_len=85300#每单位经度的长度
    each_lat_len=111300#每单位纬度的长度
    filepath="F:\\TaxiData\\gps_log0101.csv"
    line_total=34578916
    seed_num=10000
    seed_file="F:\\TaxiData\\seed.csv"
    radius=1000#半径，单位米
    random_circle_produce=RandomCircleProduce(filepath,line_total,seed_num,seed_file,radius,each_lon_len,each_lat_len)
    random_circle_produce.stat_circle_all_taxi_meanwhile()    
            