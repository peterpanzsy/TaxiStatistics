# -*- coding: utf-8 -*-  
'''
@author: zsy
'''
import cx_Oracle  
import datetime
import sys
import fileinput  
from pyasn1.type.univ import Null

class OraConn():
    def __init__(self,connstr):
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
    def getPosition(self,lon_v,lat_v):
        '''
        根据经纬度，返回该地点在第几个格子
        '''
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
        return 
    
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
        
#         self.conn = cx_Oracle.Connection(self.connstr)  
#         self.cur = self.conn.cursor()  
     
    def countTaxiNo(self):
        sql = "select  count(distinct t.licenseplateno) from GPS_LOG0101 t where t.gps_time>to_date('2014-01-01','yyyy-mm-dd') and t.gps_time<to_date('2014-01-02','yyyy-mm-dd')"            
        conn = cx_Oracle.Connection(self.connstr)  
        cur = conn.cursor() 
        cur.execute(sql)         
        row=cur.fetchone()        
        file_object = open('taxi\\result20141016.txt', 'w+')        
        res="20140101数据中的不同车牌的车的数量:"+str(row[0])
        print ("20140101数据中的不同车牌的车的数量:"+row[0])
        file_object.write(res)  
        file_object.close()
        conn.close()
    def selectTaxiSample(self):
        sql = "select  distinct t.licenseplateno from GPS_LOG0101 t"  
        conn = cx_Oracle.Connection(self.connstr)  
        cur = conn.cursor()           
        cur.execute(sql)         
        row=cur.fetchone() 
        file_object = open('result1All_20141017.txt', 'a')              
        while row:
            TAXINO=row[0]
            row=cur.fetchone ()
            print (TAXINO)
            res=TAXINO.replace('?','陕')+"\n"
            file_object.write(res)        
        file_object.close()
        conn.close()
    def trackOneTaxiAll(self,taxiNo):##一辆车一天内空车或实车的记录全部记下来
        sql = "select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG0101 t where t.car_stat1='4' or t.car_stat1='5' and t.licenseplateno='陕AU8397' order by t.gps_time"            
        conn = cx_Oracle.Connection(self.connstr)  
        cur = conn.cursor() 
        cur.execute(sql)         
        row=cur.fetchone() 
        file_object = open('trackOneTaxiAll20141016.txt', 'a')              
        while row:
            (gps_time,longitude,latitude,car_stat1)=(row[0],row[1],row[2],row[3])
            row=cur.fetchone ()
            print (gps_time,longitude,latitude,car_stat1)
            file_object.write(gps_time+","+longitude+","+latitude+","+car_stat1+'\n')        
        file_object.close()
        conn.close()
    def trackOneTaxi(self,taxiNum):
        proBeginTime=datetime.datetime.now()
        print(proBeginTime)
        pr={'taxiNo':taxiNum}
        #查询出该车的所有记录（一天）
        conn = cx_Oracle.Connection(self.connstr)  
        cur = conn.cursor() 
        sql="select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG0101 t where t.licenseplateno like :taxiNo order by t.gps_time"
        result=cur.execute(sql,pr) 
        row=cur.fetchone() 
#         file_object = open('track100Taxi20141016.txt', 'a')
        file_object = open('track\\trackAllTaxi20141021.txt', 'a')
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
                    res=taxiNum+","+str(i)+","+sourceLon+","+sourceLat+","+targetLon+","+targetLat+","+beginTime.strftime("%Y-%m-%d %H:%M:%S")+","+endTime.strftime("%Y-%m-%d %H:%M:%S")+","+lastTime.__str__()+","+rDistance+","+eDistance+'\n'
                    print (res)
                    file_object.writelines(res) 
            row=cur.fetchone ()  
        conn.close()     
        proEndTime=datetime.datetime.now()
        print(proEndTime)
#         file_object.write("Over!\n")
#         file_object.write("start time:"+proBeginTime.strftime("%Y-%m-%d %H:%M:%S"))
#         file_object.write("\nend time:"+proEndTime.strftime("%Y-%m-%d %H:%M:%S"))
#         file_object.write("\nlast time:"+(proEndTime-proBeginTime).__str__())
        
        file_object.close()   
    def track100Taxi(self):
#         f = open("taxi\\result20141016.txt") 
        f=open("result1All_20141017.txt")
        line = f.readline()             # 调用文件的 readline()方法
        while line!="\n" and line:
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n').split(",")[1]).replace('陕','%')
            print (taxiNo)  
            self.trackOneTaxi(taxiNo)                     
            line = f.readline()
        f.close()  
        
    def staticGridOneTaxiUsingOra(self,taxiNum):#追踪一辆车在一个格子上的行为次数，基于数据库操作
        '''
                        追踪一辆车在一天内的上传、下车行为，每出现一次，在相应区域格子的上车或者下车计数字段加1
                        追踪一辆车在一天内的在每个格子的空车车次，在相应格子的空车车次上加1
                        追踪一辆车在一天内的在每个格子的实车车次，在相应格子的实车车次上加1
        '''
        proBeginTime=datetime.datetime.now()
        pr={'taxiNo':taxiNum}
        sql="select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG0101 t where t.licenseplateno like :taxiNo order by t.gps_time"
        conn = cx_Oracle.Connection(self.connstr)  
        cur = conn.cursor() 
        result=cur.execute(sql,pr) 
        row=cur.fetchone() 
        statusNow="4" #记录车的状态变化
        (emp_cou_Now,full_cou_Now,squNow)=(0,0,0)#初始化为不存在的格子
        while row:
            (gps_time,longitude,latitude,car_stat1)=(row[0],float(row[1]),float(row[2]),row[3])
            #找出当前车所在的格子信息
            conn1 = cx_Oracle.Connection(self.connstr)  
            cur1 = conn1.cursor()  
            sql1="select t.emp_cou,t.full_cou,t.squ from T_SQUARE_3_CIRCLE t where t.lat_min<:latitude1 and t.lat_max>:latitude2 and t.lon_min<:longitude1 and t.lon_max>:longitude2"
            pr1={'latitude1':latitude,'latitude2':latitude,'longitude1':longitude,'longitude2':longitude}
            cur1.execute(sql1,pr1) 
            row=cur1.fetchone() 
            if row!=None:#如果在设定范围内的格子里
                (emp_cou,full_cou,squ)=(row[0],row[1],row[2])#当前格子
                if squ!=squNow or car_stat1!=statusNow:#首先判断车有没有换格子，如果换了就将上一个格子的状态车次加1（空车次数或者实车次数）；再判断车的状态有没有发生变化，变化了也将上一个格子的该状态车次数加1
                    if statusNow=="4":
                        emp_cou_Now+=1
                        sqltemp="update T_SQUARE_3_CIRCLE set emp_cou=:cou where squ=:squ"
                        pr1={'cou':emp_cou_Now,'squ':squNow}
                        result=cur1.execute(sqltemp,pr1) 
                    if statusNow=="5":
                        full_cou_Now+=1
                        sqltemp="update T_SQUARE_3_CIRCLE set full_cou=:cou where squ=:squ"
                        pr1={'cou':full_cou_Now,'squ':squNow}
                        result=cur1.execute(sqltemp,pr1)
            (emp_cou_Now,full_cou_Now,squNow)=(emp_cou,full_cou,squ)         
            conn1.commit()   
            cur1.close()
            conn1.close() 
            #统计车的上车和下车动作发生的格子，在相应格子里加1 
            if car_stat1!=statusNow:
                if statusNow=="4" and car_stat1=="5":#上车，出发                   
                    conn2 = cx_Oracle.Connection(self.connstr)  
                    cur2 = conn2.cursor()  
                    sql2="select t.in_cou,t.squ from T_SQUARE_3_CIRCLE t where t.lat_min<:latitude1 and t.lat_max>:latitude2 and t.lon_min<:longitude1 and t.lon_max>:longitude2"
                    pr2={'latitude1':latitude,'latitude2':latitude,'longitude1':longitude,'longitude2':longitude}
                    result=cur2.execute(sql2,pr2) 
                    row=cur2.fetchone() 
                    if row!=None:
                        cou=row[0]+1
                        squ=row[1]
                        sql3="update T_SQUARE_3_CIRCLE set in_cou=:cou where squ=:squ"
                        pr2={'cou':cou,'squ':squ}
                        result=cur2.execute(sql3,pr2) 
                    conn2.commit();  
                    cur2.close();  
                    conn2.close();   
                    statusNow="5"
                elif statusNow=="5" and car_stat1=="4":#下车，到达目的地
                    conn2 = cx_Oracle.Connection(self.connstr)  
                    cur2 = conn2.cursor()  
                    sql2="select t.out_cou,t.squ from T_SQUARE_3_CIRCLE t where t.lat_min<:latitude1 and t.lat_max>:latitude2 and t.lon_min<:longitude1 and t.lon_max>:longitude2"
                    pr2={'latitude1':latitude,'latitude2':latitude,'longitude1':longitude,'longitude2':longitude}
                    result=cur2.execute(sql2,pr2) 
                    row=cur2.fetchone() 
                    if row!=None:
                        cou=row[0]+1
                        squ=row[1]
                        sql3="update T_SQUARE_3_CIRCLE set out_cou=:cou where squ=:squ"
                        pr2={'cou':cou,'squ':squ}
                        result=cur2.execute(sql3,pr2) 
                    conn2.commit();  
                    cur2.close();  
                    conn2.close();  
                    statusNow="4"
               
            row=cur.fetchone ()   
        conn.close()    
        proEndTime=datetime.datetime.now()
 
    def staticGrid100Taxi(self):#追踪100辆车，将其行为次数加入相应格子，基于数据库
        f = open("taxi\\result20141016.txt") 
        line = f.readline()             # 调用文件的 readline()方法
        while line!="\n":
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n').split(",")[1]).replace('陕','%')
            print (taxiNo)  
            if taxiNo!="%AU8397":
                self.staticGridOneTaxiUsingOra(taxiNo)                     
            line = f.readline()
        f.close() 
    def staticGridAllTaxi(self):#追踪所有的出租车，一共11728辆，将其上车、下车的次数加入相应的区域格子，基于数据库
        f = open("result1All_20141017.txt") 
        line = f.readline()             # 调用文件的 readline()方法
        while line!="\n":
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n')).replace('陕','%')
            print (taxiNo)  
            self.staticGridOneTaxiUsingOra(taxiNo)                     
            line = f.readline()
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
            fl.write(str(i)+"辆\n")
            for i in self.grid_stat_list:
                fl.write(i.__str__())
                fl.write("\n")
            fl.close()
    def static_grid_all_taxi(self):#追踪所有的出租车，一共11728辆，将其上车、下车的次数加入相应的区域格子，基于内存和文件
        f = open("result1All_20141017.txt") 
        line = f.readline()  
        i=0           # 调用文件的 readline()方法
        while line!="\n" and line:
            i+=1
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n')).replace('陕','%')
            print (taxiNo)  
            self.stat_grid_1_taxi(taxiNo,i)                     
            line = f.readline()
        f.close()  
        
    def test(self):
#         num={'num':'%AU8397'}
#         sql = "select  t.licenseplateno from GPS_LOG0101 t where  t.licenseplateno like :num"            
#         self.cur.execute(sql,num)         
#         row=self.cur.fetchone()        
#         print  (row[0])
           
#         file_object = open('test.txt', 'w+')        
#         tupletest=("1","2","")
#         file_object.write("a")  
#         file_object.write((datetime.datetime.now()-row[0]).__str__())
#         file_object.close()
        taxiList=[]
        f = open("taxi\\result20141016.txt") 
        line = f.readline()             # 调用文件的 readline()方法
        while line!="\n":
            #print line,    # 后面跟 ',' 将忽略换行符
            stri=(line.strip('\n').split(",")[1]).replace('陕','')
            print (stri)
            taxiList.append(stri)             
            # print(line, end = '')　　　# 在 Python 3中使用
            line = f.readline()
        f.close()
        i=0
        for line in fileinput.input(r"track100Taxi20141016.txt", inplace=1):  
            if line.find('AU8397') > 0:
                print (line.replace('AU8397', taxiList[i],))  
                i+=1
            else: 
                print (line,)
if __name__ == '__main__':
    
#     taxiNo="%AU8397"
#     oraConn=OraConn()
#     oraConn.selectTaxiSample()
#     oraConn.test()
#     oraConn.countTaxiNo()
    
#     oraConn.trackOneTaxi("%AT7761") 
#     oraConn.track100Taxi() 

#     oraConn.staticGrid(taxiNo)
#     oraConn.staticGridAllTaxi()
    lon_min=108.852775#最小经度
    lon_max=109.056295#最大经度
    lat_min=34.200207#最小纬度
    lat_max=34.349599#最大纬度
    each_lon_len=85300#每单位经度的长度
    each_lat_len=111300#每单位纬度的长度
    each_grid_len=50#划分的每个格子的边长
    connstr="manage_taxi/taxixjtu@traffic"     
    taxiStatistics=TaxiStatistics(connstr,lon_min,lon_max,lat_min,lat_max,each_lon_len,each_lat_len,each_grid_len)
    taxiStatistics.static_grid_all_taxi()
    