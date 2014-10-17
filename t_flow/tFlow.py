# -*- coding: utf-8 -*-  
'''
@author: zsy
'''
import cx_Oracle  
import datetime
import sys
import fileinput  

class AreaGrid():
    def __init__(self,lon_min,lon_max,lat_min,lat_max,each_lon_len,each_lat_len,each_grid_len):
        self.lon_min=lon_min#最小经度
        self.lon_max=lon_max#最大经度
        self.lat_min=lat_min#最小纬度
        self.lat_max=lat_max#最大纬度
        self.each_lon_len=each_lon_len#每单位经度的长度
        self.each_lat_len=each_lat_len#每单位纬度的长度
        self.each_grid_len=each_grid_len#划分的每个格子的边长
        if ((self.lon_max-self.lon_min)*self.each_lon_len)%self.each_grid_len==0:    
            self.col_num_sum=((self.lon_max-self.lon_min)*self.each_lon_len)/self.each_grid_len
        else:
            self.col_num_sum=((self.lon_max-self.lon_min)*self.each_lon_len)/self.each_grid_len+1#总列数，最后一个格子可能不足 each_grid_len的长度
        if ((self.lat_max-self.lat_min)*self.each_lat_len)%self.each_grid_len==0:
            self.row_num_sum=((self.lat_max-self.lat_min)*self.each_lat_len)/self.each_grid_len
        else:
            self.row_num_sum=((self.lat_max-self.lat_min)*self.each_lat_len)/self.each_grid_len+1#总行数，最后一个格子可能不足 each_grid_len的长度
        
    def getPosition(self,lon_v,lat_v):
        if lon_v>self.lon_min and lon_v<self.lon_max and lat_v>self.lat_min and lat_v<self.lat_max:            
            mod_res=(lon_v-self.lon_min)*self.each_lon_len%self.each_grid_len
            if mod_res==0:
                col_num=(lon_v-self.lon_min)*self.each_lon_len/self.each_grid_len
            else:
                col_num=(lon_v-self.lon_min)*self.each_lon_len/self.each_grid_len+1
            mod_res=(lat_v-self.lat_min)*self.each_lat_len%self.each_grid_len
            if mod_res==0:
                row_num=(lat_v-self.lat_min)*self.each_lat_len/self.each_grid_len
            else:
                row_num=(lat_v-self.lat_min)*self.each_lat_len/self.each_grid_len+1
            ser_num=self.col_num_sum*(row_num-1)+col_num
            return (row_num,col_num,ser_num)
        else:
            return
    def getGPS(self,row_num,col_num,ser_num):
        return 
    
class OraConn():
    def __init__(self):
        self.connstr = "manage_taxi/taxixjtu@traffic"  
        self.conn = cx_Oracle.Connection(self.connstr)  
        self.cur = self.conn.cursor()  

    def __del__(self):
        self.conn.commit();  
        self.cur.close();  
        self.conn.close();   
    def countTaxiNo(self):
        sql = "select  count(distinct t.licenseplateno) from GPS_LOG0101 t where t.gps_time>to_date('2014-01-01','yyyy-mm-dd') and t.gps_time<to_date('2014-01-02','yyyy-mm-dd')"            
        self.cur.execute(sql)         
        row=self.cur.fetchone()        
        file_object = open('result20141016.txt', 'w+')        
        res="20140101数据中的不同车牌的车的数量:"+str(row[0])
        print ("20140101数据中的不同车牌的车的数量:"+row[0])
        file_object.write(res)  
        file_object.close()
    def selectTaxiSample(self):
        sql = "select  distinct t.licenseplateno from GPS_LOG0101 t"            
        self.cur.execute(sql)         
        row=self.cur.fetchone() 
        file_object = open('result1All_20141017.txt', 'a')              
        while row:
            TAXINO=row[0]
            row=self.cur.fetchone ()
            print (TAXINO)
            res=TAXINO.replace('?','陕')+"\n"
            file_object.write(res)        
        file_object.close()
    def trackOneTaxiAll(self,taxiNo):##一辆车一天内空车或实车的记录全部记下来
        sql = "select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG0101 t where t.car_stat1='4' or t.car_stat1='5' and t.licenseplateno='陕AU8397' order by t.gps_time"            
        self.cur.execute(sql)         
        row=self.cur.fetchone() 
        file_object = open('trackOneTaxiAll20141016.txt', 'a')              
        while row:
            (gps_time,longitude,latitude,car_stat1)=(row[0],row[1],row[2],row[3])
            row=self.cur.fetchone ()
            print (gps_time,longitude,latitude,car_stat1)
            file_object.write(gps_time+","+longitude+","+latitude+","+car_stat1+'\n')        
        file_object.close()
    def trackOneTaxi(self,taxiNum):
        proBeginTime=datetime.datetime.now()
        pr={'taxiNo':taxiNum}
        sql="select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG0101 t where t.licenseplateno like :taxiNo order by t.gps_time"
        result=self.cur.execute(sql,pr) 
        row=self.cur.fetchone() 
        file_object = open('track100Taxi20141016.txt', 'a')
        file_object.write("\n"+taxiNum+"的轨迹:\n")             
 
#         sourceLon=""
#         sourceLat=""
#         targetLon=""
#         targetLat=""
#         beginTime=""
#         endTime=""
#         lastTime=""
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
                    res=str(i)+","+sourceLon+","+sourceLat+","+targetLon+","+targetLat+","+beginTime.strftime("%Y-%m-%d %H:%M:%S")+","+endTime.strftime("%Y-%m-%d %H:%M:%S")+","+lastTime.__str__()+","+rDistance+","+eDistance+'\n'
                    print (res)
                    file_object.writelines(res) 
            row=self.cur.fetchone ()       
        proEndTime=datetime.datetime.now()
        file_object.write("Over!\n")
        file_object.write("start time:"+proBeginTime.strftime("%Y-%m-%d %H:%M:%S"))
        file_object.write("\nend time:"+proEndTime.strftime("%Y-%m-%d %H:%M:%S"))
        file_object.write("\nlast time:"+(proEndTime-proBeginTime).__str__())
        
        file_object.close()   
    def track100Taxi(self):
        f = open("result20141016.txt") 
        line = f.readline()             # 调用文件的 readline()方法
        while line!="\n":
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n').split(",")[1]).replace('陕','%')
            print (taxiNo)  
            self.trackOneTaxi(taxiNo)                     
            line = f.readline()
        f.close()  
        
    def staticGridOneTaxi(self,taxiNum):
        '''
                        追踪一辆车在一天内的上传、下车行为，每出现一次，在相应区域格子的上车或者下车计数字段加1
                        追踪一辆车在一天内的在每个格子的空车车次，在相应格子的空车车次上加1
                        追踪一辆车在一天内的在每个格子的实车车次，在相应格子的实车车次上加1
        '''
        proBeginTime=datetime.datetime.now()
        pr={'taxiNo':taxiNum}
        sql="select t.gps_time,t.longitude,t.latitude,t.car_stat1 from GPS_LOG0101 t where t.licenseplateno like :taxiNo order by t.gps_time"
        result=self.cur.execute(sql,pr) 
        row=self.cur.fetchone() 
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
               
            row=self.cur.fetchone ()       
        proEndTime=datetime.datetime.now()
 
    def staticGrid100Taxi(self):
        f = open("result20141016.txt") 
        line = f.readline()             # 调用文件的 readline()方法
        while line!="\n":
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n').split(",")[1]).replace('陕','%')
            print (taxiNo)  
            if taxiNo!="%AU8397":
                self.staticGridOneTaxi(taxiNo)                     
            line = f.readline()
        f.close() 
    def staticGridAllTaxi(self):#追踪所有的出租车，一共11728辆，将其上车、下车的次数加入相应的区域格子
        f = open("result1All_20141017.txt") 
        line = f.readline()             # 调用文件的 readline()方法
        while line!="\n":
            #print line,    # 后面跟 ',' 将忽略换行符
            taxiNo=(line.strip('\n')).replace('陕','%')
            print (taxiNo)  
            self.staticGridOneTaxi(taxiNo)                     
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
        f = open("result20141016.txt") 
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
    
    taxiNo="%AU8397"
    oraConn=OraConn()
#     oraConn.selectTaxiSample()
#     oraConn.test()
#     oraConn.countTaxiNo()
    
#     oraConn.trackOneTaxi("%AT7761") 
#     oraConn.track100Taxi() 

#     oraConn.staticGrid(taxiNo)
    oraConn.staticGridAllTaxi()
    