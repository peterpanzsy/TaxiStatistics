# -*- coding: utf-8 -*-  
'''
Created on 2016年4月23日

@author: zsy
'''
import codecs
import csv
class landmark():
    def stat_grid_all_taxi_meanwhile(self):#同时追踪所有车在每个格子上的行为次数，基于内存和文件操作        
        '''
                        追踪所有车在一天内的上传、下车行为，每出现一次，在相应区域格子的上车或者下车计数字段加1
                        追踪所有车在一天内的在每个格子的空车车次，在相应格子的空车车次上加1
                        追踪所有车在一天内的在每个格子的实车车次，在相应格子的实车车次上加1
                        经度、纬度、所属格子编号、行号、列号、上车或下车行为
        '''
        landmark_list=[]
        taxi_state_dict={}
        f = open("taxi\\result1All_20141017.txt") 
        line = f.readline()  
        i=0          
        while line!="\n" and line:
            i+=1
            taxiNo=(line.strip('\n')).replace('陕','').replace("灯",'').replace("交",'').replace("测",'')
            print (taxiNo) 
            taxi_state_dict[taxiNo]=("0")   #车的当前状态 ,车当前所在的格子序号            
            line = f.readline()
        f.close()  
        
        with codecs.open("F:\\TaxiData\\gps_log0101.csv",'rb',encoding='gbk') as f :
            i=0;
            for line in f:#遍历一天的所有记录
                line=line.encode('utf-8').replace("陕",'').replace("\"",'').replace("灯",'').replace("交",'').replace("测",'')
                linelist=line.split(",")
                i+=1
                print(i,line)
                if i==1:
                    continue
                (taxiNo,gps_time,longitude,latitude,car_stat)=(linelist[1],linelist[3],float(linelist[4]),float(linelist[5]),linelist[10])  
                try:
                    if taxi_state_dict[taxiNo][0]=="0":#获取当前车的当前格子，如果这是该车第一条记录，更新当前参数值
                        taxi_state_dict[taxiNo]=(car_stat)
                        continue
                    stat_now=taxi_state_dict[taxiNo][0]#上一次状态
                    taxi_state_dict[taxiNo]=(car_stat)
                except Exception as e:
                    print(e)
                    continue
                if car_stat!=stat_now:
                    if stat_now == "4" and car_stat=="5":#上车
                        landmark_list.append((longitude,latitude,"up"))
                    elif stat_now=="5" and car_stat=="4":#下车行为
                        landmark_list.append((longitude,latitude,"down"))
                    
                if i%100000==0:
                    fw=open('F:\\TaxiData\\landmarks\\landmark_0101_'+str(i)+'_records.csv', 'wb')
                    w=csv.writer(fw)
                    print("write result "+'F:\\TaxiData\\landmarks\\landmark_0101_'+str(i)+'_records.csv')
                    w.writerows(landmark_list) 
                    fw.close()   
                continue   
        f=open('F:\\TaxiData\\landmarks\\landmark_0101_all.csv', 'wb')
        w=csv.writer(f)
        print("write result F:\\TaxiData\\landmarks\\landmark_0101_all.csv")
        w.writerows(landmark_list)
        f.close()
        
        
       

if __name__ == '__main__':
    landmark=landmark()
    landmark.stat_grid_all_taxi_meanwhile()