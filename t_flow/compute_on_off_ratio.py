# -*- coding: utf-8 -*-  
'''
Created on

@author: zsy

1、生成持续时间的向量
2、计算每条路线的交易数量 
3、统计每个格子的上车率、下车率、上车率-下车率  
'''
import csv
import codecs
from operator import itemgetter, attrgetter  
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
            self.col_num_sum=int(((self.lon_max-self.lon_min)*self.each_lon_len)//self.each_grid_len)
        else:
            self.col_num_sum=int(((self.lon_max-self.lon_min)*self.each_lon_len)//self.each_grid_len)+1#总列数，最后一个格子可能不足 each_grid_len的长度
        if ((self.lat_max-self.lat_min)*self.each_lat_len)%self.each_grid_len==0:
            self.row_num_sum=int(((self.lat_max-self.lat_min)*self.each_lat_len)//self.each_grid_len)
        else:
            self.row_num_sum=int(((self.lat_max-self.lat_min)*self.each_lat_len)//self.each_grid_len)+1#总行数，最后一个格子可能不足 each_grid_len的长度
        self.grid_num_sum=self.row_num_sum*self.col_num_sum
    def getPosition(self,lon_v,lat_v):
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
    
class DealFlow():
    def __init__(self,lon_min,lon_max,lat_min,lat_max,each_lon_len,each_lat_len,each_grid_len):
        self.lon_min=lon_min#最小经度
        self.lon_max=lon_max#最大经度
        self.lat_min=lat_min#最小纬度
        self.lat_max=lat_max#最大纬度
        self.each_lon_len=each_lon_len#每单位经度的长度
        self.each_lat_len=each_lat_len#每单位纬度的长度
        self.each_grid_len=each_grid_len#划分的每个格子的边长
        self.area_grid=AreaGrid(self.lon_min,self.lon_max,self.lat_min,self.lat_max,self.each_lon_len,self.each_lat_len,self.each_grid_len)
        self.sour_des_tracou_dict={}
    def gen_duration_vector_1(self):# 生成持续时间的向量
        '''
        输入：
        单子记录
        输出：
        时长矩阵
        处理方法：
        主要是文本处理一下，然后导入到excel，粘贴出需要的时长列
        '''
        f = open("track100Taxi20141016.txt") 
        fw=open ( 'track100Taxi20141016_dealed.txt', 'a' ) 
        line = f.readline()            
        while line:
            print(line)
            if line.startswith('Over') or line.startswith('start') or line.startswith("end") or line.startswith("last") or line.find("AU8397")>0:
                line = f.readline()
                continue
            fw.write(line)                   
            line = f.readline()
        f.close()  
        fw.close()
    def calc_sour_des_tra_num(self):# 计算每条路线的交易数量
        '''
        输入：
        每一笔单子的记录，包括源经纬度、目的经纬度。出发时间、到达时间、持续时间
        输出：
        每条线路的单子数量，格式：源格子号、目的格子号、单子数量
        处理过程：
        中间变量字典，格式：{(源格子号，目的格子号):单子数量}
        根据输入的经纬度求出相应的格子号，将字典中相应的值加1 
        循环读取下一条记录
        '''
        f = open("track100Taxi20141016_dealed.txt") 
        line=f.readline()
        while line:
            linelist=line.split("\t")
            (lon_sour,lat_sour,lon_des,lat_des)=(float(linelist[1]),float(linelist[2]),float(linelist[3]),float(linelist[4]))
            (row_num_sour,col_num_sour,ser_num_sour)=self.area_grid.getPosition(lon_sour, lat_sour)
            (row_num_des,col_num_des,ser_num_des)=self.area_grid.getPosition(lon_des, lat_des)
            if (row_num_des,col_num_des,ser_num_des)==(0,0,0) or (row_num_sour,col_num_sour,ser_num_sour)==(0,0,0):
                line=f.readline()
                continue
            if self.sour_des_tracou_dict.has_key((ser_num_sour,ser_num_des)):
                self.sour_des_tracou_dict[(ser_num_sour,ser_num_des)]+=1
            else:
                self.sour_des_tracou_dict[(ser_num_sour,ser_num_des)]=1
            line=f.readline()
        f.close()
        
        csvfile = file('track100TaxiTraCouX_1000.csv', 'wb')
        writer = csv.writer(csvfile)
        reslist=[]
        for k in self.sour_des_tracou_dict:
            t_key=k
#             t_value=tuple(str(self.sour_des_tracou_dict[k]))#元组转换两位数的字符串比如“34”，会把它切割成两个元素“3”，“4”
#             t=t_key+t_value
            one_list=list(t_key)
            one_list.append(self.sour_des_tracou_dict[k])
            reslist.append(one_list)
        writer.writerows(reslist)
        csvfile.close()
    def calc_grid_rate(self):#统计每个格子的上车率、下车率、上车率-下车率
        '''
        统计每个格子的上车率、下车率、上车率-下车率，分别输入到三个csv中，按照格子的行号和咧号，输入相应位置，形成矩阵。
        统计方法：     
        读取原始数据的每一行，得到格子的行号、列号、上车次数、下车次数、空车车次、实车车次，
        计算上车率=上车次数/空车次数，下车率=下车次数/实车次数，要根据行、列找到位置，放到三个输出文件中
        定义三个列表存放三种比率，列表作为输出excel文件的一行数据，当列表存满一行时，将数据写入三个excel。
        是否满行的判断，用变量记录当前行号，当取出的行号不等于当前行号时说明变了。则将列表输入到文件，同时清空。
        '''
        onrate_list=[]
        offrate_list=[]
        diffrate_list=[]
#         f = open("statisticGridAll900.txt") #900辆车一天的数据统计
        onrate_csv= file('F:\\TaxiData\\grid_on_off\\on_off_ratio\\statistic_grid_0101_all_on_ratio_m.csv', 'wb')
        offrate_csv= file('F:\\TaxiData\\grid_on_off\\on_off_ratio\\statistic_grid_0101_all_off_ratio_m.csv', 'wb')
        diffrate_csv= file('F:\\TaxiData\\grid_on_off\\on_off_ratio\\statistic_grid_0101_all_diff_ratio_m.csv', 'wb')
        on_writer = csv.writer(onrate_csv)
        off_writer = csv.writer(offrate_csv)
        diff_writer = csv.writer(diffrate_csv)
#         line = f.readline()   
        cur_row_num=1    
        with codecs.open("F:\\TaxiData\\grid_on_off\\statistic_grid_0101_all_taxi.csv",'rb',encoding='gbk') as f : # 使用with读文件     
            for line in f:#遍历一天的所有记录
                print(line)
               
                line=line.strip("(")
                line=line.strip(")\n")
                linelist=line.split(",")
                (rownum,colnum,oncou,offcou,emptycou,fullcou)=(int(linelist[1]),int(linelist[2]),int(linelist[3]),int(linelist[4]),int(linelist[5]),int(linelist[6]))
#                 emptycou+=oncou#将空车车次加上上车的次数
#                 fullcou+=offcou#将实车车次加上下车的次数
#                 emptycou-=offcou
#                 fullcou-=oncou
                if emptycou==0:
                    on_rate=0
                else:
                    on_rate=float(oncou)/float(emptycou)
                if fullcou==0:
                    off_rate=0
                else:     
                    off_rate=float(offcou)/float(fullcou)
                off_on_differ=off_rate-on_rate#越大，越好打车
                if rownum==cur_row_num:#如果是当前行，则加入当前行的list中。
                    onrate_list.append(on_rate)
                    offrate_list.append(off_rate)
                    diffrate_list.append(off_on_differ)    
                else:#换行了，则将写满的行写入文件，清空当前行的list
                    print(onrate_list)
                    print(offrate_list)
                    print(diffrate_list)
                    on_writer.writerow(onrate_list)  
                    off_writer.writerow(offrate_list)
                    diff_writer.writerow(diffrate_list)  
                    onrate_list=[]
                    offrate_list=[]   
                    diffrate_list=[] 
                    cur_row_num=rownum             
#                 line = f.readline()
        print(onrate_list)#往文件输入最后一行
        print(offrate_list)
        print(diffrate_list)
        on_writer.writerow(onrate_list)  
        off_writer.writerow(offrate_list)
        diff_writer.writerow(diffrate_list)
        f.close()  
        onrate_csv.close()
        offrate_csv.close()
        diffrate_csv.close()
        
    def sort_on_off_diff(self):#统计每个格子的上车率-下车率
     
            onrate_csv= file('F:\\TaxiData\\grid_on_off\\on_off_ratio\\statistic_grid_0101_all_diff_sort_ratio_m_1.csv', 'wb')
            on_writer = csv.writer(onrate_csv)
            i=0
            row_id=0
            ratiolist=[]
            with codecs.open("F:\\TaxiData\\grid_on_off\\on_off_ratio\\statistic_grid_0101_all_diff_ratio_m.csv",'rb',encoding='gbk') as f : # 使用with读文件     
                for line in f:#遍历一天的所有记录
                    print(line)
                    row_id+=1
                    line=line.strip("\n").strip('\r')
                    linelist=line.split(",")
                    col_id=0
                    for item in linelist:
                        i+=1
                        col_id+=1
                        item=float(item)
                        if item!=0:
                            (lon_mi,lat_mi,lon_ma,lat_ma)=self.area_grid.getGPS(row_id, col_id, i)
                            ratiolist.append([item,row_id,col_id,i,lon_mi,lat_mi,lon_ma,lat_ma])
                            print(item,row_id,col_id,i,lon_mi,lat_mi,lon_ma,lat_ma)
#                             on_writer.writerow(ratiolist)  
            ratiolist=sorted(ratiolist, key=itemgetter(0),reverse=True)#好打车的在前面 
            on_writer.writerows(ratiolist)  
            f.close()
            onrate_csv.close()
        
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
    dealFlow=DealFlow(lon_min,lon_max,lat_min,lat_max,each_lon_len,each_lat_len,each_grid_len)
#     dealFlow.gen_duration_vector_1()
#     dealFlow.calc_sour_des_tra_num()



#     dealFlow.sort_on_off_diff()
    print(dealFlow.area_grid.getPosition(108.838807315112,34.0954812441984))

