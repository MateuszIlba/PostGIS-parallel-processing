import psycopg2
import multiprocessing as mp
import math
import os
import time

#### defined variables
layer1='roads_lines' #### first layers in database to query
layer2='buildings_points' #### second layers in database to query
spatialfunction='st_touches' ####name of any spatial function to do with two layers, e.g.: "ST_Equals", "ST_Disjoint", "ST_Touches", "ST_Within", "ST_Overlaps", "ST_Crosses", "ST_Intersects", "ST_Contains", "ST_Covers", "ST_CoveredBy".

#### function for estimate row count and limit per thread
def row_count():
        global limit, rowcount
        try:
                ss = time.clock()
                conn = psycopg2.connect(host="localhost", database="spatial_optimization", user="postgres", password="kartografia")
                cur = conn.cursor()
                sqlstatement="SELECT count(*) FROM "+"%s"%layer1 + " " + "layer1 JOIN "+"%s"%layer2+" "+"layer2 ON layer1.geom && layer2.geom" #### only spatialindex query
                cur.execute(sqlstatement)
                rowcount__ = cur.fetchone()
                rowcount = rowcount__[0]
                #print ('row count of pairs: '+'%s'%rowcount)
                proces= mp.cpu_count()
                proces=2
                limit = int((rowcount/proces) + 1) #### limit - depending on the number of cores - distribution of calculations into cores
                ee = time.clock()
                kk=ee-ss
                print ('time of rowcount: '+ '%s'%kk)
                return limit
                cur.close()
                conn.close()  
        except:
                print("error")

####  function for generating an offset list
def gen_list_offset():
        global listaoffset #### offset list
        offset = 0
        listaoffset=[]
        while offset<rowcount: #### rowcount from def row_count()
                listaoffset.append(offset)
                offset=offset+limit #### limit from def row_count()
        return listaoffset

#### multi-core SQL query execution function (example)
def main_query(a):
        try:
                sqlstatement = 'SELECT count(*) FROM ( SELECT layerone.pk_uid firstlayer, layerone.geom firstgeom, layertwo.pk_uid secondlayer, layertwo.geom secondgeom FROM '+ '%s'%layer1 + ' layerone JOIN ' + '%s'%layer2 + ' layertwo ON layerone.geom && layertwo.geom LIMIT ' + '%s'%limit + ' OFFSET ' + '%s'%a + ' ) AS wynik '+ 'WHERE ' + '%s'%spatialfunction + ' (firstgeom, secondgeom) = true'
                conn = psycopg2.connect(host="localhost", database="spatial_optimization", user="postgres", password="kartografia")
                cur = conn.cursor()
                cur.execute(sqlstatement)
                data = cur.fetchall()
                return data, a  ####results
                cur.close()
                conn.close()
        except:
                print ("error")

#### sqlstatement may be different, the calculation of the number of rows of the resulting table is given as an example

#### main function for parallel processing
def main():
        pool = mp.Pool(processes= mp.cpu_count())#### start cpu_count worker processes
        s = time.clock()#### only for estimate time of execution
        results = [ pool.map(main_query, (listaoffset)) ] #### start in workers function main_query, with different variables (listaoffset)
        e = time.clock()#### only for estimate time of execution
        #print(results) #### print the parallel SQL query result
        k=e-s #### only for estimate time of execution
        print('multicore processing time: ' + '%s'%k) #### only for estimate time of execution

####main algorithm
row_count()
gen_list_offset()
#print ('limit pairs for thread: '+'%s'%limit)
print (listaoffset)
if __name__ == '__main__':
        main()
