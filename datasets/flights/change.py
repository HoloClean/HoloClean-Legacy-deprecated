file1 = open("testfile.csv","w") 
with open("flights_input.csv") as f:
    count = 0.00
    index = 1
    list1 = []
    dict1 = {}
    one = []
    two = []
    file1.write("index"+","+"flight"","+"sched_dep_time"+","+"act_dep_time"+","+"sched_arr_time"+","+"act_arr_time"+"\n")
    list_onomata = ["sched_dep_time","act_dep_time","sched_arr_time","flight","act_arr_time"]
    for line in f:
        line1 = line.split(",")
        count1 = line1[0]
        if float(count) == float(count1):
            if line1[1] in list_onomata:
	        dict1[line1[1]] = line1[2]
            list1.append([line1[1],line1[2]])
        else:
              list1 = []
              if count != 0:
                           file1.write(str(count) +","+dict1["flight"]+","+dict1["sched_dep_time"]+","+dict1["act_dep_time"]+","+dict1["sched_arr_time"]+","+dict1["act_arr_time"]+"\n")
              dict1={}
              count = count1
              if line1[1] in list_onomata:
	        dict1[line1[1]] = line1[2]
              list1.append([line1[1],line1[2]]) 

        '''    
              list1 = []
              if count != 0:
                  flights = dict1["flight"]
                  if not (flights in one):
                      one.append(dict1["flight"])
                      file1.write(str(count) +","+dict1["flight"]+","+dict1["sched_dep_time"]+","+dict1["act_dep_time"]+","+dict1["sched_arr_time"]+","+dict1["act_arr_time"]+"\n")
                      index = index +1
                  else:
                       if (float(one.index(flights)) % 20 > 0):
                           file1.write(str(count) +","+dict1["flight"]+","+dict1["sched_dep_time"]+","+dict1["act_dep_time"]+","+dict1["sched_arr_time"]+","+dict1["act_arr_time"]+"\n")
                           index = index +1
                       #else:
                       #    two.append(flights)
                       #    print two
              dict1={}
              count = count1
              if line1[1] in list_onomata:
	        dict1[line1[1]] = line1[2]
              list1.append([line1[1],line1[2]])        '''   

file1.close()
      
        
