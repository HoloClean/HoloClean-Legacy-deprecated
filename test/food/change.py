file1 = open("food_input_holo.csv","w") 
with open("food_input1.csv") as f:
    count = 0.00
    index = 1
    list1 = []
    dict1 = {}
    one = []
    two = []

    file1.write("index"+","+"akaname"","+"inspectionid"+","+"city"+","+"state"+","+"results"+","+"longitude"+","+"latitude"+","+"inspectiondate"+","+"risk"+","+"location"+","+"license"+","+"facilitytype"+","+"address"+","+"inspectiontype"+","+"dbaname"+","+"zip"+"\n")
    list_onomata = ["index","akaname","inspectionid","city","state","results","longitude","latitude","inspectiondate","risk","location","license","facilitytype","address","inspectiontype","dbaname","zip"]
    for line in f:
        line1 = line.split(",")
        count1 = line1[0]
        if float(count) == float(count1):
            if line1[1] in list_onomata:
	        newstr = line1[2].replace('"', "")
	        dict1[line1[1]] =  newstr
            list1.append([line1[1],line1[2]])
        else:
              list1 = []
              if count != 0: 
                    if(int(count)<85601):
                           print count
                           file1.write(str(count) +","+ dict1["akaname"]+","+dict1["inspectionid"]+","+dict1["city"]+","+dict1["state"]+","+dict1["results"]+","+dict1["longitude"]+","+dict1["latitude"]+","+dict1["inspectiondate"]+","+dict1["risk"]+","+dict1["location"]+","+dict1["license"]+","+dict1["facilitytype"]+","+dict1["address"]+","+dict1["inspectiontype"]+","+dict1["dbaname"]+","+dict1["zip"]+"\n")
              for onomata in list_onomata:
                  dict1[onomata]=""
              count = count1
              if line1[1] in list_onomata:
                 newstr = line1[2].replace('"', "")
	         dict1[line1[1]] =  newstr
              list1.append([line1[1],line1[2]]) 

file1.close()
      
        
