#!/usr/local/bin/python3

import glob, json, os, shutil, sys

store_path  = glob.glob(os.path.join("/pfs/stores", "*.txt"))[0]
purchase_path = glob.glob(os.path.join("/pfs/purchases", "*.txt"))[0]

print("Opening store_file...: " + store_path)
 
with open(store_path, 'r') as store_json:    
    store = json.load(store_json)
    zipcode = store['address']['zipcode']
    print("zipcode: " + zipcode)

    ## Create directory with zipcode name
    os.makedirs("/pfs/out/"+zipcode, exist_ok=True)
    
    with open("/pfs/out/"+zipcode+"/"+ os.path.basename(purchase_path), 'w') as location_file:  
        # Add a text separator to identify in what store the purchase was made
        separator_line = "\nPurchase at store: "+ str(store["storeid"]) +" - "+ store["name"]+" \n"    
        location_file.write(separator_line)
  
        print("Appending : " + purchase_path + " to:" + "/pfs/out/"+zipcode+".txt")
        # Copy the content of the purchase file into the text file
        with  open(purchase_path, 'r') as purchase_file:
            location_file.write(purchase_file.read()) 




