import glob, os, sys

model_path  = glob.glob(os.path.join("/pfs/model", "*.txt"))
    
with open("/pfs/out/count.txt", 'w') as count_file:  

    line = "\n Number of files in this datum: "    
    count_file.write(line + str(len(model_path)))
    print(line + str(len(model_path)))