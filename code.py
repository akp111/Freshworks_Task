import threading 
#this is for python 3.0 and above. use import thread for python2.0 versions
from threading import*
import time

data={} 
mutex = threading.Lock()

# create operation 
#use syntax "create(key_name,value,timeout_value)" timeout is optional you can continue by passing two arguments without timeout

def create(key,value,timeout=0):
    mutex.acquire()
    if key in data:
        print("this key already exists") #error message if key already exists
    else:
        if(key.isalpha()):
            if len(data)<(1024*1024*1024) and value<=(16*1024*1024): #constraints for file size less than 1GB and Jasonobject value less than 16KB 
                if timeout==0:
                    l=[value,timeout]
                else:
                    l=[value,time.time()+timeout]
                if len(key)<=32: 
                    data[key]=l
            else:
                print("Memory limit exceeded!! ")#error message if size exceeds
        else:
            print("Invalind key_name!! key_name must contain only alphabets and no special characters or numbers")#error message3
    mutex.release()
#for read operation
#use syntax "read(key_name)"
            
def read(key):
    mutex.acquire()
    if key not in data:
        print("given key does not exist in database. Please enter a valid key") #error message if key doesnt exists
    else:
        d=data[key]
        if d[1]!=0:
            if time.time()<d[1]: #for comparing the present time with expiry time
                stri=str(key)+":"+str(d[0]) #it returns key object
                return stri
            else:
                print("time-to-live of",key,"has expired") 
        else:
            stri=str(key)+":"+str(d[0])
            return stri
    mutex.release()

#for delete operation
#use syntax "delete(key_name)"

def delete(key):
    mutex.acquire()
    if key not in data:
        print("given key does not exist in database. Please enter a valid key") #error message if key doesnt exist
    else:
        d=data[key]
        if d[1]!=0:
            if time.time()<d[1]: #comparing the current time with expiry time
                del data[key]
                print("key is successfully deleted")
            else:
                print("time-to-live of",key,"has expired") #error message if time xceeds
        else:
            del data[key]
            print("key is successfully deleted")
    mutex.release()


#for update operation 
#use syntax "update(key_name,new_value)"

def update(key,value):
    mutex.acquire()
    d=data[key]
    if d[1]!=0:
        if time.time()<d[1]:
            if key not in data:
                print("given key does not exist in database. Please enter a valid key") #error message if key doesnt exists
            else:
                l=[]
                l.append(value)
                l.append(d[1])
                data[key]=l
                print(data[key])
        else:
            print("time-to-live of",key,"has expired") #error message for time has exceeded
    else:
        if key not in data:
            print("given key does not exist in database. Please enter a valid key") #error message if key doesnt exists
        else:
            l=[]
            l.append(value)
            l.append(d[1])
            data[key]=l
    mutex.release()
