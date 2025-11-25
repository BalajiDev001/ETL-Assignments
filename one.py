import requests,json,csv,mysql.connector,pymongo
from pymongo import MongoClient

req_data=requests.get('https://dummyjson.com/carts')
cart_data=req_data.json()['carts']


json_data=[]
csv_data=[]

for cart in cart_data:
    json_data.append({"id":cart['id'],
                 "products":cart['products'],
                 "total":cart['total'],
                 "discountedTotal":cart['discountedTotal'],
                 "totalproducts":cart['totalProducts'],
                 "totalbalajiiquantity":cart['totalQuantity']
                })


fp=open('new.json','w')
data=json.dump(json_data,fp)
print('new json file created')



for cart in cart_data:
    csv_data.append((cart['id'],
                     cart['products'],
                     cart['total'],
                     cart['discountedTotal'],
                     cart['totalProducts'],
                     cart['totalQuantity']
                    )) 

fp1=open('data.csv','w')
data1=csv.writer(fp1)
data1.writerow(['id','products','total','discountedTotal','totalProducts','totalQuantity'])
data1.writerows(csv_data)
print('new csv file created')

try:
    dbcon=None
    cursor=None

    dbcon=mysql.connector.connect(host='localhost',
                              user='root',
                              password='root',
                              database='china'
                              )
    print(dbcon.is_connected)
    cursor=dbcon.cursor()

    str_data='''
              CREATE TABLE cart_details ( 
              id INT AUTO_INCREMENT PRIMARY KEY, 
              cart_id INT, 
              product_id INT, 
              title VARCHAR(255), 
              price INT, 
              quantity INT, 
              total INT, 
              discountPercentage FLOAT, 
              discountedPrice INT 
              );
             '''

    cursor.execute(str_data)       

    str_data='''
             insert into cart_details values(%s,%s,%s,%s,%s,%s,%s,%s,%s)
             '''
    cursor.executemany(str_data,csv_data)
    dbcon.commit()
    print('new sql table is created') 

except:
    pass
finally:
    fp.close()
    fp1.close()
    cursor.close()
    dbcon.close()        




