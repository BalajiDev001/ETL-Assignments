import requests,pymongo,csv,json,mysql.connector
from pymongo import MongoClient
resp_data=requests.get("https://dummyjson.com/carts")
cart_data=resp_data.json()['carts']

json_data=[]
csv_data=[]
for cart in cart_data:
    json_data.append({
                    "id":cart["id"],
                    "products":cart['products'],
                    "total_products":cart['totalProducts'],
                    "total":cart["total"],
                    "discountedTotal":cart["discountedTotal"],
                    "total_quantity":cart["totalQuantity"]
    })
for cart in cart_data:
    csv_data.append(
                    (cart["id"],
                    json.dumps(cart['products']),
                    cart['totalProducts'],
                    cart["total"],
                    cart["discountedTotal"],
                    cart["totalQuantity"])
                    
    )
#load into new json file:
fp=open("new.json",'w')
json.dump(json_data,fp)
print("new json file created")
#load into new csv file:
fp1=open("new.csv",'w',newline="")
cw=csv.writer(fp1)
cw.writerow(["id","products","totalProducts","total","discountedTotal","totalQuantity"])
cw.writerows(csv_data)
print("new csv file created")
#my sql load:
try:
    cursor=None;
    dbcon=None;

    dbcon=mysql.connector.connect(host="localhost",username='root',password='root',database='china')
    print(dbcon.is_connected())
    cursor=dbcon.cursor()
    sql_st='''
     CREATE TABLE IF NOT EXISTS carts(
            id INT PRIMARY KEY,
            products TEXT NOT NULL,
            totalProducts INT NOT NULL,
            total DOUBLE,
            discountedTotal DOUBLE,
            totalQuantity INT
        );
    '''
    cursor.execute(sql_st)
    sql_st=''' insert into carts values (%s,%s,%s,%s,%s,%s)'''
    cursor.executemany(sql_st,csv_data)
    dbcon.commit()
    print("my SQL table created")
except mysql.connector.Error as err:
        print(err)

#load into mongodb:
try:
    client=None;
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    db=client['china']
    cart_data=db['carts']
    cart_data.insert_many(json_data)
    print("loaded into mongodb")
except:
    pass
finally:
    fp.close()
    fp1.close()
    cursor.close()
    dbcon.close()
    client.close()