# -#- coding:utf-8 -*-
# @Time    :2019/8/2 10:52

import pymysql,csv,random,requests,hashlib,math
from snownlp import SnowNLP
import pandas as pd
pd.set_option('display.max_columns', None)

def select_product():
	config={
    "host":"127.0.0.1",
    "user":"root",
    "password":"123456",
    "database":"chinese_websites",
	"use_unicode":True,
	"charset":"utf8"
    }
	db = pymysql.connect(**config)
	cursor = db.cursor()
	sql = "SELECT * from jd_product_test"
	cursor.execute(sql)
	col_name_list = [tuple[0] for tuple in cursor.description]
	data = cursor.fetchall()
	product_id = []
	product_name = []
	product_url = []
	product_price = []
	product_categories = []
	for i in data:
		product_id.append(i[1])
		product_name.append(i[6].split(',')[-1].rstrip('}').split(':')[1])
		product_url.append(i[3])
		product_price.append(i[4])
		product_categories.append(i[6])
		print(i[6],'  ',i[6].split(',')[-1].rstrip('}').split(':')[1])
	cursor.close()
	db.close()
	f = open('./product.csv',"a",newline="",encoding='utf-8')
	fieldnames = ["product_id","product_name","product_url","product_price","product_categories"]
	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()
	length = len(product_id)
	temp = {}
	for i in range(length):
		temp["product_id"] = product_id[i]
		temp["product_name"] = product_name[i]
		temp["product_url"] = product_url[i]
		temp["product_price"]  = product_price[i]
		temp["product_categories"]  = product_categories[i]
		print(temp)
		writer.writerow(temp)
	f.close()

def select_review():
	config={
    "host":"127.0.0.1",
    "user":"root",
    "password":"123456",
    "database":"chinese_websites",
	"use_unicode":True,
	"charset":"utf8"
    }
	db = pymysql.connect(**config)
	cursor = db.cursor()
	sql = "SELECT * from jd_review_test"
	cursor.execute(sql)
	col_name_list = [tuple[0] for tuple in cursor.description]
	review_data = cursor.fetchall()
	sentiments = []
	product_id = []
	review_id = []
	reviewer_nickname = []
	dealed_reviewer_nickname = []
	review_content = []
	review_rating = []
	review_helpful = []
	review_time = []
	for i in review_data:
		if i[3] in reviewer_nickname:
			index = reviewer_nickname.index(i[3])
			if i[1]!=product_id[index]:
				product_id.append(i[1])
				review_id.append(i[2])
				reviewer_nickname.append(i[3])
				if len(i[3])==5:
					dealed_reviewer_nickname.append(i[3].split('*')[0]+i[3].split('*')[-1])
				else:
					dealed_reviewer_nickname.append(i[3].strip())
				content = i[4].replace('\n','').replace(' ','').replace('&nbsp;','')
				s = SnowNLP(content)
				sentiments.append(s.sentiments)
				review_content.append(content)
				review_rating.append(i[5])
				review_helpful.append(i[6])
				print(type(i[7]),i[7])
				review_time.append(i[7])
			else:
				continue
		else:
			product_id.append(i[1])
			review_id.append(i[2])
			reviewer_nickname.append(i[3])
			if len(i[3])==5:
				dealed_reviewer_nickname.append(i[3].split('*')[0]+i[3].split('*')[-1])
			else:
				dealed_reviewer_nickname.append(i[3].strip())
			content = i[4].replace('\n','').replace(' ','').replace('&nbsp;','')
			s = SnowNLP(content)
			sentiments.append(s.sentiments)
			review_content.append(content)
			review_rating.append(i[5])
			review_helpful.append(i[6])
			review_time.append(i[7])
			print(type(i[7]),i[7])
	#处理过用户名后会有不一样的，地方啊，同样一个商品下，一个人可能会重复评论，
	# 可能是水军，或者是当前网络不好重复提交了，这样的，我们也要将其处理
	cursor.close()
	db.close()

	f = open('./review.csv',"a",newline="",encoding='utf-8')
	fieldnames = ["product_id","review_id","reviewer_nickname","dealed_reviewer_nickname","review_content","sentiments","review_rating","review_helpful","review_time"]
	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()
	length = len(product_id)
	temp = {}
	for i in range(length):
		temp["product_id"] = product_id[i]
		temp["review_id"] = review_id[i]
		temp["reviewer_nickname"] = reviewer_nickname[i]
		temp["dealed_reviewer_nickname"]  = dealed_reviewer_nickname[i]
		temp["review_content"]  = review_content[i]
		temp["sentiments"]  = str(sentiments[i])
		temp["review_rating"]  = review_rating[i]
		temp["review_helpful"]  = review_helpful[i]
		temp["review_time"]  = review_time[i]
		print(temp)
		writer.writerow(temp)
	f.close()

def deal_review():
	config={
    "host":"127.0.0.1",
    "user":"root",
    "password":"123456",
    "database":"chinese_websites",
	"use_unicode":True,
	"charset":"utf8"
    }
	db = pymysql.connect(**config)
	cursor = db.cursor()
	sql = "SELECT * FROM jd_review_test WHERE product_id in (SELECT  product_id from jd_product_test where product_categories LIKE '%手机%')"
	cursor.execute(sql)
	review_data = cursor.fetchall()
	sentiments = []
	product_id = []
	review_id = []
	reviewer_nickname = []
	dealed_reviewer_nickname = []
	review_content = []
	review_rating = []
	review_helpful = []
	review_time = []
	print("评论共有",len(review_data),'条！！')
	for i in review_data:
		if i[3] in reviewer_nickname:
			index = reviewer_nickname.index(i[3])
			if i[1]!=product_id[index]:
		# if i[3] not in reviewer_nickname:
			# index = reviewer_nickname.index(i[3])
				product_id.append(i[1])
				review_id.append(i[2])
				reviewer_nickname.append(i[3])
				if len(i[3])==5:
					dealed_reviewer_nickname.append(i[3].split('*')[0]+i[3].split('*')[-1])
				else:
					dealed_reviewer_nickname.append(i[3].strip())
				content = i[4].replace('\n','').replace(' ','').replace('&nbsp;','')
				print(content)
				s = SnowNLP(content)
				sentiments.append(s.sentiments)
				review_content.append(content)
				review_rating.append(i[5])
				review_helpful.append(i[6])
				review_time.append(i[7])
			else:
				continue
		else:
			product_id.append(i[1])
			review_id.append(i[2])
			reviewer_nickname.append(i[3])
			if len(i[3])==5:
				dealed_reviewer_nickname.append(i[3].split('*')[0]+i[3].split('*')[-1])
			else:
				dealed_reviewer_nickname.append(i[3].strip())
			content = i[4].replace('\n','').replace(' ','').replace('&nbsp;','')
			print(content)
			s = SnowNLP(content)
			sentiments.append(s.sentiments)
			review_content.append(content)
			review_rating.append(i[5])
			review_helpful.append(i[6])
			review_time.append(i[7])
	#处理过用户名后会有不一样的，地方啊，同样一个商品下，一个人可能会重复评论，
	# 可能是水军，或者是当前网络不好重复提交了，这样的，我们也要将其处理
	print(len(product_id))
	print(len(sentiments))
	print(len(reviewer_nickname))
	print(len(dealed_reviewer_nickname))
	print(len(set(dealed_reviewer_nickname)))
	cursor.close()
	db.close()

	f = open('./review.csv',"a",newline="",encoding='utf-8')
	fieldnames = ["product_id","review_id","reviewer_nickname","dealed_reviewer_nickname","review_content","sentiments","review_rating","review_helpful","review_time"]
	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()
	length = len(product_id)
	temp = {}
	for i in range(length):

		temp["product_id"] = product_id[i]
		temp["review_id"] = review_id[i]
		temp["reviewer_nickname"] = reviewer_nickname[i]
		temp["dealed_reviewer_nickname"]  = dealed_reviewer_nickname[i]
		temp["review_content"]  = review_content[i]
		temp["sentiments"]  = str(sentiments[i])
		temp["review_rating"]  = review_rating[i]
		temp["review_helpful"]  = review_helpful[i]
		temp["review_time"]  = review_time[i]
		print(temp)
		writer.writerow(temp)
	f.close()


def deal_product_with_pd(): # 处理出 “手机这” 这一列 和 具体品牌这一列
	df = pd.read_csv(r'F:\Postgraduate\Python\Code\PycharmProjects\KnowlodgeGraph\Experiments\data\product.csv')
	category = df['product_categories']
	first_category = []
	second_category = []
	# print(category[:5])
	for item in category:
		first_category.append(item.split(',')[2].split(':')[1].replace("'",'').strip())
		second_category.append(item.split(',')[3].split(':')[1].replace("'",'').strip())
	first_category = pd.DataFrame(data = first_category)
	second_category = pd.DataFrame(data = second_category)
	print(first_category)
	print(second_category)
	df['first_category'] = first_category
	df['second_category'] = second_category
	df.to_csv('./data/product.csv',index=None)
	# print(df.head())

def get_gender_age():
	gender = random.choice(['男','女'])
	age = random.randint(18,60)
	return  gender,age

def deal_review_with_pd():
	df = pd.read_csv(r'./data/review.csv')
	review_time = df['review_time']
	deal_review_time = []
	for i in review_time:
		deal_review_time.append(i.split(' ')[0])
	deal_review_time = pd.DataFrame(data=deal_review_time)
	df['deal_review_time'] = deal_review_time
	df.to_excel('./data/jd/review.xlsx')
	product_virtual_info_of_reviewer()


def get_md5(string):
    """Get md5 according to the string
    """
    byte_string = string.encode("utf-8")
    md5 = hashlib.md5()
    md5.update(byte_string)
    result = md5.hexdigest()
    return result



def product_virtual_info_of_reviewer():
	df = pd.read_excel(r'./data/review.xlsx')
	genders = []
	ages = []
	person_ids = []
	reviewer_nickname = df['reviewer_nickname']
	for i in reviewer_nickname:
		gender,age = get_gender_age()
		person_id = get_md5('{},{},{}'.format(i, gender, age))
		genders.append(gender)
		ages.append(age)
		person_ids.append(person_id)
	sentiments = df['sentiments']
	str_sentiments = []
	for i in sentiments:
		if i == 1:
			temp = '0.9999'
		else:
			temp = str(round(i,4))
			if len(temp)<6:
				temp += (6 - len(temp))*'0'
			if temp.startswith('1'):
				temp = '0.9999'
			if temp == '0.0000':
				temp = '0.0001'
		str_sentiments.append(temp)
	df.insert(loc=4, column='person_id',value=person_ids)
	df.insert(loc=5, column='gender',value=genders)
	df.insert(loc=6, column='age',value=ages)
	df.insert(loc=9, column='str_sentiments',value=str_sentiments)
	df.to_csv('./data/str_review.csv',index=None)



if __name__ == '__main__':

	select_product()
	select_review()
	deal_review()
	deal_product_with_pd()
	deal_review_with_pd()

	product_virtual_info_of_reviewer()

