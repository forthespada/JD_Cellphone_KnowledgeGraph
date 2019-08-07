import os
import csv
import hashlib

def get_md5(string):
	"""Get md5 according to the string
	"""
	byte_string = string.encode("utf-8")
	md5 = hashlib.md5()
	md5.update(byte_string)
	result = md5.hexdigest()
	return result


def build_person(person_prep, person_import):
	"""Create an 'person' file in csv format that can be imported into Neo4j.
	format -> person_id:,name,gender,age:int,:LABEL
	label -> Person
	"""
	print('Writing to {} file...'.format(person_import.split('/')[-1]))
	with open(person_prep, 'r', encoding='utf-8') as file_prep, \
		open(person_import, 'w', encoding='utf-8',newline='') as file_import:
		file_prep_csv = csv.reader(file_prep, delimiter=',')
		file_import_csv = csv.writer(file_import, delimiter=',')

		headers = ['person_id:ID', 'name', 'gender', 'age:int', ':LABEL']
		file_import_csv.writerow(headers)
		for i, row in enumerate(file_prep_csv):
			if i == 0 or len(row)<14:
				continue
			info = [row[4], row[2], row[5], row[6]]
			# generate md5 according to 'name' 'gender' and 'age'
			info.append('Person')
			print(info)
			file_import_csv.writerow(info)
	print('- done.')

def build_review(review_prep, review_import):
	"""Create an 'concept' file in csv format that can be imported into Neo4j.
	format -> 'review_id:review_id', 'product_id', 'person_id','content','sentiment','review_time',':LABEL'
	label -> Review
	"""
	print('Writing to {} file...'.format(review_import.split('/')[-1]))
	with open(review_prep, 'r', encoding='utf-8') as file_prep, \
		open(review_import, 'w', encoding='utf-8',newline='') as file_import:
		file_prep_csv = csv.reader(file_prep, delimiter=',')
		file_import_csv = csv.writer(file_import, delimiter=',')

		# headers = ['review_id:review_id', 'product_id', 'person_id','content','sentiment','review_time',':LABEL']
		headers = ['review_id:ID', 'content','sentiment','review_time',':LABEL']
		file_import_csv.writerow(headers)
		for i, row in enumerate(file_prep_csv):
			if i == 0 or len(row)<14:
				continue
			info = [row[1], row[7],row[9], row[13]]
			# generate md5 according to 'name' 'gender' and 'age'
			info.append('Review')
			print(info)
			file_import_csv.writerow(info)
	print('- done.')

def build_person_review(person_prep, relation_import):
	"""Create an 'person_stock' file in csv format that can be imported into Neo4j.
	format -> :START_ID,:END_ID,:TYPE
			   person_id         review_id
	type -> review_of
	"""
	print('Writing to {} file...'.format(relation_import.split('/')[-1]))
	with open(person_prep, 'r', encoding='utf-8') as file_prep, \
		open(relation_import, 'w', encoding='utf-8',newline='') as file_import:
		file_prep_csv = csv.reader(file_prep, delimiter=',')
		file_import_csv = csv.writer(file_import, delimiter=',')
		headers = [':START_ID',  ':END_ID', ':TYPE']
		file_import_csv.writerow(headers)

		for i, row in enumerate(file_prep_csv):
			if i == 0 or len(row)<14:
				continue
			# generate md5 according to 'name' 'gender' and 'age'
			relation = [row[4], row[1], 'review_of']
			print(relation)
			file_import_csv.writerow(relation)
	print(" - done")

def bulid_first_category(product_prep, first_category_import):
	"""Create an 'first_category' file in csv format that can be imported into Neo4j.
	format -> :cateogry_id:category_id,name,:TYPE
	type -> first_category
	"""
	print('Writing to {} file...'.format(first_category_import.split('/')[-1]))
	with open(product_prep, 'r', encoding='utf-8') as file_prep, \
		open(first_category_import ,'w', encoding='utf-8', newline='') as file_import:
		file_prep_csv = csv.reader(file_prep, delimiter=',')
		file_import_csv = csv.writer(file_import, delimiter=',')

		headers = ['first_cateogry_id:ID','name',':LABEL']
		file_import_csv.writerow(headers)
		for i, row in enumerate(file_prep_csv):
			if i==0 or len(row) <7:
				continue
			cateogry_id = get_md5(row[-2])
			# print(cateogry_id,' ',name)
			info = [cateogry_id, row[-2], '手机']
			print(info)
			file_import_csv.writerow(info)
	print("-done")

def bulid_second_category(product_prep, second_category_import):
	"""Create an 'first_category' file in csv format that can be imported into Neo4j.
	format -> :cateogry_id:category_id,name,:TYPE
	type -> first_category
	"""
	print('Writing to {} file...'.format(second_category_import.split('/')[-1]))
	with open(product_prep, 'r', encoding='utf-8') as file_prep, \
		open(second_category_import ,'w', encoding='utf-8', newline='') as file_import:
		file_prep_csv = csv.reader(file_prep, delimiter=',')
		file_import_csv = csv.writer(file_import, delimiter=',')

		headers = ['second_cateogry_id:ID','name',':LABEL']
		file_import_csv.writerow(headers)
		for i, row in enumerate(file_prep_csv):
			if i==0 or len(row) <7:
				continue
			second_category_id = get_md5(row[-1])
			# print(cateogry_id,' ',name)
			info = [second_category_id, row[-1], '手机品牌']
			print(info)
			file_import_csv.writerow(info)
	print("-done")

def build_first_second(first_pre, second_pre, first_second_import):
	'''
	Create an 'first_second' file in csv format taht can be imported into neo4j
	Format -> START_ID,END_ID,TYPE
	   first_cateogry_id, second_category_id , subcategory_of
	Lbale -> subcategory
	'''
	with open(first_pre, 'r', encoding='utf8') as first_file_pre, \
		open(second_pre, 'r', encoding='utf8') as second_file_pre, \
		open(first_second_import, 'w', encoding='utf8', newline='') as first_second_import:
		first_prep_csv = csv.reader(first_file_pre, delimiter=',')
		second_prep_csv = csv.reader(second_file_pre, delimiter=',')
		file_import_csv = csv.writer(first_second_import, delimiter=',')
		headers = [':START_ID', ':END_ID', ':TYPE']
		file_import_csv.writerow(headers)

		for i, first in enumerate(first_prep_csv):
			if i ==1:
				start_id = first[0]
				break
		total_info = []
		for j, second in enumerate(second_prep_csv):
			if j ==0:
				continue
			end_id = second[0]
			info = start_id +','+ end_id +',' + 'subcategory_of'
			if info in total_info:
				continue
			else:
				total_info.append(info)

		for info in set(total_info):
			info = info.split(',')
			file_import_csv.writerow(info)
		print('-done')


def build_second_product(product_pre, second_product_import):
	'''
	Create an 'second_product' file in csv format taht can be imported into neo4j
	Format -> START_ID,END_ID,TYPE
	   second_category_id , product_id, product_of
	Lbale -> product_of
	'''
	with open(product_pre, 'r', encoding='utf8') as product_file_pre, \
		open(second_product_import, 'w', encoding='utf8', newline='') as second_product_file_import:
		product_prep_csv = csv.reader(product_file_pre, delimiter=',')
		file_import_csv = csv.writer(second_product_file_import, delimiter=',')
		headers = [':START_ID', ':END_ID', ':TYPE']
		file_import_csv.writerow(headers)

		for i, row in enumerate(product_prep_csv):
			if i ==0 or len(row)<7:
				continue
			start_id = get_md5(row[-1])
			info = [start_id, row[0], 'product_of']
			print(info)
			file_import_csv.writerow(info)
		print('-done')


def build_product(product_prep, product_import):
	"""Create an 'product' file in csv format that can be imported into Neo4j.
	format -> product_id:product_id,name,price:LABEL
	label -> Company,ST
	"""
	with open(product_prep, 'r', encoding='utf8') as product_file_pre, \
	open(product_import, 'w', encoding='utf8', newline='') as product_file_import:
		product_prep_csv = csv.reader(product_file_pre, delimiter=',')
		file_import_csv = csv.writer(product_file_import, delimiter=',')
		headers = ['product_id:ID','name','price',':LABEL']
		file_import_csv.writerow(headers)

		for i, row in enumerate(product_prep_csv):
			if i ==0 or len(row)<7:
				continue
			info = [row[0], row[1][2:-1], row[3].split('.')[0]+'￥', 'product']
			print(info)
			file_import_csv.writerow(info)
	print('-done')


def build_review_product(review_prep, relation_import):
	"""Create an 'review_product' file in csv format that can be imported into Neo4j.
	format -> :START_ID,:END_ID,:TYPE
			   review_id   product_id
	type -> industry_of
	"""
	with open(review_prep, 'r', encoding='utf-8') as file_prep, \
		open(relation_import, 'w', encoding='utf-8', newline='') as file_import:
		file_prep_csv = csv.reader(file_prep, delimiter=',')
		file_import_csv = csv.writer(file_import, delimiter=',')
		headers = [':START_ID', ':END_ID', ':TYPE']
		file_import_csv.writerow(headers)

		for i, row in enumerate(file_prep_csv):
			if i == 0 or len(row)<14:
				continue
			relation = [row[1], row[0], 'review_on']
			file_import_csv.writerow(relation)
	print('-done')



if __name__ == '__main__':
	import_path = 'data/import'
	if not os.path.exists(import_path):
		os.makedirs(import_path)
	build_person('data/str_review.csv', 'data/import/person.csv')
	build_review('data/str_review.csv','data/import/review.csv')
	build_person_review('data/str_review.csv', 'data/import/person_review.csv')
	# bulid_first_category('data/product.csv', 'data/import/first.csv')
	bulid_second_category('data/product.csv', 'data/import/second.csv')
	build_first_second('data/import/first.csv', 'data/import/second.csv', 'data/import/first_second.csv')

	# build_second_product( 'data/product.csv', 'data/import/second.csv')
	build_product('data/product.csv', 'data/import/product.csv')

	build_review_product('data/str_review.csv', 'data/import/review_product.csv')

