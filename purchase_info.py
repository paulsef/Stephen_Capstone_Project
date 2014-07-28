import pandas as pd
import numpy as np
import sys, getopt
from scipy import stats

''' in general, you want to avoid using for loops when dealing with data in python
they're super slow and it will become super obvious when you have enough data and 
try out different implementations

additionally, this seems to have a similar API to the frequency script. IE group by user,
extract a bunch of information about the purchase/event behavior, save result. Consider
rolling them into one script so that you have like with like
'''

def main(inputfile, outputfile):

	df = pd.read_csv(inputfile)

	df = df.dropna()

	keep = np.where((df.transaction_type == 'purchase') & (df.num_items >= 0) & (df.total_order_value > 0))[0]

	df = df.iloc[keep]
	
	# you can skip the np.where()
	# if you pass a mask (boolean series) to the .ix method, only the values that are True will be returned
	# so, df = df.ix[(df.transaction_type == 'purchase') & (df.num_items >= 0) & (df.total_order_value > 0),:]

	users = list(set(df['user_id']))
	# also, np.unique('user_id').tolist(), but keep in mind that you probably want to use the 
	# groupby and apply methods that I mentioned elswhere

	df['transaction_date'] = pd.to_datetime(df['transaction_date'])

	first_purchase_amounts = []

	for user in users:
		tmin = min(df[df['user_id']==user]['transaction_date'])
		first_purchase_amounts.append(df[(df['user_id']==user) & (df['transaction_date']==tmin)]['total_order_value'].iloc[0])

	max_created_on = df[['user_id','transaction_date']].groupby('user_id').max().reset_index()[['user_id','transaction_date']]
	max_created_on.columns = ['user_id','last_purch_date']

	min_created_on = df[['user_id','transaction_date']].groupby('user_id').min().reset_index()[['user_id','transaction_date']]
	min_created_on.columns = ['user_id','first_purch_date']

	temp_list = map(list, zip(*[users,first_purchase_amounts]))

	first_purchase_amount_df = pd.DataFrame(temp_list,columns=['user_id','first_purchase_amount'])

	most_used_store = df[['user_id', 'store_id']].groupby('user_id').agg(lambda x: stats.mode(x['store_id'])[0]).reset_index()[['user_id', 'store_id']]
	most_used_store.columns = ['user_id', 'most_used_store']

	df_purchase_sum = df[['user_id','num_items','total_order_value','commission_value']].groupby('user_id').sum().reset_index()[['user_id','num_items','total_order_value','commission_value']]
	df_purchase_sum.columns = ['user_id','num_items_purch','total_order_value','commission_value']

	final_df = pd.merge(df_purchase_sum, first_purchase_amount_df, on='user_id')
	final_df = pd.merge(final_df,max_created_on, on='user_id')
	final_df = pd.merge(final_df,min_created_on, on='user_id')
	final_df = pd.merge(final_df, most_used_store, on='user_id')

	final_df.to_csv(outputfile)

if __name__ == '__main__':
	main(inputfile, outputfile)
