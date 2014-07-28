import pandas as pd
import numpy as np
import sys, getopt

def get_means_medians(df, user_id):

	temp_indices = np.where(df['user_id'] == user_id)
	temp_indices = list(temp_indices[0])

	temp_dates = list(df['created_on'].iloc[temp_indices])

	temp_dates = sorted(temp_dates)

	diffs = []

	for j in xrange(len(temp_dates)-1):
		temp_diff = temp_dates[j+1]-temp_dates[j]
		diffs.append(temp_diff)
	
	diffs = [x.days for x in diffs]

	if np.mean(diffs) != 0:
		std_temp = float(np.std(diffs))/float(np.mean(diffs))
	else:
		std_temp = -1
	
	mean_temp = np.mean(diffs)
	median_temp = np.median(diffs)

	return user_id, mean_temp, median_temp, std_temp

def main(inputfile, outputfile):

	df = pd.read_csv(inputfile)

	df['created_on'] = pd.to_datetime(df['created_on'])

	print "Getting max and min creation dates..."

	max_created_on = df[['user_id','created_on']].groupby('user_id').max().reset_index()[['user_id','created_on']]
	# alternatively, df.groupby('user_id')['created_at'].max().reset_index()
	
	max_created_on.columns = ['user_id','last_use_date']
	# alternatively, max_created_on.rename(columns = {'created_on':'last_use_date'}) if you only
	# have to rename on or a couple of columns

	min_created_on = df[['user_id','created_on']].groupby('user_id').min().reset_index()[['user_id','created_on']]
	min_created_on.columns = ['user_id','first_use_date']

	use_counts = df[['user_id','created_on']].groupby('user_id').count().reset_index()[['user_id','created_on']]
	use_counts.columns = ['user_id','use_count']

	# last few lines in one go:
	# df.groupby('user_id')['created_on'].agg({'first_use_date':max, 
	#						'last_use_date':mon,
	#						'count':count}).reset_index()
	
	ns_in_day = float(8.64*10**13)

	all_stds = []
	all_means = []
	all_medians = []
	all_user_ids = []

	unique_user_ids = list(set(df['user_id']))

	print "Getting medians and means..."

	for i, user_id in enumerate(unique_user_ids):
		if i%10000 ==0:
			print i
		user_id, mean_temp, median_temp, std_temp = get_means_medians(df, user_id)

		all_stds.append(std_temp)
		all_means.append(mean_temp)
		all_medians.append(median_temp)
		all_user_ids.append(user_id)

	d_ = {'user_id': all_user_ids, 'mean_freq': all_means, 'median_freq': all_medians, 'std_freq': all_stds}

	output_df = pd.DataFrame(data=d_)

	output_df = pd.merge(output_df, min_created_on, on = 'user_id')

	output_df = pd.merge(output_df, max_created_on, on = 'user_id')

	output_df = pd.merge(output_df, use_counts, on = 'user_id')

	output_df.to_csv(outputfile)
	
	''' whole script in a few lines:
	def helper(x):
		diffs = x.diff
		day_diffs.apply(lambda x: x.astype('timedelta64[D]')/np.timedelta64(1, 'D'))
		return day_diffs.mean(), day_diff.median(), day_diff.std
		
	df = pd.read_csv(inputfile, parse_dates = [INSERT DATE COLUMN INDEX HERE])
	grouped = df.groupby('user_id')
	min_max_date = grouped['created_on'].agg({'first_use':min, 'last_use':max})
	stats = grouped['created_on'].apply(lambda x: helper(x))
	result = pd.concat(stats, min_max_date).reset_index()
	result.to_csv()
	
	PANDAS IS AWESOME
	'''
	
	

if __name__ == '__main__':
	main(inputfile, outputfile)
