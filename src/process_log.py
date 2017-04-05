# Load libraries

import numpy as np 
import pandas as pd 
import sys


def load_data(data_path):
	'''
	This function takes a data path and parse the data into a pandas data frame.
	'''

	# Read data in and parse into a pandas dataframe.
	# Split the string on each line by the character '['.
	df = pd.read_csv(data_path, sep='[', engine = 'python', header = None)


	# Perform string manipulation to put data into correct columns.
	# Create columns to hold the full logs, host sites, full timestamps, timestamps,
	# requests, HTTP reply codes, and bandwidths.
	df.loc[:, 'log'] = df[0]+'['+ df[1]
	df.loc[:, 'host'] = df.iloc[:, 0].map(lambda x: x[:-5].strip())
	df.loc[:, 'time_full'] = df.iloc[:, 1].map(lambda x: x.split(']')[0].strip())
	df.iloc[:, 1] = df.iloc[:, 1].map(lambda x: x.split(' '))
	df.loc[:, 'time'] = df.iloc[:, 1].map(lambda x: x[0].strip())
	df.loc[:, 'request'] = df.iloc[:, 1].map(lambda x: x[3].strip()[:-1] if x[3].endswith('"') else x[3].strip())
	df.loc[:, 'code'] = df.iloc[:, 1].map(lambda x: x[-2].strip())
	df.loc[:, 'bytes'] = df.iloc[:, 1].map(lambda x: 0 if x[-1] == '-' else int(x[-1].strip()))

	# Drop unnecessary columns.
	df = df.drop([0,1], axis = 1)

	return df

def make_feature_1(df, output_path):
	'''
	This function takes a data frame and engineer the first feature.
	'''

	# Look at the 'host' column. For each unique host/IP address, 
	# count the number of times accessing the site. 
	# Take the top 10 hosts and write to file.
	top_host = pd.DataFrame(df.groupby('host').size().reset_index())
	top_host.columns = ['host', 'count']
	top_host = top_host.sort_values(['count', 'host'], ascending = [False, True])
	top_host = top_host[0:10]
	top_host.to_csv(path_or_buf = output_path, index = False, header = False)


def make_feature_2(df, output_path):
	'''
	This function takes a data frame and engineer the second feature.
	'''

	# Consider two columns 'request' and 'bytes'.
	# Group by the 'request' column and find the total sum of bandwidth used for each request.
	# Sort the result and write to file the top 10 requests that consume the most bandwidth.

	top_bandwidth = df.groupby(['request'], as_index = False)['bytes'].sum()
	top_bandwidth = top_bandwidth.sort_values('bytes', ascending = False).iloc[0:10]['request']
	top_bandwidth.to_csv(path = output_path, index = False)


def make_feature_3(df, output_path):
	'''
	This function takes a data frame and engineer the third feature.
	'''

	#Consider two column 'time_full' and 'time'.
	df = df.loc[:, ['time_full', 'time']]

	# Clean the data by summarizing to get unique timestamps and 
	# the number of times the site is accessed at each timestamp.
	df = pd.DataFrame(df.groupby(['time', 'time_full']).size()).reset_index()
	df.columns = ['time', 'time_full', 'count']
	
	# Find the time lapse between each timestamp and the last timestamp.
	df.loc[:, 'time'] = pd.to_datetime(df.loc[:, 'time'], format = '%d/%b/%Y:%X')
	min_time = df.iloc[0]['time']
	df.loc[:, 'diff'] = df.loc[:, 'time'].map(lambda x: int((x-min_time).total_seconds()))

	# Find the maximum time lapse.
	max_diff = np.max(df.loc[:, 'diff'])

	# Create an array where at index equal time lapse, the value is 
	# the number of times the site is accessed.
	values = df.loc[:, 'count']
	index = df.loc[:, 'diff']
	array = np.array([0 for i in xrange(max_diff + 1)])
	array[index] = values

	# Find the index range that specifies the 60-minute window at each beginning timestamp.
	index_range = np.array([[i, i+3600] if i+3600 < max_diff else [i, max_diff + 1] for i in index])
	
	# Find the total number of times the site is accessed at each timestamps.
	# Append the sum column to the existing data frame.
	cumulative_sum = np.r_[0, array.cumsum()][index_range]
	sums = cumulative_sum[:,1] - cumulative_sum[:,0]
	df.loc[:, 'sum'] = sums

	# Sort to find the top 10 busiest times the site is accessed.
	# Write output to file.
	busiest_time = df.sort_values(['sum', 'time_full'], ascending = [False, True])[0:10][['time_full', 'sum']]
	busiest_time.to_csv(path_or_buf = output_path, index = False, header = False)

def find_blocked_log(df):
	'''
	This function find the blocked logs of one host/IP address.
	It takes a dataframe that holds all logs of one host/IP address.
	'''

	# Reset the index of the data frame to make sure every data frame starts at 0.
	df = df.reset_index()

	# Create variables. 
	# window is the 20-second window to count three consecutive fails.
	# time_at_first_fail and time_at_second_fail keep track of the time we have 
	# two fails in a row within a window.
	# block_time_remains counts the time left before blocking is lifted.
	# previous_time is initialized as the first timestamp.

	window = 20
	time_at_first_fail = time_at_second_fail = -1
	block_time_remains = 0
	previous_time = df['time'].iloc[0]

	log = []
	for i in xrange(len(df)):
		current_time = df.loc[i]['time']
		time_elapsed = (current_time - previous_time).total_seconds()
		block_time_remains = max(0, (block_time_remains - time_elapsed))

		# block is in effect. 
		if block_time_remains > 0:
			log.append(df['log'].iloc[i])

		# block is not in effect, encounter a successful login.
		elif df.loc[i]['code'] == '200':
			time_at_first_fail = -1
			time_at_second_fail = -1
        
        # block is not in effect, encounter neither a successful nor a failed login
		elif df.loc[i]['code'] != '200' and df.loc[i]['code'] != '401':
			pass

		# block is not in effect, encounter the first failed login
		elif time_at_first_fail == -1:
			time_at_first_fail = current_time
	     
	    # block is not in effect.  
		elif time_at_second_fail == -1:
			# encounter the second failed login  
			if (current_time - time_at_first_fail).total_seconds() < window:
				time_at_second_fail = current_time
			# the second failed login is outside of window. reset the count.
			else:
				time_at_first_fail = current_time
				time_at_second_fail = -1

		# block is not in effect. 
		else:
			# if encounter third failed login, block is set.
			if (current_time - time_at_first_fail).total_seconds() < window:
				block_time_remains = 300
				time_at_first_fail = -1
			
			# if third failed login is outside window, reset the count.
			else:
				time_at_first_fail = current_time
				time_at_second_fail = -1

	previous_time = current_time
		    
	return pd.Series(log)


def make_feature_4(df, output_path):
	'''
	This function takes a data frame and engineer the fourth feature.
	It calls the function find_blocked_log for each host/IP address.
	It takes the full data frame and filters only hosts/IP addresses that have
	failed login attempts at least three times.
	'''

	# Find the lists of hosts/IP addresses that have at least three failed logins.
	fail = pd.DataFrame(df.groupby(['host','code']).size())
	fail = fail.reset_index()
	fail.columns = ['host', 'code', 'count']
	fail_hosts = fail.loc[(fail['code']=='401') & (fail['count'] >=3), 'host']

	# Subset from the full data set only hosts/IP addresses found in previous step.
	df = df.iloc[:][df['host'].isin(fail_hosts)]
	df = df.sort_values(['host', 'time'])
	df.loc[:, 'time'] = pd.to_datetime(df.loc[:, 'time'], format = '%d/%b/%Y:%X')

	# For each host/IP address, apply the function find_blocked_log to get the logs we want.
	host_groups = df.groupby('host')
	block = pd.Series()
	for g in host_groups.groups:
		df_temp = host_groups.get_group(g)
		block = block.append(find_blocked_log(df_temp), ignore_index = True)

	# Write the result to file.
	block.to_csv(path = output_path, index = False)



if __name__ == '__main__':

	arguments = sys.argv

	input_directory = arguments[1]
	hosts = arguments[2]
	hours = arguments[3]
	resources = arguments[4]
	blocked = arguments[5]	

	print "Reading data in ..."
	df = load_data(input_directory)
	print "Finish reading data."

	print "Making the first feature..."
	make_feature_1(df, hosts)
	print "Finish making the first feature!"

	print "Making the second feature..."
	make_feature_2(df, resources)
	print "Finish making the second feature!"

	print "Making the third feature..."
	make_feature_3(df, hours)
	print "Finish making the third feature!"

	print "Making the fourth feature..."
	make_feature_4(df, blocked)
	print "Finish making the fourth feature!"
