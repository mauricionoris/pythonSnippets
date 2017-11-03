
import urllib2
import json
import time
import pandas
import numpy

def agg_df(df,byArg,aggArg,col_names):
    df = df.groupby(by=byArg).agg(aggArg)
    df.columns = df.columns.droplevel(level=0)
    df = df.reset_index()
    df.columns = col_names
    return df;

def filter_dict(df, dic):
    #filter generic function
    filterSeries = pandas.Series(numpy.ones(df.shape[0],dtype=bool))
    for column, value in dic.items():
        filterSeries = ((df[column].isin(value)) & filterSeries)
    return filterSeries;

def import_file(filePath, filter, cols, dates_to_parse):
    #get the information from csv file
    df = pandas.read_csv(filePath,parse_dates=dates_to_parse);
    df = df[filter_dict(df,filter)];
    df.filter(items=cols);
    return df.filter(items=cols);

#Getting data from csv files
stories = import_file('data/stories.csv', {'category_one': ['horror']},['id','title'], []);
reading = import_file('data/reading.csv', {'story_id': stories['id'].values },['tracking_time','visitor_id', 'id', 'visit_id'],['tracking_time']);
visits  = import_file('data/visits.csv' , {'visitor_id': reading['visit_id'].values },['visitor_id', 'country'], []);

#join reading and visits
df = pandas.concat([reading.set_index('visit_id'), visits.set_index('visitor_id')], axis=1, join='inner');

#grouping by date
df['reading_date'] = df['tracking_time'].dt.date;
df.set_index('reading_date');

reading_by_reader_by_day = agg_df(df
                                 ,['visitor_id','country','reading_date']
                                 ,{'tracking_time': {'seconds_of_reading': lambda x: (max(x) - min(x)).total_seconds()
                                                            ,'num_events':'count'}
                                                    }
                                 ,['visitor_id','country','reading_date','num_events','seconds_of_reading']);

reading_by_reader_total = agg_df(reading_by_reader_by_day
                                 ,['country','visitor_id']
                                 ,{'seconds_of_reading': 'sum', 'num_events':'sum','reading_date':['min','max']}
                                 ,['country','visitor_id','reading_first_time','reading_last_time','num_events','total_reading_by_reader']);

reading_by_country = agg_df(reading_by_reader_total
                                 ,['country']
                                 ,{'total_reading_by_reader': 'sum','num_events':  ['sum','count']}
                                 ,['country','number_of_events','number_of_readers','reading_time_total_by_country']);

print '*****************HOW MUCH EACH READER READ BY DAY***************************************************************'
print reading_by_reader_by_day
print '****************************************************************************************************************'

print '*****************HOW MUCH DID THEY READ AT ALL (BY READER)******************************************************'
print reading_by_reader_total
print '****************************************************************************************************************'

print '*****************HOW MANY READERS ARE THERE (BY COUNTRY)********************************************************'
print reading_by_country
print '****************************************************************************************************************'
