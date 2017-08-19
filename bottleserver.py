from bottle import Bottle, run, template, get, post, request, route, static_file
from sqlalchemy import text, create_engine
from pandas import DataFrame, read_excel, read_csv
from pandas.io import sql
import pandas as pd
import numpy as np
import MySQLdb, re, sys, datetime

app = Bottle()
engine = create_engine("mysql+mysqldb://python:1234@localhost/database")

girls = pd.read_csv('girlnames.csv',low_memory=False, header=None, dtype=str)
boys = pd.read_csv('boynames.csv',low_memory=False, header=None, dtype=str)
boys.columns = ['NAME']
girls.columns = ['NAME']

formupdate='''<head>
<title>Search Page.</title>
<style type="text/css">

    body {
    background-color:aliceblue;
    font-family:'Courier New';
    font-size:11pt;
    }

    .footnotes {
    diplay: block;
    background-color:aliceblue;
    width:240px;
    font-family:'Courier New';
    font-size:8pt;
    }

	.stat {
	margin-top:-8pt;
	margin-bottom:-3pt;	
	}

    .searchform {
    margin-top:50px;
    margin-left:100px;
    margin-bottom:20pt;
    width:640px;
    background-color:transparent;
    }

    table {width:95%; border:solid 0px darkgrey; table-layout:fixed;margin-left:3%;margin-top:30pt;}
    table td {border:solid 1px gainsboro; overflow:hidden; white-space:nowrap;background-color:whitesmoke;font-size:8pt;}
    table th {border:solid 0px grey; overflow:hidden; white-space:nowrap;background-color:gainsboro;font-size:9pt;}
    .fixedcell {width:75px;}
    .fluidcell {width:auto; overflow:hidden; white-space:nowrap;}
</style>
</head>
<body>
    <div class="searchform" align="right">
    <form action="/dump" method="post">
        <p>Subscriber Name: <input name="subname" type="text" value="_subname" /></p>
        <p>Search For:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</p>
        <p>Company: <input name="company" type="text" value="_company" /></p>
        <div class="footnotes stat">rows: _numcomps</p></div>

        <p>Zip: <input name="zipcode" type="text" value="_zipcode" /></br>
        <div class="footnotes stat">rows: _numzips</p></div>

        <p>Email: <input name="email" type="text" value="_email" /><br>
        <div class="footnotes stat">rows: _numemail</p></div>

        <div class="footnotes">To search by domain, begin your query with an '@'<br> (ex. '@msn.com')</p></div>
        <p>Name: <input name="name" type="text" value="_name" /><br>
        <div class="footnotes stat">rows: _numnames</p></div>
        <div class="footnotes">To search by last name only, begin your query with a space<br> (ex. ' Roberts') </p></div>

        <p>SIC4: <input name="sic4" type="text" value="_sic4" /></p>
        <div class="footnotes stat">rows: _numsic</p></div>

        <p>Gender: <input name="gender" type="text" value="_gender" /></p>
        <div class="footnotes stat">rows: _numgender</p></div>
        
        <p>Income: <input name="income" type="text" value="_income" /></p>
        <div class="footnotes stat">rows: _numincome</p></div>
        
        <p>Title: <input name="title" type="text" value="_title" /></p>
        <div class="footnotes stat">rows: _numtitle</p></div>
        
        <p>Website: <input name="website" type="text" value="_website" /></p>
        <div class="footnotes stat">rows: _numwebs</p></div>

        <p>Number of rows: <input name="numrows" type="text" value="_numrows" /></p>
        <p><input value="Search" type="submit" /></p>
        <p>Download this search request <input name="download" type="checkbox" value="sresults" /></p>

    </form><div class="footnotes">
'''
print 'getting headers'

# Fetch all the data into memory
query='SELECT * FROM index_dated_prod limit 100000000;'
try:
	print 'Fetching database'
	allrows = pd.read_sql_query(query,engine)
	print str(allrows)
	print 'Loaded database. Titling contact names...'
	allrows['CONTACT_NAME'] = allrows['CONTACT_NAME'].str.title()
	print 'Titling Addresses...'
	allrows['ADDRESS'] = allrows['ADDRESS'].str.title()
	print 'Pivoting female gender info'
	allrows.loc[allrows.CONTACT_NAME.str.replace(r'\ .*','').isin(boys['NAME']),'GENDER'] = 'Male'
	print 'Pivoting male gender info'
	allrows.loc[allrows.CONTACT_NAME.str.replace(r'\ .*','').isin(girls['NAME']),'GENDER'] = 'Female'

	print 'done downloading datbase'
except:
    print 'Exited fetching database'
    sys.exit()

print 'Retrieved ' + str(len(allrows)) + ' rows from database'

#============================================

@app.route('/dump')
def login():
    global formupdate
    localupdate = str(formupdate)
    localupdate = localupdate.replace('_subname','Subscruber name is required')
    localupdate = localupdate.replace('_company','')
    localupdate = localupdate.replace('_zipcode','')
    localupdate = localupdate.replace('_email','')
    localupdate = localupdate.replace('_name','')
    localupdate = localupdate.replace('_numrows','1000')
    localupdate = localupdate.replace('_website','')
    localupdate = localupdate.replace('_sic4','')
    localupdate = localupdate.replace('_gender','')
    localupdate = localupdate.replace('_income','')
    localupdate = localupdate.replace('_title','')
    return localupdate

#============================================

@app.route('/dump', method='POST')
def do_login():
	global formupdate
	localupdate = str(formupdate)

	#filename = re.sub(r'[^0-9a-zA-Z\.\-\_]*','',request.forms.get('filename'))
	subname = re.sub(r'[^0-9a-zA-Z\-\_\&\ ]*','',request.forms.get('subname'))

	utc_datetime = datetime.datetime.utcnow()
	utc_datetime.strftime("%Y-%m-%d-%H%M") #Result: '2011-12-12-0939Z'
	filename = str(subname).replace(' ','') + '_' + str(utc_datetime).replace(':','_') + '.csv'
	company = re.sub(r'[^0-9a-zA-Z\-\_\&]*','',str(request.forms.get('company')))
	email = re.sub(r'[^0-9a-zA-Z\.\@\-]*','',str(request.forms.get('email')))
	name = re.sub(r'[^0-9a-zA-Z\.\-\-\_\&]*','',str(request.forms.get('name')))
	zipcode = re.sub(r'[^0-9]*','',str(request.forms.get('zipcode')))
	sic4 = re.sub(r'[^0-9]*','',str(request.forms.get('sic4')))
	website = re.sub(r'[^0-9a-zA-Z\.\/\-\_\&]*','',str(request.forms.get('website')))
	numrows = int(request.forms.get('numrows'))
	sresults = str(request.forms.get('download'))
	gender = str(request.forms.get('gender'))
	title = str(request.forms.get('title'))
	income = str(request.forms.get('income'))

	localupdate = localupdate.replace('_numcomps',str(allrows['COMPANY_NAME'].str.contains("[a-zA-Z0-9]").sum()))
	localupdate = localupdate.replace('_numemail',str(allrows['EMAIL'].str.contains("[a-zA-Z0-9]").sum()))
	localupdate = localupdate.replace('_numzips',str(allrows['ZIPCODE'].str.contains("[0-9]").sum()))
	localupdate = localupdate.replace('_numnames',str(allrows['CONTACT_NAME'].str.contains("[a-zA-Z0-9]").sum()))
	localupdate = localupdate.replace('_numsic',str(allrows['SIC_CODE'].str.contains("[0-9]").sum()))
	localupdate = localupdate.replace('_numgender',str(allrows['GENDER'].str.contains("[a-zA-Z0-9]").sum()))
	localupdate = localupdate.replace('_numwebs',str(allrows['WEB_ADDRESS'].str.contains('[A-Za-z0-9]').sum()))
	localupdate = localupdate.replace('_numincome',str(allrows['ANNUAL_REVENUE'].str.contains("[0-9]").sum()))
	localupdate = localupdate.replace('_numtitle',str(allrows['TITLE'].str.contains("[a-zA-Z0-9]").sum()))

	#yield '<p>' + str(allrows['GENDER'].str.contains("[a-zA-Z0-9]").sum()) + '<p>'

	#localupdate = localupdate.replace('_filename',filename)
	localupdate = localupdate.replace('_subname',subname)
	localupdate = localupdate.replace('_company',company)
	localupdate = localupdate.replace('_zipcode',zipcode)
	localupdate = localupdate.replace('_email',email)
	localupdate = localupdate.replace('_name',name)
	localupdate = localupdate.replace('_numrows',str(numrows))
	localupdate = localupdate.replace('_website',website)
	localupdate = localupdate.replace('_sic4',sic4)
	localupdate = localupdate.replace('_gender',gender)
	localupdate = localupdate.replace('_income',sic4)
	localupdate = localupdate.replace('_title',title)

	yield localupdate

	query='SELECT * FROM user_' + str(subname).replace(' ','') + ';'

	yield 'Collecting download history for ' + subname + '...<p>'
	try:
	   user_downloaded_rows = pd.read_sql_query(query,engine)

	   #yield 'Done fetching history for ' + subname + '<p>'
	except:
	   yield 'No existing history for ' + subname + '.<p>'
	   user_downloaded_rows = allrows.head(n=0)


	findrows = len(user_downloaded_rows)
	#Now we have a full database copy, and a copy of all rows the user has already received.
	#Create a database without the records user already has
	yield 'Collecting search index for ' + subname  + '...<p>'

	rez = allrows[~(allrows['REC_NUM'].isin(user_downloaded_rows['REC_NUM']))]

	#A handy bit of code: set a column value based on a list of other values
	#rez.loc[rez.CONTACT_NAME.isin(['Jim','Fred','John','Bob']),'GENDER'] = 'Male'

	if title:
		yield 'Applying Title filter...<br>'
		rez = rez[rez['TITLE'].str.contains(title)]
	if income:
		yield 'Applying Income filter...<br>'
		rez = rez[rez['ANNUAL_REVENUE'].str.contains(income)]
	if gender:
		yield 'Applying Gender filter...<br>'
		rez = rez[rez['GENDER'].str.contains(gender)]
	if company:
		yield 'Applying Company filter...<br>'
		rez = rez[rez['COMPANY_NAME'].str.contains(company)]
	if zipcode:
		yield 'Applying Zipcode filter...<br>'
		rez = rez[rez['ZIPCODE'].str.contains(zipcode)]
	if email:
		yield 'Applying Email filter...<br>'
		_email = email.upper()
		rez = rez[rez['EMAIL'].str.contains(_email)]
	if name:
		yield 'Applying Name filter...<br>'
		rez = rez[rez['CONTACT_NAME'].str.contains(name)]
	if sic4:
		yield 'Applying SIC filter...<br>'
		rez = rez[rez['SIC_CODE'].str.contains(sic4)]
	if website:
		yield 'Applying Website filter...<br>'
		rez = rez[rez['WEB_ADDRESS'].str.contains(website)]

	#print str(rez.head(n=5))
	#print str(allrows.head(n=5))
	yield '<p></p>Found ' + str(len(rez)) + ' candidates.'
	try:
		rows = pd.unique(np.random.choice(rez.index.values, numrows + (numrows/10)))
		#rows = pd.unique(np.sample(rez.index.values, numrows))
		yield str(len(rows)) + ' unique records.'
	except:
		rows = []
		yield 'No results for ' + subname + ' in query.<p>'

	report = rez.ix[rows]
	report = report.head(n=numrows)
	if sresults == 'sresults':
		if rows <> []:
			report.to_csv('../' + filename,index=False,quoting=1)
			yield '<p><h3>Getting ' + str(numrows) + ' rows into <a href="http://bigfeeds.net/' + filename + '">  Filename.</a></h3></p>'	
			try:
				results = report['REC_NUM'].to_sql('user_' + str(subname).replace(' ',''), con=engine, if_exists='append', index=True)
			except:
				print 'There was an error saving user download history for ' + subname + ': unable to write to database. Download terminated. Please try your search again or contact support'
	yield '</div></div>'
		
	if rows <> []:
		yield str(report.head(n=10).to_html(index=False,border=0)) 

	return 
	
run(app, host='xxxxxx', port=xxxx)
