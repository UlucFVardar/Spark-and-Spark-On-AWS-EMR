 ![alt text](https://raw.githubusercontent.com/UlucFVardar/Spark-and-Spark-On-AWS-EMR/master/SS/spark.png)

# Why Spark?
>Apache Spark is a new framework which utilizes in-memory capabilities to deliver fast processing (almost 100 times faster than Hadoop). So, the Spark product is increasingly being used in a world of big data, and mainly for faster processing.

# Main idea of the Repo
The porpuse of the Repo to give information about Spark usage from start to finish with simple to Hard examples
  > -Introduction, Spark Fundementals
  > -Crimes On New York city (make data structured, data cleaning, finding crimes location on Google Map)
  > -Dodgers Summary (Try to give a meaning to data changing, draw charts)
  > -Spark On Amazon Web Services (Using AWS S3 bucket and AWS EMR tools Data analysis )

# How Does Spark Works?
 >Spark is simply a frameWorkTool that distributes your data and processes it in parallel, shortening the data processing time and allowing you to work with this much larger data (BIG DATA).
 > A visual about distributed Data and Processes in Spark:
 ![alt text](https://raw.githubusercontent.com/UlucFVardar/Spark-and-Spark-On-AWS-EMR/master/SS/blog19.png)



 
### Topic Research and Source
If you want to learn more about these topics you can go for it:
* [Spark Beginning](https://app.pluralsight.com/library/courses/apache-spark-beginning-data-exploration-analysis/table-of-contents) - You can find expended info in this *Pluralsight*  video.
* [Spark Fundemantals](https://app.pluralsight.com/library/courses/apache-spark-fundamentals/table-of-contents
) -You can find expended info in this *Pluralsight*  video.
* [AWS Big Data Tools OverView](https://app.pluralsight.com/library/courses/big-data-amazon-web-services/table-of-contents
)-  OverView and Create Clusters information in *Pluralsight*  video.
* [AWS Documantation]((https://aws.amazon.com/tr/emr/details/spark/))-  OverView and Create Clusters information in *Pluralsight*  video.

After you view, lets continue

### Installation

>Firstly you need python2.7 and python3 
>For production environments...
[Install python versions to your computer](https://www.python.org/downloads/)

| Required Libraries  | installation |
| ------ | ------ |
| matplotlib |  pip/pip3 install matplotlib |
| datetime |  pip/pip3 install datetime |

>You also need Spark and Jupyter notebook
>For production environments...
[Install Spark  to your computer](https://spark.apache.org/downloads.html)
[Install Jupyter to your computer](http://jupyter.org/install.html)
> Don't forget set the System Variables, Paths.
> You can also install Anaconda.


# Usage
>Call of Spark on Jupyter from terminal
```sh
$  python pyspark
```
> First View of the Jupyter Notebook :
![alt text](https://raw.githubusercontent.com/UlucFVardar/Spark-and-Spark-On-AWS-EMR/master/SS/Juypter.png)

 > After open a new Python file and first code View:
 ![alt text](https://raw.githubusercontent.com/UlucFVardar/Spark-and-Spark-On-AWS-EMR/master/SS/first%20View.png)
 



***
# Crimes On New York City 
```python
import csv
from StringIO import StringIO
from collections import namedtuple

#loading data
filePath="NYPD_7_Major_Felony_Incidents.csv"
data = sc.textFile(filePath)

```

```python
def parse(row):
    reader = csv.reader(StringIO(row))
    row = reader.next()
    return Crime(*row)
```

```python
def extracCoords(location):
    #string maniplation  [1:location.index(",")] or [location.index(",")+1:-1]
    lat = float(location[1:location.index(',')]) 
    lon = float(location[location.index(',')+1:-1])
    return (float(lat),float(lon))
```



```python
#cleaning data from non-header rows

#header loading
header = data.first()
print header
dataWoHeader = data.filter(lambda x : x<>header)

fields = header.replace(" ","_").replace("/","_").split(",")
# we give name every column
Crime = namedtuple('Crime',fields, verbose=True ) 

crimes = dataWoHeader.map(parse)
# A lookUp
crimes.first()

>   An output Example 
    Crime(OBJECTID='1', Identifier='f070032d', Occurrence_Date='09/06/1940 07:30:00 PM', Day_of_Week='Friday', Occurrence_Month='Sep', Occurrence_Day='6', Occurrence_Year='1940', Occurrence_Hour='19', CompStat_Month='9', CompStat_Day='7', CompStat_Year='2010', Offense='BURGLARY', Offense_Classification='FELONY', Sector='D', Precinct='66', Borough='BROOKLYN', Jurisdiction='N.Y. POLICE DEPT', XCoordinate='987478', YCoordinate='166141', Location_1='(40.6227027620001, -73.9883732929999)')


##we can exemine that if there are any emty slots or not 
crimes.map(lambda x : x.Offense).countByValue()

>    defaultdict(int,
                {'BURGLARY': 191369,
                 'FELONY ASSAULT': 184042,
                 'GRAND LARCENY': 428993,
                 'GRAND LARCENY OF MOTOR VEHICLE': 101963,
                 'MURDER & NON-NEGL. MANSLAUGHTE': 4574,
                 'NA': 1,
                 'RAPE': 13779,
                 'ROBBERY': 198744})

#filter returns true or false if false that row dies.
#we can exemine that if there are any emty slots or not 
#and when we look at all years we understant that there are missing data before 2006..

crimesFiltered = crimes.filter(lambda x : not (x.Offense == "NA" or x.Occurrence_Year==''))\
        .filter(lambda x : int(x.Occurrence_Year)>=2006)
```


```python
# Data set location max and min finding 
    # if there is any anomaly in the cordinates According to New York city we have to clean DataSet from dirty data
crimesFiltered.map(lambda x: (extracCoords(x.Location_1))).reduce(lambda x,y :(min(x[0],y[0]),min(x[1],y[1])))

>    (40.112709974, -77.519206334)


# Data set location max and min finding 
    # if there is any anomaly in the cordinates According to New York city we have to clean DataSet from dirty data
crimesFiltered.map(lambda x :(extracCoords(x.Location_1))).reduce(lambda x,y :(max(x[0],y[0]),max(x[1],y[1])))

>   (59.5805088160001, -73.700716685)


#clean location anomalyies these crimes are occurse in new york
crimesFinal = crimesFiltered.filter(lambda x: extracCoords(x.Location_1)[0]>=40.477399 and\
                                   extracCoords(x.Location_1)[0]<=40.917577 and\
                                   extracCoords(x.Location_1)[1]>=-74.25909 and\
                                   extracCoords(x.Location_1)[1]<=-73.700009 )
```


```
#try to give a meaning

#trend By year without any filter
crimesFinal.map(lambda x : x.Occurrence_Year).countByValue()

#Trend by crime type in 2015
crimesFinal.filter(lambda x : x.Occurrence_Year=='2015')\
            .map(lambda x : x.Offense)\
            .countByValue()

>    defaultdict(int,
                {'BURGLARY': 14967,
                 'FELONY ASSAULT': 20189,
                 'GRAND LARCENY': 41873,
                 'GRAND LARCENY OF MOTOR VEHICLE': 7250,
                 'MURDER & NON-NEGL. MANSLAUGHTE': 336,
                 'RAPE': 1156,
                 'ROBBERY': 16886})
```

```python
# gmplot lib. is allows us plot dos on googlemap 
import gmplot
gmap = gmplot.GoogleMapPlotter(37.428,-122.145,16).from_geocode("New York City")

#in 2015 all BURGLARY crimes on the map give lat.
b_lats = crimesFinal.filter(lambda x : x.Offense == 'GRAND LARCENY OF MOTOR VEHICLE' and x.Occurrence_Year=="2015")\
                     .map(lambda x: extracCoords(x.Location_1)[0])\
                     .collect()

#in 2015 all BURGLARY crimes on the map give lon.
b_lons = crimesFinal.filter(lambda x : x.Offense == 'GRAND LARCENY OF MOTOR VEHICLE' and x.Occurrence_Year=="2015")\
                     .map(lambda x: extracCoords(x.Location_1)[1])\
                     .collect()

gmap.scatter(b_lats,b_lons,'#DE1515',size=40,marker=False)

#save myMap.html
gmap.draw("mymap.html")
```

 > After open a new Python file and first code View:
 ![alt text](https://raw.githubusercontent.com/UlucFVardar/Spark-and-Spark-On-AWS-EMR/master/SS/Crime%20Map.png)

***
# Dodgers Summary


```python
# needed lib import
from datetime import datetime
import csv
from StringIO import StringIO
```


```python
#file paths
trafficPath = "Dodgers.data"
gamesPath = "DodgersEvents"

#creating rdd
traffic = sc.textFile(trafficPath)
games = sc.textFile(gamesPath)

# games.take(10)
traffic.take(1)

>    [u'4/10/2005 0:00,-1']
```

```
# to maniplate the parse Traffic data
def parseTraffic(row):
    DATE_FMT = "%m/%d/%Y %H:%M"
    row = row.split(',')
    row[0] = datetime.strptime(row[0],DATE_FMT)
    row[1] = int(row[1])
    return (row[0],row[1])

#Creating a pair Rdd
trafficParsed = traffic.map(parseTraffic)
trafficParsed.take(1)

>    [(datetime.datetime(2005, 4, 10, 0, 0), -1)]
```

```python
# every day how many cars around to the stat.
dailyTrend = trafficParsed.map(lambda x:(x[0].date(),x[1]))\
            .reduceByKey(lambda x,y : x+y)
dailyTrend.take(2)

>    [(datetime.date(2005, 8, 9), 5958), (datetime.date(2005, 6, 29), 5437)]
```

```python
dailyTrend.sortBy( lambda x : -x[1]).take(5)

>    [(datetime.date(2005, 7, 28), 7661),
     (datetime.date(2005, 7, 29), 7499),
     (datetime.date(2005, 8, 12), 7287),
     (datetime.date(2005, 7, 27), 7238),
     (datetime.date(2005, 9, 23), 7175),
``` 

```python
#join with Games
def parseGames(row):
    DATE_FMT = "%m/%d/%y"
    row = row.split(",")
    row[0] = datetime.strptime(row[0],DATE_FMT).date()
    return (row[0],row[4])

gamesParsed = games.map(parseGames)
gamesParsed.take(1)

>    [(datetime.date(2005, 4, 12), u'San Francisco')]


dailyTrendCombined = dailyTrend.leftOuterJoin(gamesParsed)
dailyTrendCombined.take(1)

>    [(datetime.date(2005, 9, 24), (5848, u'Pittsburgh'))]
```

```python
# according to date and also team trend without noGameDay
dailyTrendCombined.filter(lambda x : x[1][1]!=None ).sortBy( lambda x : x[1][0]).take(5)

>    [(datetime.date(2005, 6, 28), (-96, u'San Diego')),
     (datetime.date(2005, 9, 10), (2851, u'San Diego')),
     (datetime.date(2005, 6, 27), (2907, u'San Diego')),
     (datetime.date(2005, 5, 30), (3973, u'Chicago Cubs')),
     (datetime.date(2005, 9, 5), (4023, u'San Francisco'))]
```

```python
# date, day type, Opponent, How many cars are around to the stat.
def checkGameDay(row):
    if row[1][1] ==None:
        return (row[0],"Regular Day",row[1][1],row[1][0])
    else :
        return (row[0],"--Game day-",row[1][1],row[1][0])
    
dailyTrendByGames = dailyTrendCombined.map(checkGameDay)

dailyTrendByGames.sortBy( lambda x : -x[3]).take(5)


>    [(datetime.date(2005, 7, 28), '--Game day-', u'Cincinnati', 7661),
     (datetime.date(2005, 7, 29), '--Game day-', u'St. Louis', 7499),
     (datetime.date(2005, 8, 12), '--Game day-', u'NY Mets', 7287),
     (datetime.date(2005, 7, 27), '--Game day-', u'Cincinnati', 7238),
     (datetime.date(2005, 9, 23), '--Game day-', u'Pittsburgh', 7175)]
```

```python
#average car on game day and regular day
TrendRateOfOpponent = dailyTrendByGames.map(  lambda x : (x[2],x[3])).combineByKey( (lambda x : (x,1)),
                                           (  lambda acc,y : (acc[0]+y ,acc[1]+1 )),
                                           (  lambda acc1,acc2 : (acc1[0]+acc2[0] , acc1[1]+acc2[1])))\
                                       .mapValues( lambda x: (x[0]/x[1]))
```


```python
# Graph of the Opponent Trend 
import matplotlib.pyplot as plt

y=list(TrendRateOfOpponent.map(lambda x: int (x[1])).collect())
x=list(TrendRateOfOpponent.map(lambda x: str (x[0]) ).collect())
plt.title("Car Trend According to Opponent of The Game")
plt.plot(x, y)
plt.xticks(x, x, rotation='vertical')
plt.show()
```

![DodgersGraph](https://raw.githubusercontent.com/UlucFVardar/Spark-and-Spark-On-AWS-EMR/master/SS/DodgersGraph.png)
***
# Spark On Amazon Web Services

this topic will loaded in a certain time.



***

