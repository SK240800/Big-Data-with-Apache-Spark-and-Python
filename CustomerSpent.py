
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    CustomerID = int(fields[0])
    Price = float(fields[2])
    return (CustomerID,Price)


lines = sc.textFile("file:///Users/sk/Desktop/LEARNING/SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)
custP = parsedLines.reduceByKey(lambda x,y: x+y)
scustP=custP.map(lambda x:(x[1],x[0])).sortByKey()
results = scustP.collect();

for r in results:
    print(r[1],"\t{:.2f}$".format(r[0]))