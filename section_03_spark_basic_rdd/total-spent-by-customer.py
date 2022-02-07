from pyspark import SparkConf, SparkContext

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

def main():
    conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
    sc = SparkContext(conf = conf)

    input = sc.textFile("logs/customer-orders.csv")
    mappedInput = input.map(extractCustomerPricePairs)
    totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

    results = totalByCustomer.collect();
    for result in results:
        print(result)

if __name__ == "__main__":
    main()