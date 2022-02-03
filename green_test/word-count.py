from pyspark import SparkConf, SparkContext

def main():
    conf = SparkConf().setMaster("local").setAppName("WordCount")
    sc = SparkContext(conf = conf)

    input = sc.textFile("book.txt")
    words = input.flatMap(lambda x: x.split())
    print(words.collect()[:5])
    wordCounts = words.countByValue()

    for word, count in wordCounts.items():
        cleanWord = word.encode('ascii', 'ignore') # 인코딩 문제 처리 위한 코드
        if (cleanWord):
            print(cleanWord.decode() + " " + str(count))

if __name__ == "__main__":
    main()