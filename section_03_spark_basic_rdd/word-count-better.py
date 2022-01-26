import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    # '\W+' # 텍스트를 단어 단위로 분리
    # 구두점도 사라짐
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

def main():
    conf = SparkConf().setMaster("local").setAppName("WordCount")
    sc = SparkContext(conf = conf)

    input = sc.textFile("data/book.txt")
    words = input.flatMap(normalizeWords)
    wordCounts = words.countByValue()

    for word, count in wordCounts.items():
        cleanWord = word.encode('ascii', 'ignore')
        if (cleanWord):
            print(cleanWord.decode() + " " + str(count))

if __name__ == "__main__":
    main()