from pyspark.sql import SparkSession
from argparse import ArgumentParser
import re

def split_line(line):
    """
    Split the line into a tuple of (node, neighbor)
    """
    def strip_category(word):
        if word.startswith("category:"):
            return word[len("category:"):]
        else:
            return word

    nodes = re.split(r'\t+', line)
    n1 = strip_category(nodes[0].lower())
    n2 = strip_category(nodes[1].lower())
    return (n1, n2)

def val_input(line):
    # if len(line) is 0 or startswith('#')
    if len(line) == 0 or line[0] == '#':
        return False

    # if not in the format of "node1 node2"
    words = re.split(r'\t+', line)
    if len(words) < 2:
        return False

    #Filter out the lines with invalid input format
    if ":" in words[1] and not words[1].startswith("Category:"):
        return False

    if ":" in words[0] and not words[0].startswith("Category:"):
        return False

    return True

def contrib(rank, nodes):
    """
    Compute contribution of node to all it's neighbors
    """
    num_nodes = len(nodes)
    for node in nodes:
        yield (node, rank / num_nodes)

def main(args):
    """
    Create a SparkSession and read the input file to sort by country code and timestamp and write to output file
    """
    spark = SparkSession.builder.appName("PageRank").master("spark://10.10.1.1:7077").getOrCreate()
    data_rdd = spark.read.text(args.inputfile).rdd.map(lambda line: line[0])
    data_rdd = data_rdd.filter(lambda line: val_input(line)).map(lambda line: split_line(line)).distinct()
    node_adj = data_rdd.groupByKey()

    #partition the data as specified by the user
    node_adj = node_adj.partitionBy(int(args.num_partitions))

    node_ranks = node_adj.mapValues(lambda x: 1.0) #mapValues preserves the partitioning of the RDD

    for i in range(10):
        contribs = node_adj.join(node_ranks).flatMap(lambda x: contrib(x[1][1], x[1][0]))
        contribs = contribs.reduceByKey(lambda x, y: x+y)
        node_ranks = contribs.mapValues(lambda x: 0.15 + 0.85*x).partitionBy(int(args.num_partitions)) #partition again as join does not preserve the partitioning
         
    node_ranks.saveAsTextFile(args.outputfile)

def parse_args():
    """
    Parse command line arguments
    """
    parser = ArgumentParser()
    parser.add_argument("-i", "--input", dest="inputfile", help="input file", metavar="FILE")
    parser.add_argument("-o", "--output", dest="outputfile", help="output file", metavar="FILE")
    parser.add_argument("-n", "--num_part", dest='num_partitions', help='number of partitions', metavar='INT')
    return parser.parse_args()

if __name__ == "__main__":
   main(parse_args())