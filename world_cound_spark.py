import re
import time
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sys

from pyspark import SparkContext, SparkConf


app_name = "hello_world"
master = "local[*]"


def emit_word_counts(line):
    # Split the line into words
    for word in line.split():
        # Return a tuple of word, count
        yield (word, 1)


if __name__ == "__main__":
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf=conf)

    corpus = [
        "Peter Piper picked a peck of pickled peppers",
        "a peck of pickled peppers Peter Piper picked",
        "if Peter Piper picked a peck of pickled peppers",
        "where’s the peck of pickled peppers that Peter Piper picked",
        "peck picker"
    ]

    rdd = sc.parallelize(corpus)
    word_counts = rdd.flatMap(emit_word_counts).reduceByKey(lambda x, y: x + y).sortBy(lambda x: -x[1])
    word_counts.saveAsTextFile("results_pyspark")