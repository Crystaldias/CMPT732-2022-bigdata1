'''
samples = [integer from command line]
total_iterations = 0
repeat batches times:
    iterations = 0
    # group your experiments into batches to reduce the overhead
    for i in range(samples/batches):
        sum = 0.0
        while sum < 1
            sum += [random double 0 to 1]
            iterations++
    total_iterations += iterations

print(total_iterations/samples)
'''
from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
import random
# import time

def get_random_value():
    return random.random()

def get_sum(a, b):
    return a+b

def key_value(a,b):
    return (a,b)

def get_sample_sum(sample_no):
    iterations = 0
    sum = 0.0
    random.seed()
    while sum < 1.0:
        sum += random.random()
        iterations += 1
    return iterations       

    
def main(no_of_samples):
    total_iterations = 0
    iterations_list = []
    samples = sc.parallelize(range(no_of_samples),numSlices=10)
    # print(samples.glom().collect())
    total_iterations = samples.map(get_sample_sum).reduce(get_sum)
    return total_iterations/no_of_samples
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('Euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    # start_time = time.time()
    no_of_samples = int(sys.argv[1])
    euler = main(no_of_samples)
    
    print(euler)
    # print("--- %s seconds ---" % (time.time() - start_time))