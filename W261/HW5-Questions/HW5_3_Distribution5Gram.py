from mrjob.job import MRJob
from mrjob.step import MRStep

class Distribution5Gram(MRJob):

    # stream through lines, yield word count
    def mapper(self, _, line):
        # get counts
        n_gram, cnt, p_cnt, b_cnt = line.strip().split('\t')
        yield len(n_gram), int(cnt)

    # combiner/reducer
    def combiner(self, n_gram, count):
        yield n_gram, sum(count)

    # job to sort the results ###########################
    def mapper_sort1(self, word, ratio):
        yield (word, ratio), None

    def reducer_sort1(self, results, dummy):
        yield results

    def steps(self):
        jobconf1 = {  #key value pairs
            'mapreduce.job.maps': '30',
            'mapreduce.job.reduces': '30',
        }

        jobconf2 = {  #key value pairs
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k2,2nr',
            'mapreduce.job.maps': '8',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
        }

        return [MRStep(mapper=self.mapper
                       ,combiner=self.combiner
                       ,reducer=self.combiner
                       ,jobconf=jobconf1
                       )
                ,MRStep(mapper=self.mapper_sort1
                       ,reducer=self.reducer_sort1
                       ,jobconf=jobconf2
                       )
               ]


if __name__ == '__main__':
    Distribution5Gram.run()
