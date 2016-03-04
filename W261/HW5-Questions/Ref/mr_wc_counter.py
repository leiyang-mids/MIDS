from mrjob.job import MRJob
from mrjob.step import MRStep
import re
 
WORD_RE = re.compile(r"[\w']+")
 
class MRWordFreqCount(MRJob):
    def init_get_words(self):
        self.words = {}

    def get_words(self, _, line):
        self.increment_counter('group', 'Num_mapper_calls', 1)
        for word in WORD_RE.findall(line):
            word = word.lower()
            self.words.setdefault(word, 0)
            self.words[word] = self.words[word] + 1

    def final_get_words(self):
        self.increment_counter('group', 'Num_mapper_final_calls', 1)
        for word, val in self.words.iteritems():
            yield word, val

    def sum_words_combiner(self, word, counts):
        self.increment_counter('group', 'Num_combiner_calls', 1)
        yield word, sum(counts)
        
    def sum_words(self, word, counts):
        self.increment_counter('group', 'Num_reducer_calls', 1)
        yield word, sum(counts)
        
    def steps(self):
        return [MRStep(mapper_init=self.init_get_words,
                       mapper=self.get_words,
                       mapper_final=self.final_get_words,
                       combiner=self.sum_words_combiner,
                       reducer=self.sum_words)]

if __name__ == '__main__':
    MRWordFreqCount.run()