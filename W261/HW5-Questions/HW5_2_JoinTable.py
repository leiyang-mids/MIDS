from mrjob.job import MRJob
from mrjob.step import MRStep

class JoinTable(MRJob):
    
    def mapper_init(self):
        self.left = {}
        self.right = []
        self.vistor = None
        
    # stream through lines, yield char count
    def mapper_inner(self, _, line):
        # get page id
        line = line.strip()
        # A-line
        if line[0] == 'A':
            d1, p_id, d2, p_name, url = line.split(',')
            self.left[p_id] = [p_name, url]
            return
        # C-line
        if line[0] == 'C':
            d1, d2, v_id = line.split(',')
            self.vistor = 'C_' + v_id
            return
        # V-line
        if line[0] == 'V':
            d1, p_id, d2 = line.split(',')
        else:
            return
        
        # inner join
        if p_id not in self.left:
            return
        else:        
            self.right.append(p_id)
            yield (p_id,self.vistor), self.left[p_id][1]
            
    def mapper_right(self, _, line):
        # get page id
        line = line.strip()
        # A-line
        if line[0] == 'A':
            d1, p_id, d2, p_name, url = line.split(',')
            self.left[p_id] = [p_name, url]
            return
        # C-line
        if line[0] == 'C':
            d1, d2, v_id = line.split(',')
            self.vistor = 'C_' + v_id
            return
        # V-line
        if line[0] == 'V':
            d1, p_id, d2 = line.split(',')
        else:
            return
        
        # right join
        if p_id not in self.left:
            yield (p_id, self.vistor), None
        else:            
            yield (p_id, self.vistor), self.left[p_id][1]
            
    def mapper_left(self, _, line):
        # get page id
        line = line.strip()
        # A-line
        if line[0] == 'A':
            d1, p_id, d2, p_name, url = line.split(',')
            self.left[p_id] = [p_name, url]
            return
        # C-line
        if line[0] == 'C':
            d1, d2, v_id = line.split(',')
            self.vistor = 'C_' + v_id
            return
        # V-line
        if line[0] == 'V':
            d1, p_id, d2 = line.split(',')
        else:
            return
        
        # right join
        if p_id in self.left:                        
            yield (p_id, self.vistor), self.left[p_id][1]
        elif p_id not in self.right:
            self.right.append(p_id)
    
    # left join only, yield left which right doesn't have
    def mapper_final(self):
        for p_id in self.right:
            yield (p_id, None), self.left[p_id][1]
        

    def reducer_init(self):
        self.n_row = 0
        
    def reducer(self, page, url):        
        self.n_row += 1
        
    def reducer_final(self):
        yield None, str(self.n_row)

    def steps(self):
        jobconf = {
            'mapreduce.job.maps': '3',
            'mapreduce.job.reduces': '1',
        }

        return [MRStep(mapper_init=self.mapper_init
                       # inner join
                       #,mapper=self.mapper_inner
                       # right join
                       #,mapper=self.mapper_right
                       # left join
                       ,mapper=self.mapper_left, mapper_final=self.mapper_final
                       # reducer
                       ,reducer_init=self.reducer_init, reducer=self.reducer, reducer_final=self.reducer_final
                       #,jobconf=jobconf
                       )                
               ]


if __name__ == '__main__':
    JoinTable.run()
