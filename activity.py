import luigi

''' 
    Streams is a basic task that reads a local files with 1000 rows.
'''
class Streams(luigi.Task):
    date = luigi.DateParameter()

    def run(self):
        with self.output().open("w") as file:
            for i in range(1000):
                file.write(f"{i} {i} {i}\n")

    def output(self):
        return luigi.LocalTarget(self.date.strftime("data/artist_streams_%Y_%m_%d.csv"))


# class HadoopFileStreams(Streams):

#     def output(self):
#         return hdfs.HdfsTarget(self.date.strftime("data/artists_streams_hdfs_%Y_%m_%d.txt"))


class MyTask(luigi.Task):
    date = luigi.DateParameter()

    '''
        The requires method deals with dependencies statically. 
    '''
    def requires(self)->list:
        return [Streams(self.date)]
    
    def run(self):
        number_of_lines = 0
        for input in self.input():
            with input.open("r") as in_file:
                for _ in in_file:
                    number_of_lines += 1
        
        with self.output().open("w") as output:
            output.write(f"number_{number_of_lines}")
        
    def output(self):
        return luigi.LocalTarget(self.date.strftime("output/date_%Y_%m_%d.txt"))
    
class MyTask2(luigi.Task):
    usestream = luigi.BoolParameter(default = True, parsing=luigi.BoolParameter.EXPLICIT_PARSING)
    date = luigi.DateParameter()

    
    def run(self):
        #dynamically using dependencies
        if self.usestream:
            inp = yield Streams(self.date)

            number_of_lines = 0
            with inp.open("r") as in_file:
                for _ in in_file:
                    number_of_lines += 1
            
            with self.output().open("w") as output:
                output.write(f"number_{number_of_lines}")
        
    def output(self):
        return luigi.LocalTarget(self.date.strftime("output/task2_date_%Y_%m_%d.txt"))

if __name__ == "__main__":
    luigi.run()

    
#command
#luigi --module activity MyTask --date 2014-02-02
#luifi --module activity Mytask2 --date 2014-02-02
