import luigi
import pandas as pd
from io import StringIO
''' 
    Streams is a basic task that reads a local files with 1000 rows.
'''
class Streams(luigi.Task):
    stream = luigi.OptionalPathParameter()
        
    def output(self):
        return luigi.LocalTarget(f"{self.stream}")

class Merge(luigi.Task):
    def requires(self):
        return [Streams(stream = "input/input1.csv"), Streams(stream = "input/input2.csv")]
    
    def run(self):
        dataframe = pd.DataFrame()
        for file in self.input():
            with file.open("r") as file:
                string = StringIO(file.read())
                df = pd.read_csv(string, sep=",")
                df1 = df[["id","name","address"]].copy()
                if dataframe.empty:
                    dataframe = df1
                else:
                    dataframe = pd.concat([dataframe, df1])
    

        with self.output().open("w") as file:
            string_buf = StringIO(newline="")
            dataframe.to_csv(string_buf, index=False, lineterminator="\n")
            string = string_buf.getvalue()
            count = 0
            for i in string:
                if i == "\n":
                    count += 1
                
            print("count: ", count)

            file.write(string_buf.getvalue())
            
    def output(self):
        return luigi.LocalTarget("output/merge.csv")

# luigi --module activity3 Merge --local-scheduler