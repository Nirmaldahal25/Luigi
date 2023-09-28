import luigi

class A(luigi.Task):
    def run(self):
        with self.output().open('w') as file:
            file.write("task A\n")
        print("task A")

    #dynamically assign priority level
    @property
    def priority(self):
        return 1

    def output(self):
        return luigi.LocalTarget('output/activity2_taska.csv')
    
class B(luigi.Task):
    #statically assign priority level
    priority = 1000 #has higher priority
    def run(self):
        with self.output().open('w') as file:
            file.write("task B\n")
        print("task B") 
    
    def output(self):
        return luigi.LocalTarget('output/activity2_taskb.csv')
    
    

class C(luigi.Task):
    def requires(self):
        return [A(),B()] #task B has higher priority than A
    def run(self):
        print("task C")
    
    def output(self):
        return luigi.LocalTarget('output/activity2_taskc.csv')
    

#command 
#luigi --module activity2 C 