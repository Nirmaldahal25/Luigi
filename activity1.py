import luigi
import time


#Progress report for Luigi
class Task1(luigi.Task):
    def run(self):
        max_loop = 100
        for i in range(max_loop):
            time.sleep(1)
            percent = i / max_loop * 100
            if(int(percent) % 10 == 0):
                self.set_status_message("Progress: %d / 100" % percent)
            
            self.set_progress_percentage(percent)
        


#command
#luigi --module activity1 Task1
'''The progress report can be seen at central planner'''
#luigid
'''The progress report cant be viewed clicking see progress button for a particular task.'''
