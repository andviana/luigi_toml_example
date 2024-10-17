import luigi
import time
from interfaces.task_interface import TaskInterface

class BaixarDados(luigi.Task, TaskInterface):
    
    def output(self):
        return luigi.LocalTarget(self.get_config()['local_files']['input_file'])

    def run(self):
        time.sleep(30)
        
        with self.output().open('w') as output_file:
            output_file.write('dados de exemplo')
