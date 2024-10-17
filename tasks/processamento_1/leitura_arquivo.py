import luigi
from interfaces.task_interface import TaskInterface

class LeituraArquivo(luigi.Task, TaskInterface):    
    
    def output(self):
        return luigi.LocalTarget(self.get_config()['paths']['input'])
    
    def run(self):
        with open(self.get_config()['paths']['input'], 'w') as input_data:
            input_data.write(self.get_config()['input_data']['content'])