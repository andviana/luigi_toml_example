import luigi
from tasks.processamento_1.leitura_arquivo import LeituraArquivo
from interfaces.task_interface import TaskInterface
import time

class Processamento1b(luigi.Task, TaskInterface):
        
    def requires(self):
        return LeituraArquivo()
    
    def output(self):
        return luigi.LocalTarget(self.get_config()['paths']['output_1_b'])
    
    def run(self):
        time.sleep(30)
        
        with self.input().open('r') as infile:
            data = infile.read()
            processed_data = ' '.join(reversed(data))
            
        with self.output().open('w') as outfile:
            outfile.write(processed_data)
            
        