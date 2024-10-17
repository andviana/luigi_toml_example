import luigi
from tasks.processamento_1.processamento_1_a import Processamento1a
from tasks.processamento_1.processamento_1_b import Processamento1b
from tasks.processamento_1.processamento_1_c import Processamento1c
from tasks.processamento_1.processamento_1_d import Processamento1d
from interfaces.task_interface import TaskInterface
import time

class ProcessamentoFinal1(luigi.Task, TaskInterface):    
    
    def requires(self):
        return [Processamento1a(), Processamento1b(), Processamento1c(), Processamento1d()]
    
    def output(self):
        return luigi.LocalTarget(self.get_config()['paths']['output_final_1'])
    
    def run(self):
        time.sleep(30)
        
        with self.input()[0].open('r') as infile_1_a:
            data_1_a = infile_1_a.read()
            
        with self.input()[1].open('r') as infile_1_b:
            data_1_b = infile_1_b.read()
            
        with self.input()[2].open('r') as infile_1_c:
            data_1_c = infile_1_c.read()
            
        final_data_1 = data_1_a + "\n" + data_1_b + "\n" + data_1_c
        
        with self.output().open('w') as outfile_1:
            outfile_1.write(final_data_1)
