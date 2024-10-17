import luigi
from tasks.processamento_1.leitura_arquivo import LeituraArquivo
from tasks.processamento_2.gerar_relatorio import GerarRelatorio
from interfaces.task_interface import TaskInterface
import time

class Processamento1d(luigi.Task, TaskInterface):
            
    def requires(self):
        return [LeituraArquivo(), GerarRelatorio()]
    
    def output(self):
        return luigi.LocalTarget(self.get_config()['paths']['output_1_d'])
    
    def run(self):
        time.sleep(30)
        
        with self.input()[0].open('r') as infile1:
            data1 = infile1.read()
            
        with self.input()[1].open('r') as infile2:
            data2 = infile2.read()
            
        processed_data = 'finalizado processamento do pipeline D \n \n ' + data1 + "\n" + data2
            
        with self.output().open('w') as outfile:
            outfile.write(processed_data)
            
        