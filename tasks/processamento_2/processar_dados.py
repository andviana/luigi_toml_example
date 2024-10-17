import luigi
import time
from interfaces.task_interface import TaskInterface
from tasks.processamento_2.limpar_dados import LimparDados

class ProcessarDados(luigi.Task, TaskInterface):
    
    def requires(self):
        return LimparDados()
    
    def output(self):
        return luigi.LocalTarget(self.get_config()['local_files']['processed_file'])
    
    def run(self):
        time.sleep(30)
        
        with self.input().open('r') as input_file:
            dados = input_file.read()
            
        dados_processados = dados.upper()
        
        with self.output().open('w') as output_file:
            output_file.write(dados_processados)        
