import luigi
import time
from interfaces.task_interface import TaskInterface
from tasks.processamento_2.baixar_dados import BaixarDados

class LimparDados(luigi.Task, TaskInterface):
    
    def requires(self):
        return BaixarDados()
    
    def output(self):
        return luigi.LocalTarget(self.get_config()['local_files']['cleaned_file'])
    
    def run(self):
        time.sleep(30) 
        
        with self.input().open('r') as input_file:
            dados = input_file.read()
            
        dados_limpos = dados.replace('exemplo', 'limpo')
        
        with self.output().open('w') as output_file:
            output_file.write(dados_limpos)
