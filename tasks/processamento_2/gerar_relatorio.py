import luigi
import time
from interfaces.task_interface import TaskInterface
from tasks.processamento_2.processar_dados import ProcessarDados

class GerarRelatorio(luigi.Task, TaskInterface):
    
    def requires(self):
        return ProcessarDados()

    def output(self):
        return luigi.LocalTarget(self.get_config()['local_files']['report_file'])

    def run(self):
        time.sleep(30)
        
        with self.input().open('r') as input_file:
            dados = input_file.read()
            
        relatorio = f"Relatório de Dados:\n\n{dados}"
        
        with self.output().open('w') as output_file:
            output_file.write(relatorio)
