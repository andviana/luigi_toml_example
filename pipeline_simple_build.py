import luigi
import toml
import time
import os
import shutil


class BaixarDados(luigi.Task):
    
    def output(self):
        return luigi.LocalTarget(config['local_files']['input_file'])

    def run(self):
        time.sleep(30)
        
        with self.output().open('w') as f:
            f.write('dados de exemplo')
    
    
class LimparDados(luigi.Task):
    
    def requires(self):
        return BaixarDados()

    def output(self):
        return luigi.LocalTarget(config['local_files']['cleaned_file'])

    def run(self):
        time.sleep(30)

        with self.input().open('r') as input_file:
            dados = input_file.read()
            
        dados_limpos = dados.replace('exemplo', 'limpo')
        
        with self.output().open('w') as output_file:
            output_file.write(dados_limpos)
        
        
class ProcessarDados(luigi.Task):
    
    def requires(self):
        return LimparDados()

    def output(self):
        return luigi.LocalTarget(config['local_files']['processed_file'])

    def run(self):
        time.sleep(30)  
        
        with self.input().open('r') as input_file:
            dados = input_file.read()
            
        dados_processados = dados.upper()
        
        with self.output().open('w') as output_file:
            output_file.write(dados_processados)
        

class GerarRelatorio(luigi.Task):
    
    def requires(self):
        return ProcessarDados()

    def output(self):
        return luigi.LocalTarget(config['local_files']['report_file'])

    def run(self):
        time.sleep(30)
        
        with self.input().open('r') as input_file:
            dados = input_file.read()
            
        relatorio = f"Relatório de Dados:\n\n{dados}"
        
        with self.output().open('w') as output_file:
            output_file.write(relatorio)
        

class Pipeline(luigi.WrapperTask):
    
    def requires(self):
        return GerarRelatorio()


if __name__ == '__main__':
    # Carrega a configuração
    config = toml.load('config.toml')
    shutil.rmtree('dados')
    os.makedirs('dados', exist_ok = True)
    # luigi.run()
    # sem o wrapper (executa diretamente o GerarRelatorio)
    luigi.build([GerarRelatorio()], scheduler_host = "localhost")

    # run this command on terminal
    # .venv/bin/python3 pipeline_simple_build.py Pipeline --scheduler-host localhost