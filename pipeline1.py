import luigi
import toml
import time
import os


class BaixarDados(luigi.Task):
    def output(self):
        return luigi.LocalTarget(config['local_files']['input_file'])

    def run(self):
        with self.output().open('w') as f:
            f.write('dados de exemplo')
        time.sleep(60)  # Pausa de 1 minuto


class LimparDados(luigi.Task):
    def requires(self):
        return BaixarDados()

    def output(self):
        return luigi.LocalTarget(config['local_files']['cleaned_file'])

    def run(self):
        with self.input().open('r') as input_file:
            dados = input_file.read()
        dados_limpos = dados.replace('exemplo', 'limpo')
        with self.output().open('w') as output_file:
            output_file.write(dados_limpos)
        time.sleep(60)  # Pausa de 1 minuto

class ProcessarDados(luigi.Task):
    def requires(self):
        return LimparDados()

    def output(self):
        return luigi.LocalTarget(config['local_files']['processed_file'])

    def run(self):
        with self.input().open('r') as input_file:
            dados = input_file.read()
        dados_processados = dados.upper()
        with self.output().open('w') as output_file:
            output_file.write(dados_processados)
        time.sleep(60)  # Pausa de 1 minuto

class GerarRelatorio(luigi.Task):
    def requires(self):
        return ProcessarDados()

    def output(self):
        return luigi.LocalTarget(config['local_files']['report_file'])

    def run(self):
        with self.input().open('r') as input_file:
            dados = input_file.read()
        relatorio = f"Relatório de Dados:\n\n{dados}"
        with self.output().open('w') as output_file:
            output_file.write(relatorio)
        time.sleep(60)  # Pausa de 1 minuto

class Pipeline(luigi.WrapperTask):
    def requires(self):
        return GerarRelatorio()

if __name__ == '__main__':
    
    
    
    # Carrega a configuração
    config = toml.load('config.toml')
    os.makedirs('dados', exist_ok = True)
    # luigi.run()

    # sem o wrapper
    luigi.build([GerarRelatorio()], workers=5, scheduler_host = "localhost")

    #to execute run this command on terminal
    # .venv/bin/python3 my_pipeline.py Pipeline --scheduler-host localhost