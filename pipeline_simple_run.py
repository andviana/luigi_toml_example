import luigi
import toml
import time
import os
import shutil


class Tarefa1(luigi.Task):
    
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(config['pipe_simple']['numbers_up_to_10_file'])

    def run(self):
        time.sleep(30)

        with self.output().open('w') as f:
            for i in range(1, 11):
                f.write("{}\n".format(i))

       

class Tarefa2(luigi.Task):
    
    def requires(self):
        return Tarefa1()

    def output(self):
        return luigi.LocalTarget(config['pipe_simple']['squares_file'])

    def run(self):
        time.sleep(30)
        
        with self.input().open() as fin, self.output().open('w') as fout:
            for line in fin:
                n = int(line.strip())
                out = n * n
                fout.write("{}:{}\n".format(n, out))
        


class Tarefa3(luigi.Task):
    
    def requires(self):
        return Tarefa2()

    def output(self):
        return luigi.LocalTarget(config['pipe_simple']['cubes_file'])
        
    def run(self):
        time.sleep(30)
        
        with self.input().open() as fin, self.output().open('w') as fout:
            for line in fin:
                n, _ = line.strip().split(":")
                out = int(n) ** 3
                fout.write("{}:{}\n".format(n, out))
        

if __name__ == '__main__':
    config = toml.load('config.toml')
    shutil.rmtree('dados')
    os.makedirs('dados', exist_ok = True)
    luigi.run()

#.venv/bin/python3 -m pipeline_simple_run Tarefa3 --scheduler-host localhost --scheduler-port 8082