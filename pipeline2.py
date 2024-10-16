import luigi
import toml
import time
import os

class Tarefa1(luigi.Task):
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(config['local_files']['numbers_up_to_10_file'])

    def run(self):
        with self.output().open('w') as f:
            for i in range(1, 11):
                f.write("{}\n".format(i))

        time.sleep(30)

class Tarefa2(luigi.Task):
    def requires(self):
        return Tarefa1()

    def output(self):
        return luigi.LocalTarget(config['local_files']['squares_file'])

    def run(self):
        with self.input().open() as fin, self.output().open('w') as fout:
            for line in fin:
                n = int(line.strip())
                out = n * n
                fout.write("{}:{}\n".format(n, out))
        time.sleep(30)

class Tarefa3(luigi.Task):
    def requires(self):
        return Tarefa2()

    def output(self):
        return luigi.LocalTarget(config['local_files']['cubes_file'])
        
    def run(self):
        with self.input().open() as fin, self.output().open('w') as fout:
            for line in fin:
                n, _ = line.strip().split(":")
                out = int(n) ** 3
                fout.write("{}:{}\n".format(n, out))
        time.sleep(30)

if __name__ == '__main__':
    config = toml.load('config.toml')
    os.makedirs('dados', exist_ok = True)
    luigi.run()

#.venv/bin/python3 -m pipeline2 Tarefa3 --workers 2 --scheduler-host localhost --scheduler-port 8082