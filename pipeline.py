import luigi
import os
import shutil
from tasks.processamento_1.processamento_final_1 import ProcessamentoFinal1

class Pipeline(luigi.WrapperTask):
    def requires(self):
        return ProcessamentoFinal1()

if __name__ == '__main__':
    shutil.rmtree('dados')
    os.makedirs('dados', exist_ok = True)
             
    luigi.run()
    