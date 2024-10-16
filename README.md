# Orquestração de tarefas (pipeline) com Luigi
[](./images/luigi.webp)
exemplo de implemtação de orquestração de tarefas em python utilizando as tecnologias
- [luigi](https://luigi.readthedocs.io/en/stable/index.html)
- [toml](https://github.com/toml-lang/toml)

> O Luigi é um pacote python que auxilia a construção de pipelines complexos de trabaho em lotes. Ele lida com:
- resolução de dependência
- gerenciamento de fluxo de trabalho
- visualização
- gerenciamento de falhas
- CLI e outros

### Passo 1
faça o download do projeto para a pasta desejada 
```
git clone https://github.com/andviana/luigi_toml_example.git
```
### Passo 2
entre na pasta do projeto e abra a sua IDE
```
cd luigi_toml_example
code .
```

### Passo 3 (opcional)
caso esteja no ubuntu e precisar criar um ambiente vitual, no terminal, execute os comandos abaixo
```
python3 -m venv .venv
source .venv/bin/activate
```
caso queira desativar o venv, use no teminal o comando 
> caso seja utilizado o deactivate, problemas podem ocorrer ao utilizar o pip
```
deactivate
```

### Passo 4
no terminal, realize a instalação do luigi com toml
```
pip install luigi[toml]
```
### Passo 5
Abrir o monitor web do luigi informando a porta 8082
```
#rodar o monitor web do luigi
.venv/bin/luigid --port 8082
```
abra o navegador no endereço [http://localhost:8082](http://localhost:8082)

### Passo 6
O exemplo possui 2 pipelines. para executá-las, no terminal da sua IDE execute os comandos:
```
.venv/bin/python3 pipeline1.py Pipeline --scheduler-host localhost

.venv/bin/python3 -m pipeline2 Tarefa3 --workers 2 --scheduler-host localhost --scheduler-port 8082

```

### Passo 7
Para executar novamente os pipelines, exclua a pasta "dados"
```
rm -rf dados
```

## Capturas de tela da Execução
1. Lista de Tarefas (tasklist)
[](./images/taskllist.png)

2. Graficos da Execução
[](./images/grafico.png)

3. Workers
[](./images/workers.png)

