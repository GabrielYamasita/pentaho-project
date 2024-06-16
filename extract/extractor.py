# importar sqlalchemy
from sqlalchemy import create_engine
# pymysql para conexão com mysql, schudule e time para agendamento
import pymysql, schedule, time
# datetime para log de erros
from datetime import datetime

##
# Melhorias:
# - Adicionar geração de logs caso houver um crescimento na qtd de arquivos e no volume de dados,
# porém por ser um extrator simples por agora ficará somente imprimindo no terminal os erros.
##

# importar pandas para leitura de csv em dataframe
import pandas as pd

# definição da lista de arquivos para leitura
caminho_lista = "C:\\treinamento\\Projeto-Revenda\\arquivos"
# definição do caminho dos dados em csv
caminho_csvs = "C:\\treinamento\\projetos\\projeto-pentaho-engdados1\\dados\\ext"

# metodo de extração e carga no datalake, atualmente sendo usado no mysql
def extracao_e_carga(lista, caminho):

    # declarando lista vazia
    lista_csvs = []

    try:
        with open(lista + "\\lista_arquivos.txt", 'r') as txt_arquivos:
            leitor_lista = txt_arquivos.readlines()
            
            #print(leitor_lista)
            for linha in leitor_lista:
                lista_csvs.append(linha.replace('\n', ''))

        # lista de csvs ok
        if len(lista_csvs) > 0:
            
            # imprime que a lista foi obtida
            print('\nLista de csvs obtida.')

            # cria a conexão
            conn = pymysql.Connect(
                host='localhost',
                user='seu_usuario_aqui',
                password='sua_senha_aqui',
                database='datalake-revenda-hop'    
            )

            # imprime a conn
            #print(conn)
            
            print('Realizando ingestão dos dados:')

            # iterando a lista de arquivos em txt
            for arquivo in lista_csvs:

                # leitura do csv de cultura
                df = pd.read_csv(caminho+'\\'+arquivo, delimiter=';')

                #print(df.head)

                # verificar tipagem das colunas no dataframe
                #print(df.dtypes)

                # criar engine sqlalchemy
                engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                                    .format(user="seu_usuario_aqui",
                                            pw="sua_senha_aqui",
                                            db="datalake-revenda-hop"))

                # para teste, extraí o nome da tabela conforme está na lista usando split, e
                # adicionei o sufixo '_py' para gravar a tabela com este nome no bd.
                # exemplos: clifor_py, cultura_py, cidade_py...
                tabela = arquivo.split('.')[0].split('_')[-1].lower()

                print('Criando a tabela e gravando os dados no MySQL, arquivo', arquivo)

                # inserir dataframe no banco criando a tabela com o nome definido
                df.to_sql(tabela, engine, if_exists = 'append', index = False)

                # fechar conexão
                engine.dispose()

        # lista de csvs vazia
        else:    
            print('Lista vazia, verificar o arquivo')
    # tratando erros input/output        
    except IOError:
        print('IOError: não foi possível ler a lista de arquivos.', IOError)
    # tratando erros gerais
    except Exception as e:
        print('Ocorreu um erro.')
        print('\nDetalhes da Exception:')
        print('Exception em: ', e)
        print('Tipo de Exception: ',type(e).__name__)

# metodo principal a ser agendado com módulo schedule
def extrair_dados():
    try:
        extracao_e_carga(caminho_lista, caminho_csvs)
    except Exception as e:
        print('Ocorreu um erro.')
        print('\nDetalhes da Exception:')
        print('Exception at: ', e)
        print('Tipo de Exception: ',type(e).__name__)
    
    # imprimir horário em que finalizou
    agora = datetime.now()
    horario_finalizacao = agora.strftime("%H:%M:%S")
    print('\nExecução finalizada às ', horario_finalizacao)


#extrair_dados()

# agendamento da execução, todos os dias a cada 12 horas, às 05:00 e 17:00
schedule.every().day.at("17:00").do(extrair_dados)
schedule.every().day.at("05:00").do(extrair_dados)

# manter rodando o agendamento
while True:
    schedule.run_pending()
    time.sleep(1)