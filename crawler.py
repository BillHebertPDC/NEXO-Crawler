import pandas as pd
import psutil
import time
import os
from datetime import datetime

# --- Configurações do projeto ---
DURACAO_CAPTURA = 1 * 60  # 30 minutos em segundos
INTERVALO_COLETA = 30     # 30 segundos
CAMINHO_PASTA = 'dados_monitoramento'
NOME_ARQUIVO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
CAMINHO_ARQUIVO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO)
CAMINHO_LOG = os.path.join(CAMINHO_PASTA, 'log_processamento.csv')
CAMINHO_CHUNKS = os.path.join(CAMINHO_PASTA, 'chunks_processados.csv')

# --- Funções de apoio ---
def coletar_dados_hardware():
    return {
        'timestamp': datetime.now(),
        'cpu': psutil.cpu_percent(),
        'ram': psutil.virtual_memory().percent,
        'disco': psutil.disk_usage('/').percent
    }
def salvar_arquivo(dataFrame,CAMINHO):
    if os.path.exists(CAMINHO):
        dataFrame.to_csv(CAMINHO, mode='a', header=False, index=False)
    else:
        dataFrame.to_csv(CAMINHO, index=False)
def registrar_log(mensagem):
    log_data = pd.DataFrame([{
        'timestamp': datetime.now(),
        'evento': mensagem
    }])
    salvar_arquivo(log_data,CAMINHO_LOG)
def adicionar_a_chunks(nome_arquivo):
    chunk_data = pd.DataFrame([{
        'timestamp': datetime.now(),
        'nome_arquivo': nome_arquivo
    }])
    salvar_arquivo(chunk_data,CAMINHO_CHUNKS)
def redefinir_caminho():
    global NOME_ARQUIVO
    global CAMINHO_ARQUIVO
    NOME_ARQUIVO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    CAMINHO_ARQUIVO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO)
    return CAMINHO_ARQUIVO,NOME_ARQUIVO
# --- Lógica principal ---
def main():
    print("Iniciando o monitoramento. Pressione Ctrl+C a qualquer momento para sair.")
    if not os.path.exists(CAMINHO_PASTA):
        os.makedirs(CAMINHO_PASTA)
    inicio_captura = time.time()
    dados_coletados = []
    redefinir_caminho()
    while True:
        try:
            time.sleep(1)
            dados_coletados.append(coletar_dados_hardware())
            df_dados = pd.DataFrame(dados_coletados)
            df_dados.to_csv(CAMINHO_ARQUIVO, index=False)
            if time.time() - inicio_captura >= DURACAO_CAPTURA:
                redefinir_caminho()
                registrar_log(f"Novo arquivo de dados criado: {NOME_ARQUIVO}")
                print(f"Captura finalizada. Dados salvos em {CAMINHO_ARQUIVO}")
                adicionar_a_chunks(NOME_ARQUIVO)
                inicio_captura = time.time()
                dados_coletados = []
        except KeyboardInterrupt:
            print("\nMonitoramento interrompido pelo usuário.")
            registrar_log("Monitoramento interrompido manualmente.")
            break
        except Exception as e:
            print(f"Ocorreu um erro: {e}")
            registrar_log(f"ERRO: {e}")
            break

if __name__ == "__main__":
    main()