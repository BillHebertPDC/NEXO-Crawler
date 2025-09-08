import pandas as pd
import psutil
import time
import os
from datetime import datetime
from uuid import getnode as get_mac

# --- Configurações do projeto ---
DURACAO_CAPTURA = 1 * 60  # 30 minutos em segundos
CAMINHO_PASTA = 'dados_monitoramento'
MAC_ADRESS = get_mac()
NOME_ARQUIVO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')} - {MAC_ADRESS}.csv"
NOME_ARQUIVO_PROCESSO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}-Processos {MAC_ADRESS}.csv"
CAMINHO_ARQUIVO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO)
CAMINHO_ARQUIVO_PROCESSO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO_PROCESSO)
NOME_LOG = f"log_processamento_{MAC_ADRESS}.csv"
CAMINHO_LOG = os.path.join(CAMINHO_PASTA, NOME_LOG)
NOME_CHUNK = f"chunks_processados_{MAC_ADRESS}.csv"
CAMINHO_CHUNKS = os.path.join(CAMINHO_PASTA, NOME_CHUNK)

# --- Funções de apoio ---
def coletar_dados_hardware():
    return {
        'timestamp': datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
        'cpu': psutil.cpu_percent(),
        'ram': psutil.virtual_memory().percent,
        'disco': psutil.disk_usage('/').percent,
        'mac' : MAC_ADRESS
    }
def coletar_dados_processos():
    processos_info = []
    for proc in psutil.process_iter():
        try:
           proc.cpu_percent(interval=None)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue


    time.sleep(1)


    for proc in psutil.process_iter():
        try:
            cpu = round(proc.cpu_percent(interval=None)/ psutil.cpu_count(logical=True),1)
            disco = round((proc.io_counters().write_bytes / (1024 ** 2)),1)
            ram = round((proc.memory_info().rss * 100 / psutil.virtual_memory().total),1)
            if cpu > 0 or ram > 1 or disco > 1:
                if ram < 1:
                    ram = 0
                if disco < 1:
                    disco = 0
                processos_info.append({ 
                    'timestamp' : datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
                    'processo' : proc.name(),
                    'cpu' : cpu,
                    'ram' : ram,
                    'dados_gravados' : disco,
                    'mac' : MAC_ADRESS})
        
             
                       
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return processos_info
   
def salvar_arquivo(dataFrame,CAMINHO):
    if os.path.exists(CAMINHO):
        dataFrame.to_csv(CAMINHO, mode='a', header=False, index=False)
    else:
        dataFrame.to_csv(CAMINHO, index=False)
def registrar_log(mensagem):
    log_data = pd.DataFrame([{
        'timestamp': datetime.now(),
        'evento': mensagem,
        'mac' : MAC_ADRESS
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
    global NOME_ARQUIVO_PROCESSO
    global CAMINHO_ARQUIVO_PROCESSO
    global MAC_ADRESS
    NOME_ARQUIVO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')} - {MAC_ADRESS}.csv"
    CAMINHO_ARQUIVO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO)
    NOME_ARQUIVO_PROCESSO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}-Processos {MAC_ADRESS}.csv"
    CAMINHO_ARQUIVO_PROCESSO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO_PROCESSO)
    return CAMINHO_ARQUIVO,NOME_ARQUIVO, NOME_ARQUIVO_PROCESSO, CAMINHO_ARQUIVO_PROCESSO
# --- Lógica principal ---
def main():
    print("Iniciando o monitoramento. Pressione Ctrl+C a qualquer momento para sair.")
    if not os.path.exists(CAMINHO_PASTA):
        os.makedirs(CAMINHO_PASTA)
    inicio_captura = time.time()
    dados_coletados = []
    processos = []
    redefinir_caminho()
    while True:
        try:
            time.sleep(1)
            dados_coletados.append(coletar_dados_hardware())
            processos = coletar_dados_processos()

            df_dados = pd.DataFrame(dados_coletados)
            df_dados.to_csv(CAMINHO_ARQUIVO, index=False)

            df_processo = pd.DataFrame(processos)
            df_processo.to_csv(CAMINHO_ARQUIVO_PROCESSO, index=False)
            
            if time.time() - inicio_captura >= DURACAO_CAPTURA:
                redefinir_caminho()

                registrar_log(f"Novo arquivo de dados criado: {NOME_ARQUIVO}")
                registrar_log(f"Novo arquivo de dados criado: {NOME_ARQUIVO_PROCESSO}")

                print(f"Captura finalizada. Dados salvos em {CAMINHO_ARQUIVO} e em {CAMINHO_ARQUIVO_PROCESSO}")

                adicionar_a_chunks(NOME_ARQUIVO_PROCESSO)
                adicionar_a_chunks(NOME_ARQUIVO)

                inicio_captura = time.time()
                dados_coletados = []
                processos = []
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