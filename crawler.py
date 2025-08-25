import pandas as pd
import psutil
import time
import os
from datetime import datetime

# --- Configurações do projeto ---
DURACAO_CAPTURA = 30 * 60  # 30 minutos em segundos
INTERVALO_COLETA = 30     # 30 segundos
CAMINHO_PASTA = 'dados_monitoramento'

# --- Funções de apoio ---

def coletar_dados_hardware():
    """Coleta dados de CPU, RAM e Disco e retorna um dicionário."""
    return {
        'timestamp': datetime.now(),
        'cpu': psutil.cpu_percent(),
        'ram': psutil.virtual_memory().percent,
        'disco': psutil.disk_usage('/').percent
    }

def registrar_log(mensagem):
    """Adiciona uma entrada ao arquivo de log."""
    caminho_log = os.path.join(CAMINHO_PASTA, 'log_processamento.csv')
    log_data = pd.DataFrame([{
        'timestamp': datetime.now(),
        'evento': mensagem
    }])

    # Checa se o arquivo de log já existe para adicionar ou criar
    if os.path.exists(caminho_log):
        log_data.to_csv(caminho_log, mode='a', header=False, index=False)
    else:
        log_data.to_csv(caminho_log, index=False)

def adicionar_a_chunks(nome_arquivo):
    """Adiciona o nome do novo arquivo criado ao arquivo de chunks."""
    caminho_chunks = os.path.join(CAMINHO_PASTA, 'chunks_processados.csv')
    chunk_data = pd.DataFrame([{
        'timestamp': datetime.now(),
        'nome_arquivo': nome_arquivo
    }])
    
    # Checa se o arquivo de chunks já existe para adicionar ou criar
    if os.path.exists(caminho_chunks):
        chunk_data.to_csv(caminho_chunks, mode='a', header=False, index=False)
    else:
        chunk_data.to_csv(caminho_chunks, index=False)

# --- Lógica principal ---
def main():
    print("Iniciando o monitoramento. Pressione Ctrl+C a qualquer momento para sair.")

    # Garante que a pasta de destino exista
    if not os.path.exists(CAMINHO_PASTA):
        os.makedirs(CAMINHO_PASTA)

    inicio_captura = time.time()
    dados_coletados = []

    while True:
        try:
            # Verifica se já passou 30 minutos
            if time.time() - inicio_captura >= DURACAO_CAPTURA:
                
                # Nome do arquivo com timestamp para ser único
                nome_arquivo_local = f"dados_totem_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
                caminho_arquivo_local = os.path.join(CAMINHO_PASTA, nome_arquivo_local)

                df_dados = pd.DataFrame(dados_coletados)
                df_dados.to_csv(caminho_arquivo_local, index=False)
                
                print(f"Captura de 30 minutos finalizada. Dados salvos em {caminho_arquivo_local}")
                
                # Log e Chunk
                registrar_log(f"Novo arquivo de dados criado: {nome_arquivo_local}")
                adicionar_a_chunks(nome_arquivo_local)
                
                # Reseta para a próxima captura
                inicio_captura = time.time()
                dados_coletados = []
            
            # Coleta os dados a cada 30 segundos
            dados_coletados.append(coletar_dados_hardware())
            print(f"Dados coletados em {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(INTERVALO_COLETA)

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