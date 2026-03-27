import os
import csv
import requests
import time

GITHUB_API = "https://api.github.com/graphql"

# 🔐 Token direto no código (APENAS PARA TESTE)
# ⚠️ NÃO COMMITE ESTE ARQUIVO COM O TOKEN REAL!

headers = {
    "Accept": "application/vnd.github.v4+json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

query = """
query ($queryString: String!, $first: Int!, $after: String) {
  search(query: $queryString, type: REPOSITORY, first: $first, after: $after) {
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      ... on Repository {
        nameWithOwner
        url
        description
        stargazerCount
        createdAt
        pushedAt
        diskUsage
        forkCount
        releases { totalCount }
        primaryLanguage { name }
      }
    }
  }
}
"""

def run_query_with_retry(variables, max_retries=5):
    """Executa a query com tentativas automáticas em caso de erro"""
    for attempt in range(max_retries):
        try:
            print(f"Tentativa {attempt + 1}/{max_retries}...")
            
            r = requests.post(
                GITHUB_API,
                json={'query': query, 'variables': variables},
                headers=headers,
                timeout=60
            )
            
            print(f"Status code: {r.status_code}")
            
            # Se for erro 502, tenta novamente
            if r.status_code == 502:
                wait_time = 10 * (attempt + 1)
                print(f"Erro 502 (Bad Gateway). Aguardando {wait_time} segundos antes de tentar novamente...")
                time.sleep(wait_time)
                continue
            
            # Se for erro 401 (token inválido)
            if r.status_code == 401:
                print("ERRO: Token inválido! Verifique seu token do GitHub.")
                raise Exception("Token inválido - verifique suas credenciais")
            
            # Qualquer outro erro não 200
            if r.status_code != 200:
                raise Exception(f"Query falhou com status {r.status_code}\nBody: {r.text}")
            
            # Sucesso!
            return r.json()
            
        except requests.exceptions.Timeout:
            print(f"Timeout na tentativa {attempt + 1}")
            if attempt == max_retries - 1:
                raise
            time.sleep(10)
            
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(10)
    
    raise Exception("Falha após múltiplas tentativas")

def main():
    query_string = "language:Java sort:stars-desc"
    per_page = 100
    after = None
    collected = []
    total_to_collect = 1000
    page_count = 0

    print("Iniciando coleta dos top 1000 repositórios Java...")
    
    while len(collected) < total_to_collect:
        page_count += 1
        print(f"\n--- Página {page_count} | Coletados: {len(collected)}/{total_to_collect} ---")
        
        variables = {
            "queryString": query_string,
            "first": per_page,
            "after": after
        }

        try:
            data = run_query_with_retry(variables)
            
            repos = data["data"]["search"]["nodes"]
            
            if not repos:
                print("Nenhum repositório encontrado nesta página.")
                break

            for repo in repos:
                collected.append({
                    "nameWithOwner": repo.get("nameWithOwner"),
                    "url": repo.get("url"),
                    "description": repo.get("description") or "",
                    "stargazerCount": repo.get("stargazerCount"),
                    "createdAt": repo.get("createdAt"),
                    "pushedAt": repo.get("pushedAt"),
                    "diskUsage": repo.get("diskUsage"),
                    "forkCount": repo.get("forkCount"),
                    "releaseCount": (repo.get("releases") or {}).get("totalCount"),
                    "primaryLanguage": repo.get("primaryLanguage", {}).get("name")
                })

                if len(collected) >= total_to_collect:
                    break

            page_info = data["data"]["search"]["pageInfo"]

            if not page_info["hasNextPage"]:
                print("Não há mais páginas disponíveis.")
                break

            after = page_info["endCursor"]
            
            # Salva checkpoint a cada 100 repositórios
            if len(collected) % 100 == 0:
                print(f"Checkpoint: {len(collected)} repositórios coletados. Salvando...")
                save_checkpoint(collected)
            
            # Aguarda entre as páginas
            time.sleep(3)
            
        except Exception as e:
            print(f"Erro fatal: {e}")
            print(f"Salvando checkpoint com {len(collected)} repositórios coletados...")
            save_checkpoint(collected)
            break

    # Salva o resultado final
    save_final_results(collected)
    print(f"\n✅ Coleta finalizada! {len(collected)} repositórios salvos em data/top1000_repositorios_java.csv")

def save_checkpoint(collected):
    """Salva um checkpoint parcial"""
    keys = [
        "nameWithOwner", "url", "description", "stargazerCount",
        "createdAt", "pushedAt", "diskUsage", "forkCount",
        "releaseCount", "primaryLanguage"
    ]
    
    os.makedirs("data", exist_ok=True)
    
    with open("data/checkpoint.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in collected:
            writer.writerow(row)
    print(f"Checkpoint salvo: {len(collected)} repositórios")

def save_final_results(collected):
    """Salva o resultado final"""
    keys = [
        "nameWithOwner", "url", "description", "stargazerCount",
        "createdAt", "pushedAt", "diskUsage", "forkCount",
        "releaseCount", "primaryLanguage"
    ]
    
    os.makedirs("data", exist_ok=True)
    
    with open("data/top1000_repositorios_java.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in collected:
            writer.writerow(row)

if __name__ == "__main__":
    main()