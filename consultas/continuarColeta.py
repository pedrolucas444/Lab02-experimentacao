# continuar_coleta_corrigido.py
import os
import csv
import requests
import time

GITHUB_API_REST = "https://api.github.com"

headers = {
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

def search_repos(page=1, per_page=100):
    """Busca repositórios Java usando a API REST"""
    url = f"{GITHUB_API_REST}/search/repositories"
    params = {
        "q": "language:Java",
        "sort": "stars",
        "order": "desc",
        "per_page": per_page,
        "page": page
    }
    
    try:
        print(f"Buscando página {page}...")
        r = requests.get(url, headers=headers, params=params, timeout=60)
        print(f"Status: {r.status_code}")
        
        if r.status_code == 403:
            remaining = r.headers.get('X-RateLimit-Remaining')
            reset_time_raw = r.headers.get('X-RateLimit-Reset')
            print(f"Rate limit restante: {remaining}")
            
            if reset_time_raw:
                reset_time = int(reset_time_raw)
                wait_time = reset_time - time.time()
                if wait_time > 0:
                    print(f"Aguardando {wait_time:.0f} segundos até o reset...")
                    time.sleep(wait_time + 5)
                    return search_repos(page, per_page)
            return None
            
        if r.status_code != 200:
            print(f"Erro: {r.status_code}")
            return None
            
        return r.json()
        
    except Exception as e:
        print(f"Erro na requisição: {e}")
        return None

def main():
    arquivo_csv = "data/top1000_repositorios_java.csv"
    
    # Verifica se o arquivo existe e lê os repositórios já coletados
    collected = []
    if os.path.exists(arquivo_csv):
        with open(arquivo_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                collected.append(row)
        print(f"Repositórios já coletados: {len(collected)}")
    else:
        print("Arquivo não encontrado. Coletando do zero...")
    
    # Se já tem 1000, não precisa continuar
    if len(collected) >= 1000:
        print("✅ Já temos 1000 repositórios!")
        return
    
    total_to_collect = 1000
    # Calcula quantas páginas já foram coletadas
    # Se coletou 900, significa que foram 9 páginas (página 10 está incompleta)
    pages_collected = len(collected) // 100
    start_page = pages_collected + 1
    
    print(f"Continuando da página {start_page}...")
    
    per_page = 100
    
    # Conjunto de nomes já coletados para evitar duplicatas
    collected_names = {repo["nameWithOwner"] for repo in collected}
    
    page = start_page
    while len(collected) < total_to_collect and page <= 15:  # até página 15 por segurança
        print(f"\n--- Página {page} | Coletados: {len(collected)}/{total_to_collect} ---")
        
        result = search_repos(page, per_page)
        
        if not result or "items" not in result:
            print("Erro ou fim dos resultados")
            break
            
        repos = result["items"]
        
        if not repos:
            print("Nenhum repositório encontrado")
            break
        
        novos_coletados = 0
        for repo in repos:
            full_name = repo.get("full_name", "")
            
            # Verifica se já não foi coletado
            if full_name in collected_names:
                continue
                
            collected_names.add(full_name)
            collected.append({
                "nameWithOwner": full_name,
                "url": repo.get("html_url", ""),
                "description": repo.get("description") or "",
                "stargazerCount": repo.get("stargazers_count", 0),
                "createdAt": repo.get("created_at", ""),
                "pushedAt": repo.get("pushed_at", ""),
                "diskUsage": repo.get("size", 0),
                "forkCount": repo.get("forks_count", 0),
                "releaseCount": 0,
                "primaryLanguage": repo.get("language", "Java")
            })
            
            novos_coletados += 1
            print(f"  [{len(collected)}] Coletado: {full_name} ({repo.get('stargazers_count', 0)} ⭐)")
            
            if len(collected) >= total_to_collect:
                break
        
        print(f"Página {page}: coletados {novos_coletados} novos repositórios")
        
        if novos_coletados == 0:
            print("Nenhum novo repositório nesta página. Verificando próxima...")
        
        page += 1
        
        # Salva checkpoint a cada página
        save_checkpoint(collected)
        time.sleep(3)  # Delay entre páginas
    
    # Salva o resultado final
    save_final_results(collected)
    print(f"\n✅ Coleta finalizada! {len(collected)} repositórios salvos em {arquivo_csv}")

def save_checkpoint(collected):
    """Salva um checkpoint"""
    keys = ["nameWithOwner", "url", "description", "stargazerCount",
            "createdAt", "pushedAt", "diskUsage", "forkCount",
            "releaseCount", "primaryLanguage"]
    
    with open("data/checkpoint.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in collected:
            writer.writerow(row)

def save_final_results(collected):
    """Salva o resultado final"""
    keys = ["nameWithOwner", "url", "description", "stargazerCount",
            "createdAt", "pushedAt", "diskUsage", "forkCount",
            "releaseCount", "primaryLanguage"]
    
    with open("data/top1000_repositorios_java.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in collected:
            writer.writerow(row)

if __name__ == "__main__":
    main()