import os
import csv
import requests
import time

GITHUB_API_REST = "https://api.github.com"

# Seu token

headers = {
    "Authorization": f"token {TOKEN}",
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
            # Verifica rate limit
            remaining = r.headers.get('X-RateLimit-Remaining')
            print(f"Rate limit restante: {remaining}")
            if remaining == '0':
                reset_time = int(r.headers.get('X-RateLimit-Reset', time.time() + 60))
                wait_time = reset_time - time.time()
                if wait_time > 0:
                    print(f"Aguardando {wait_time:.0f} segundos...")
                    time.sleep(wait_time + 5)
            return None
            
        if r.status_code != 200:
            print(f"Erro: {r.status_code}")
            return None
            
        return r.json()
        
    except Exception as e:
        print(f"Erro na requisição: {e}")
        return None

def main():
    os.makedirs("data", exist_ok=True)
    
    collected = []
    total_to_collect = 1000
    page = 1
    per_page = 100
    
    print("Iniciando coleta dos top 1000 repositórios Java via API REST...")
    
    while len(collected) < total_to_collect and page <= 10:
        print(f"\n--- Página {page} | Coletados: {len(collected)}/{total_to_collect} ---")
        
        result = search_repos(page, per_page)
        
        if not result or "items" not in result:
            print("Erro ou fim dos resultados")
            break
            
        repos = result["items"]
        
        if not repos:
            print("Nenhum repositório encontrado")
            break
        
        for repo in repos:
            full_name = repo.get("full_name", "")
            collected.append({
                "nameWithOwner": full_name,
                "url": repo.get("html_url", ""),
                "description": repo.get("description") or "",
                "stargazerCount": repo.get("stargazers_count", 0),
                "createdAt": repo.get("created_at", ""),
                "pushedAt": repo.get("pushed_at", ""),
                "diskUsage": repo.get("size", 0),
                "forkCount": repo.get("forks_count", 0),
                "releaseCount": 0,  # API REST não retorna releases facilmente
                "primaryLanguage": repo.get("language", "Java")
            })
            
            print(f"  Coletado: {full_name} ({repo.get('stargazers_count', 0)} ⭐)")
            
            if len(collected) >= total_to_collect:
                break
        
        page += 1
        time.sleep(2)  # Delay entre páginas
    
    # Salva resultados
    keys = [
        "nameWithOwner", "url", "description", "stargazerCount",
        "createdAt", "pushedAt", "diskUsage", "forkCount",
        "releaseCount", "primaryLanguage"
    ]
    
    with open("data/top1000_repositorios_java.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in collected:
            writer.writerow(row)
    
    print(f"\n✅ Coleta finalizada! {len(collected)} repositórios salvos em data/top1000_repositorios_java.csv")

if __name__ == "__main__":
    main()