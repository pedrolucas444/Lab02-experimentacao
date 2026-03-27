import os
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

sns.set(style="whitegrid")
plt.rcParams["figure.figsize"] = (10, 6)

# Arquivos de entrada
CK_ALL_CSV = "data/todas_metricas_repositorios.csv"  
TOP1000_CSV = "data/top1000_repositorios_java.csv"  

# Diretório e arquivos de saída
RESULTS_DIR = "resultados"  # antes: results
REPO_LEVEL_CSV = os.path.join(RESULTS_DIR, "metricas_por_repositorio.csv")  
STATS_SUMMARY_CSV = os.path.join(RESULTS_DIR, "resumo_estatisticas.csv")  
CORR_TESTS_CSV = os.path.join(RESULTS_DIR, "testes_correlacao.csv")  


def normalize_repo_name(name):
    """Normaliza o nome do repositório para uso como chave"""
    if pd.isna(name):
        return ""
    n = str(name).strip()
    n = n.replace("/", "_")
    n = re.sub(r'[\'"]', "", n)
    return n

def compute_age_years(created_at):
    """Calcula a idade do repositório em anos a partir da data de criação"""
    try:
        dt = pd.to_datetime(created_at, utc=True)
        now = pd.Timestamp.now(tz="UTC")
        delta = now - dt
        return delta.days / 365.25
    except Exception:
        return np.nan

def main():
    # Cria diretório de resultados se não existir
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    # Carrega os dados de métricas das classes e metadados dos repositórios
    df_classes = pd.read_csv(CK_ALL_CSV, low_memory=False)
    df_top = pd.read_csv(TOP1000_CSV, low_memory=False)

    # Padroniza os nomes das colunas de metadados para garantir compatibilidade
    rename_map = {}
    for c in df_top.columns:
        lc = c.lower()
        if lc == "namewithowner" or lc == "name_with_owner":
            rename_map[c] = "nameWithOwner"
        elif lc == "stargazercount" or lc == "stars" or lc == "stargazers" or lc == "stargazer_count":
            rename_map[c] = "stargazerCount"
        elif lc == "createdat" or lc == "created_at":
            rename_map[c] = "createdAt"
        elif lc == "pushedat" or lc == "pushed_at" or lc == "updatedat" or lc == "updated_at":
            rename_map[c] = "pushedAt"
        elif lc == "diskusage" or lc == "disk_usage":
            rename_map[c] = "diskUsage"
        elif lc == "forkcount" or lc == "fork_count" or lc == "forks":
            rename_map[c] = "forkCount"
        elif lc == "releases" or lc == "releasecount" or lc == "releases_totalcount":
            rename_map[c] = "releaseCount"
    if rename_map:
        df_top = df_top.rename(columns=rename_map)

    # Identifica a coluna de nome do repositório nos metadados
    name_cols = [c for c in df_top.columns if c.lower() in ("namewithowner", "name_with_owner", "name", "namewithowner") or c == "nameWithOwner"]
    repo_col_top = name_cols[0] if len(name_cols) >= 1 else df_top.columns[0]
    df_top["repo_key"] = df_top[repo_col_top].apply(normalize_repo_name)

    # Extrai a chave do repositório a partir dos dados do CK
    if "repo_name" in df_classes.columns:
        df_classes["repo_key"] = df_classes["repo_name"].apply(normalize_repo_name)
    else:
        if "filename" in df_classes.columns:
            df_classes["repo_key"] = df_classes["filename"].apply(lambda x: normalize_repo_name(x.split("/")[0]) if isinstance(x, str) else "")
        else:
            df_classes["repo_key"] = ""

    # Mapeia os nomes das colunas de métricas (considerando variações de nomenclatura)
    lower_cols = {c.lower(): c for c in df_classes.columns}
    def col_name(preferred):
        return lower_cols.get(preferred.lower(), None)

    cols_map = {
        "cbo": col_name("cbo") or "cbo",
        "dit": col_name("dit") or "dit",
        "lcom": col_name("lcom") or "lcom",
        "loc": col_name("loc") or "loc",
        "wmc": col_name("wmc") or "wmc"
    }

    # Garante que todas as colunas de métricas existam
    for k, v in cols_map.items():
        if v not in df_classes.columns:
            df_classes[v] = np.nan

    # Converte colunas para numérico
    for v in cols_map.values():
        df_classes[v] = pd.to_numeric(df_classes[v], errors="coerce")

    # Define as funções de agregação para cada métrica
    agg_funcs = {
        cols_map["cbo"]: ["mean", "median", "std"],
        cols_map["dit"]: ["mean", "median", "std"],
        cols_map["lcom"]: ["mean", "median", "std"],
        cols_map["loc"]: ["sum", "mean", "median"],
        cols_map["wmc"]: ["mean", "median"]
    }

    # Agrega as métricas por repositório
    df_agg = df_classes.groupby("repo_key").agg(agg_funcs)
    df_agg.columns = ["_".join(filter(None, map(str, col))).strip() for col in df_agg.columns.values]
    df_agg = df_agg.reset_index().rename(columns={"repo_key": "repo_key"})
    
    # Adiciona contagem de classes por repositório
    class_counts = df_classes.groupby("repo_key").size().reset_index(name="num_classes")
    df_agg = df_agg.merge(class_counts, on="repo_key", how="left")

    # Constrói um dataframe de metadados normalizado com colunas canônicas
    df_top_small_cols = {
        "url": next((c for c in df_top.columns if c.lower() == "url"), None),
        "stars": "stargazerCount" if "stargazerCount" in df_top.columns else (next((c for c in df_top.columns if "star" in c.lower()), None)),
        "created_at": "createdAt" if "createdAt" in df_top.columns else (next((c for c in df_top.columns if "created" in c.lower()), None)),
        "pushed_at": "pushedAt" if "pushedAt" in df_top.columns else (next((c for c in df_top.columns if ("pushed" in c.lower() or "updated" in c.lower())), None)),
        "disk_usage": "diskUsage" if "diskUsage" in df_top.columns else (next((c for c in df_top.columns if "disk" in c.lower()), None)),
        "forks": "forkCount" if "forkCount" in df_top.columns else (next((c for c in df_top.columns if "fork" in c.lower()), None)),
        "releases": "releaseCount" if "releaseCount" in df_top.columns else (next((c for c in df_top.columns if "release" in c.lower()), None)),
    }
    
    # Seleciona apenas as colunas que existem
    keep_cols = [c for c in df_top_small_cols.values() if c is not None]
    df_top_small = df_top[["repo_key"] + keep_cols].copy()

    # Renomeia para nomes canônicos
    rename_canonical = {v: k for k, v in df_top_small_cols.items() if v is not None}
    df_top_small = df_top_small.rename(columns=rename_canonical)

    # Calcula idade do repositório
    if "created_at" in df_top_small.columns:
        df_top_small["age_years"] = df_top_small["created_at"].apply(compute_age_years)
    else:
        df_top_small["age_years"] = np.nan

    # Calcula dias desde o último push (atividade)
    if "pushed_at" in df_top_small.columns:
        try:
            pushed_dt = pd.to_datetime(df_top_small["pushed_at"], utc=True)
            now_utc = pd.Timestamp.now(tz="UTC")
            df_top_small["days_since_push"] = (now_utc - pushed_dt) / np.timedelta64(1, 'D')
        except Exception:
            df_top_small["days_since_push"] = np.nan
    else:
        df_top_small["days_since_push"] = np.nan

    # Converte tipos numéricos
    if "stars" in df_top_small.columns:
        df_top_small["stars"] = pd.to_numeric(df_top_small["stars"], errors="coerce")
    else:
        df_top_small["stars"] = np.nan
        
    if "releases" in df_top_small.columns:
        df_top_small["releases"] = pd.to_numeric(df_top_small["releases"], errors="coerce")

    # Combina métricas agregadas com metadados
    df_repo = df_agg.merge(df_top_small, on="repo_key", how="left")
    df_repo.to_csv(REPO_LEVEL_CSV, index=False)
    
    # Gera estatísticas descritivas
    stats = df_repo.describe(include=[np.number]).transpose()
    stats.to_csv(STATS_SUMMARY_CSV)

    # RQ01: Popularidade vs Qualidade (Stars vs LCOM)
    if "stars" in df_repo.columns and df_repo["stars"].notna().sum() > 0 and (cols_map["lcom"] + "_median") in df_repo.columns:
        plt.figure()
        sns.regplot(x="stars", y=cols_map["lcom"] + "_median", 
                    data=df_repo.dropna(subset=["stars", cols_map["lcom"] + "_median"]), 
                    scatter_kws={"s": 20}, lowess=True)
        plt.xscale("log")
        plt.xlabel("Stars (log)")
        plt.ylabel("LCOM (mediana)")
        plt.title("Relação entre Stars e LCOM")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "plot_scatter_stars_vs_lcom.png"))
        plt.close()

    # RQ02: Maturidade vs Qualidade (Idade vs CBO)
    cbo_med_col = cols_map["cbo"] + "_median"
    if "age_years" in df_repo.columns and cbo_med_col in df_repo.columns:
        plt.figure()
        sns.regplot(x="age_years", y=cbo_med_col, 
                    data=df_repo.dropna(subset=["age_years", cbo_med_col]), 
                    scatter_kws={"s": 20}, lowess=True)
        plt.xlabel("Idade do Repositório (anos)")
        plt.ylabel("CBO (mediana)")
        plt.title("Relação entre Idade e CBO")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "plot_scatter_age_vs_cbo.png"))
        plt.close()

    # RQ03: Atividade vs Qualidade (Releases vs LCOM, com fallback para recência de push)
    lcom_med_col = cols_map["lcom"] + "_median"
    if "releases" in df_repo.columns and df_repo["releases"].notna().sum() > 0 and lcom_med_col in df_repo.columns:
        plt.figure()
        sns.regplot(x="releases", y=lcom_med_col, 
                    data=df_repo.dropna(subset=["releases", lcom_med_col]), 
                    scatter_kws={"s": 20})
        plt.xlabel("Número de Releases")
        plt.ylabel("LCOM (mediana)")
        plt.title("Relação entre Atividade (releases) e LCOM")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "plot_scatter_releases_vs_lcom.png"))
        plt.close()
    elif "days_since_push" in df_repo.columns and lcom_med_col in df_repo.columns:
        plt.figure()
        sns.regplot(x="days_since_push", y=lcom_med_col, 
                    data=df_repo.dropna(subset=["days_since_push", lcom_med_col]), 
                    scatter_kws={"s": 20})
        plt.xlabel("Dias desde o último push")
        plt.ylabel("LCOM (mediana)")
        plt.title("Relação entre Recência de Push e LCOM")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "plot_scatter_days_since_push_vs_lcom.png"))
        plt.close()

    # RQ04: Tamanho vs Qualidade (LOC vs DIT)
    loc_sum_col = cols_map["loc"] + "_sum"
    dit_med_col = cols_map["dit"] + "_median"
    if loc_sum_col in df_repo.columns and dit_med_col in df_repo.columns:
        plt.figure()
        sns.regplot(x=loc_sum_col, y=dit_med_col, 
                    data=df_repo.dropna(subset=[loc_sum_col, dit_med_col]), 
                    scatter_kws={"s": 20})
        plt.xscale("log")
        plt.xlabel("LOC (soma, log)")
        plt.ylabel("DIT (mediana)")
        plt.title("Relação entre LOC e DIT")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "plot_scatter_loc_vs_dit.png"))
        plt.close()

    # Salva testes de correlação para os pares chave (Spearman e Pearson)
    try:
        from scipy.stats import spearmanr, pearsonr
        tests = []
        
        def add_test(x_col, y_col, label):
            """Adiciona teste de correlação para um par de variáveis"""
            sub = df_repo[[x_col, y_col]].dropna()
            if len(sub) >= 3 and sub[x_col].nunique() > 1 and sub[y_col].nunique() > 1:
                s_r, s_p = spearmanr(sub[x_col], sub[y_col])
                try:
                    p_r, p_p = pearsonr(sub[x_col], sub[y_col])
                except Exception:
                    p_r, p_p = (np.nan, np.nan)
                tests.append({
                    "pair": label,
                    "n": len(sub),
                    "spearman_r": s_r,
                    "spearman_p": s_p,
                    "pearson_r": p_r,
                    "pearson_p": p_p,
                })

        # Adiciona testes para cada RQ
        if "stars" in df_repo.columns and lcom_med_col in df_repo.columns:
            add_test("stars", lcom_med_col, "Stars vs LCOM_median")
            
        if "age_years" in df_repo.columns and cbo_med_col in df_repo.columns:
            add_test("age_years", cbo_med_col, "AgeYears vs CBO_median")
            
        if "releases" in df_repo.columns and lcom_med_col in df_repo.columns:
            add_test("releases", lcom_med_col, "Releases vs LCOM_median")
        elif "days_since_push" in df_repo.columns and lcom_med_col in df_repo.columns:
            add_test("days_since_push", lcom_med_col, "DaysSincePush vs LCOM_median")
            
        if loc_sum_col in df_repo.columns and dit_med_col in df_repo.columns:
            add_test(loc_sum_col, dit_med_col, "LOC_sum vs DIT_median")

        # Salva os resultados dos testes
        if tests:
            pd.DataFrame(tests).to_csv(CORR_TESTS_CSV, index=False)
    except Exception:
        pass

    # Gera mapa de correlação de Spearman para colunas numéricas
    num_cols = df_repo.select_dtypes(include=[np.number])
    if not num_cols.empty and num_cols.shape[1] > 1:
        corr = num_cols.corr(method="spearman")
        plt.figure(figsize=(12, 10))
        sns.heatmap(corr, annot=True, fmt=".2f", cmap="RdBu_r", center=0, linewidths=0.3, linecolor="white")
        plt.title("Mapa de Correlação (Spearman)")
        plt.tight_layout()
        plt.savefig(os.path.join(RESULTS_DIR, "plot_heatmap_correlations.png"))
        plt.close()

if __name__ == "__main__":
    main()