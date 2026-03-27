import os
import glob
import pandas as pd

CK_OUTPUT_DIR = "ck_outputs"
OUTPUT_FILE = "data/todas_metricas_repositorios.csv"


def main():
    all_files = glob.glob(os.path.join(CK_OUTPUT_DIR, "*class.csv"))
    if not all_files:
        print("nenhum arquivo encontrado em ck_outputs/")
        return

    dfs = []
    for file in all_files:
        repo_name = os.path.basename(file).replace("class.csv", "")
        try:
            df = pd.read_csv(file)
            df["repo_name"] = repo_name
            dfs.append(df)
        except Exception as e:
            print(f"erro lendo {file}: {e}")

    if not dfs:
        print("nenhum dado consolidado")
        return

    merged = pd.concat(dfs, ignore_index=True)
    os.makedirs("data", exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"arquivo consolidado salvo em {OUTPUT_FILE}")

if __name__ == "__main__":
    main()