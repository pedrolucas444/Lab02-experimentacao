set -euo pipefail

CK_JAR_PATH="./ck/ck.jar"
INPUT_CSV="data/top1000_repositorios_java.csv"
CLONE_DIR="repos"
OUTPUT_DIR="ck_outputs"
NUM=${1:-1}  # 1 repo 

mkdir -p "${CLONE_DIR}"
mkdir -p "${OUTPUT_DIR}"

tail -n +2 "${INPUT_CSV}" | head -n "${NUM}" | awk -F',' '{print $1}' | while read -r nameWithOwner; do
  owner_repo=$(echo "$nameWithOwner" | tr -d '"')
  repo_url="https://github.com/${owner_repo}.git"
  repo_dir="${CLONE_DIR}/${owner_repo//\//_}"
  echo "cloning ${repo_url} into ${repo_dir}"
  git clone --depth 1 "${repo_url}" "${repo_dir}" || { echo "clone failed for ${repo_url}"; continue; }
  out="${OUTPUT_DIR}/${owner_repo//\//_}"
  mkdir -p "${out}"
  echo "running ck on ${repo_dir} output ${out}"
  java -jar ${CK_JAR_PATH} "${repo_dir}" true 0 false "${out}"
  echo "done ${owner_repo}"
done