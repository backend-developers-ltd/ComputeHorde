cd "$(dirname "$0")"

for file in *.puml
do
  plantuml "$file" -tsvg
done