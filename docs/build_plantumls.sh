cd "$(dirname "$0")"

for file in *.puml.txt
do
  plantuml "$file" -tsvg
done