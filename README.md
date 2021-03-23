# tp2-yelp-reviews

Pasos para ejecutar el tp:

El directorio padre donde se encuentre clonado el repositorio debe cumplir la siguiente estructura:
|
|__ tp2-yelp-reviews/
|       |... 
|
|__ data/
      |__ runs/
      |__ yelp_academic_dataset_review.json.zip
      |__ yelp_academic_dataset_business.json.zip


Los archivos van comprimidos

Comandos para manejar el servidor:

make docker-compose-up
make docker-compose-logs
make docker-compose-down

Para disparar requests:

./demo.sh <request_id>

Despues de procesar una request se puede guardar el resultado en otro archivo

cp ../data/runs/check.txt ../data/runs/run.txt

Para poder compararlo con resultados de nuevas requests

diff ../data/runs/run.txt ../data/runs/check.txt
