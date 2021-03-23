# tp2-yelp-reviews

Pasos para ejecutar el tp:
<pre>
El directorio padre donde se encuentre clonado el repositorio debe cumplir la siguiente estructura:
|  
|__ tp2-yelp-reviews/  
|       |...   
|
|__ data/  
      |__ runs/  
      |__ yelp_academic_dataset_review.json.zip  
      |__ yelp_academic_dataset_business.json.zip  
</pre>

Los archivos van comprimidos

Comandos para manejar el servidor:
<pre>
make docker-compose-up
make docker-compose-logs
make docker-compose-down
</pre>
Para disparar requests:
<pre>
./demo.sh "request_id"
</pre>
Despues de procesar una request se puede guardar el resultado en otro archivo
<pre>
cp ../data/runs/check.txt ../data/runs/run.txt
</pre>
Para poder compararlo con resultados de nuevas requests
<pre>
diff ../data/runs/run.txt ../data/runs/check.txt
</pre>
