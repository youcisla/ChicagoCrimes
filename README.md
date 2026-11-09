# ChicagoCrimes

TP Big Data sur le jeu *Crimes - 2001 to Present* de la ville de Chicago : la même analyse écrite en
pandas puis en Spark, avec les temps et la mémoire mesurés sur la machine.

## Contenu

- `TP_Crimes_Chicago.ipynb` : le notebook exécuté, sorties réelles incluses
- `TP_Crimes_Chicago.html` : le même rendu, lisible sans Jupyter
- `Reponses_TP_Crimes.md` : les réponses question par question
- `requirements.txt` : de quoi relancer le notebook

## Données

Le CSV n'est pas dans le dépôt (1,9 Go, ~8,07 M de lignes). Il se télécharge ici :

<https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD>

Posez-le à côté du notebook et pointez `PATH` dessus (ou la variable d'environnement `CRIMES_CSV`).

L'endpoint bulk renvoie parfois `503`. Dans ce cas, l'API resource fonctionne, 50 000 lignes par
requête :

<https://data.cityofchicago.org/resource/ijzp-q8t2.csv?$limit=50000>

Les mesures du notebook ont été prises sur un extrait de 1,13 Go (4 778 681 lignes) tiré de cette
API, mêmes colonnes, mêmes types. Le code ne change pas pour l'export complet.

## Relancer

```
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt
jupyter lab
```

Prérequis : Java 17 pour PySpark 4.
