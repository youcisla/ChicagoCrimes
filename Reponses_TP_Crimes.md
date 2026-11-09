# TP Crimes à Chicago — réponses

Données : *Crimes - 2001 to Present* (Chicago Police Department), 22 colonnes, jeu `ijzp-q8t2`
du portail `data.cityofchicago.org`. L'export officiel fait ~8,07 M de lignes pour ~1,9 Go.

Les mesures ci-dessous viennent d'un fichier de 1,13 Go (4 778 681 lignes), extrait du même
export avec les mêmes colonnes. Le code ne change pas pour l'export complet : il suffit de mettre
`PATH` dessus.

Machine : Windows 11, 14 cœurs, 31,5 Go de RAM, Java 17, PySpark 4.3.0, pandas 3.x, Python 3.12.
Tous les temps sortent de `time.perf_counter()`, fichier en cache disque (voir la note de fin).

Un point de méthode avant de commencer. Spark est paresseux : `spark.read.csv()` seul ne prend que
quelques millisecondes, parce qu'il ne lit rien. Chaque « temps de lecture » ci-dessous est donc
lecture + première action (`count()`).

## 1. Lecture avec pandas

| mesure | valeur |
|---|---|
| `pd.read_csv` (moteur par défaut, pandas 3) | 1,66 s |
| `pd.read_csv(..., engine="c")` (pandas 2.x) | 6,38 s |
| lignes × colonnes | 4 778 681 × 22 |
| RAM pic du process | 2,78 Go |
| mémoire du DataFrame (`deep`) | 1,36 Go |

Le fichier entier est matérialisé dans un seul processus : 2,78 Go de RAM pour un DataFrame de
1,36 Go, soit 2,5 fois la taille du fichier. La lecture en elle-même est rapide, et même le vieux
moteur C tient en 6,38 s.

Ce qui coince n'est pas le `read_csv`, c'est la suite. Un filtre, un `groupby`, une jointure
tournent sur un seul thread, et il faut recharger le fichier à chaque exécution. Sur 8 M de lignes
et 1,9 Go, ça devient un choix de moteur plutôt qu'une préférence.

## 2. Lecture avec Spark

| lecture | temps | lignes |
|---|---|---|
| `spark.read.csv(PATH)` (défaut) | 1,93 s | 4 778 682 (données + en-tête) |
| `spark.read.csv(PATH, header=True)` | 0,99 s | 4 778 681 |

Sans option, Spark prend la première ligne pour une donnée : d'où les 4 778 682 lignes au lieu de
4 778 681, les colonnes `_c0 … _c21` et des types tous en `string`. `header=True` règle les deux,
sans coût supplémentaire.

Face à pandas, le temps est du même ordre sur ce volume. La différence est ailleurs : Spark lit
par partitions de 128 Mo, en parallèle, et ne fait jamais transiter le fichier par le driver.

## 3. Partitions, colonnes, lignes

- partitions : 14 (un bloc de 128 Mo = une partition, réglé par `spark.sql.files.maxPartitionBytes`)
- colonnes : 22
- lignes : 4 778 681

## 4. Affichage type pandas

```python
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 5)
```

Évaluer `df` rend alors un tableau HTML avec en-têtes et types, au lieu d'un `DataFrame[...]`.

## 5. `inferSchema=True`

6,91 s contre 0,99 s sans inférence, soit environ 7 fois plus. La cause est simple : Spark doit
parcourir le fichier une seconde fois, uniquement pour deviner le type de chaque colonne, avant la
vraie lecture. Deux passes complètes. Sur l'export de 1,9 Go, c'est le surcoût de plusieurs
dizaines de secondes dont parle l'énoncé.

## 6. Types, sélection, ajout et suppression de colonnes

Types inférés : `ID` int, `Case Number` string, `Date` string, `Arrest`/`Domestic` boolean,
`Beat`/`District`/`Ward`/`Community Area`/`Year` int, `X`/`Y Coordinate` int,
`Latitude`/`Longitude` double, `Updated On` string.

Un piège au passage : `inferSchema` laisse `Date` et `Updated On` en `string`. Il ne devine pas les
formats de dates, et un cast naïf en timestamp les vide. C'est traité en Q8.

```python
df.select("District", "Case Number")
df2 = df.select("District", "Case Number")
df2.withColumn("un", F.lit(1))      # ajout d'une colonne constante
df2.drop("un")                      # suppression
df.drop("ID", "Case Number", "Block")
```

## 7. Affichages

```python
df.select("IUCR").show(5)
df.select("Case Number", "Date", "Arrest").show(5)
df.select("Case Number", "Date", "Arrest", "Primary Type", "Block", "Description") \
  .show(2, truncate=False, vertical=True)
```

## 8. Schéma manuel

`StructType` explicite de 22 `StructField` : `Date`/`Updated On` en `timestamp`, `Arrest`/`Domestic`
en `boolean`, coordonnées en `double`. Les dates arrivent en texte et passent par
`try_to_timestamp` (formats ISO et `MM/DD/YYYY hh:mm:ss AM`).

| lecture | temps |
|---|---|
| `inferSchema=True` | 6,91 s |
| schéma manuel | 0,85 s |

Huit fois plus rapide, et les types sont ceux qu'on a décidés plutôt que ceux devinés.

## 9. `limit()`, `head()`, `take()`, `collect()`

| appel | résultat |
|---|---|
| `df.limit()` | `TypeError: missing 1 required positional argument: 'num'` |
| `df.take()` | `TypeError: missing 1 required positional argument: 'num'` |
| `df.limit(5)` | DataFrame en 6,4 ms, paresseux, 0 task |
| `df.head()` | 1 `Row` en 0,26 s |
| `df.take(5)` | liste de 5 `Row` en 0,12 s |
| `df.collect()` | échec en 22 s : 541,9 Mo de résultats pour 512 Mo autorisés |

`limit()` et `take()` sans argument ne compilent pas, il faut un nombre. `limit(n)` ne déclenche
aucun job. `head()` et `take(n)` rapatrient n lignes, ce qui reste borné.

`collect()` est l'appel qui casse : il ramène toutes les lignes dans le driver. Ici il s'arrête sur
`spark.driver.maxResultSize`, mais sans ce garde-fou il aurait pris la RAM de la machine. On agrège
côté Spark et on ne collecte que du réduit.

## 10. Cache

| opération | temps |
|---|---|
| `df.cache()` + 1er `count()` (matérialisation) | 33,20 s |
| 2e `count()` | 0,39 s |

Gain de 85× sur le second passage. Le `storageLevel` affiché (`Disk Memory Deserialized 1x
Replicated`) dit autre chose d'intéressant : le cache déborde sur disque, les 4 Go du driver ne
suffisent pas pour 4,8 M de lignes.

## 11. `describe()` vs `summary()`

| | temps |
|---|---|
| `describe()` | 39,29 s |
| `summary()` | 203,28 s |

`describe()` renvoie `count`, `mean`, `stddev`, `min` et `max`. `summary()` ajoute les quantiles
25, 50 et 75 % (`percentile_approx`), ce qui explique l'écart. Les deux relancent un scan complet,
donc les enchaîner après un `cache()` est la seule façon de ne pas payer deux fois.

## 12. Valeurs manquantes, valeurs uniques, effectifs par type

Temps : 5,30 s pour les manquantes (une passe, aucun shuffle) et 19,94 s pour les `countDistinct`
(un shuffle par colonne).

| colonne | manquantes | % | uniques |
|---|---|---|---|
| ID | 0 | 0,00% | 4 695 426 |
| Case Number | 0 | 0,00% | 4 695 409 |
| Date | 0 | 0,00% | 1 950 543 |
| Block | 0 | 0,00% | 42 382 |
| IUCR | 0 | 0,00% | 417 |
| Primary Type | 0 | 0,00% | 35 |
| Description | 0 | 0,00% | 554 |
| Location Description | 13 335 | 0,28% | 168 |
| Arrest | 0 | 0,00% | 2 |
| Domestic | 0 | 0,00% | 2 |
| Beat | 0 | 0,00% | 306 |
| District | 10 | 0,00% | 25 |
| Ward | 15 747 | 0,33% | 51 |
| Community Area | 16 421 | 0,34% | 78 |
| FBI Code | 0 | 0,00% | 27 |
| X Coordinate | 139 165 | 2,91% | 73 928 |
| Y Coordinate | 139 165 | 2,91% | 123 536 |
| Year | 0 | 0,00% | 26 |
| Updated On | 0 | 0,00% | 7 471 |
| Latitude | 139 165 | 2,91% | 594 464 |
| Longitude | 139 165 | 2,91% | 594 164 |
| Location | 139 165 | 2,91% | 594 998 |

Peu de trous, sauf sur la géolocalisation : 2,91 % des faits n'ont ni coordonnées XY ni latitude,
et ce sont les mêmes 139 165 lignes d'une colonne à l'autre. `Ward` et `Community Area` manquent
sur 0,3 % des enregistrements.

Top 10 des types de crime sur les 4 778 681 faits :

| # | Primary Type | effectif | % |
|---|---|---|---|
| 1 | THEFT | 1 044 026 | 21,85 % |
| 2 | BATTERY | 856 076 | 17,91 % |
| 3 | CRIMINAL DAMAGE | 521 203 | 10,91 % |
| 4 | NARCOTICS | 450 421 | 9,43 % |
| 5 | ASSAULT | 304 841 | 6,38 % |
| 6 | OTHER OFFENSE | 295 000 | 6,17 % |
| 7 | BURGLARY | 269 230 | 5,63 % |
| 8 | DECEPTIVE PRACTICE | 246 498 | 5,16 % |
| 9 | MOTOR VEHICLE THEFT | 213 950 | 4,48 % |
| 10 | ROBBERY | 178 236 | 3,73 % |

Trois catégories font la moitié des faits : THEFT, BATTERY et CRIMINAL DAMAGE totalisent 50,7 %.

## Ce que je retiens

pandas reste parfait pour ouvrir le fichier une fois et regarder. Dès qu'on enchaîne les traitements
ou qu'on dépasse la RAM, Spark prend le relais sans réécrire le code.

Les deux erreurs que j'ai payées pendant ce TP : `inferSchema=True`, qui ajoute une passe complète
sur tout le fichier, et `collect()`, qui ramène le dataset dans le driver. Les deux se voient dans
les temps ci-dessus.

---

### Note sur la variabilité

Mesures prises fichier en cache disque. À froid, sur une première lecture réelle : `read_csv` 7,7 s,
`header=True` + `count()` 6,7 s, `inferSchema` 21,5 s, soit 4 à 7 fois plus lent, sans changer
aucune conclusion. Sur l'export complet (1,9 Go, ~8,07 M lignes), comptez environ 1,7 fois ces
valeurs.
