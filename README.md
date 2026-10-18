# Formation Big Data et Business Intelligence

<img src="https://raw.githubusercontent.com/rbizoi/FormationBigDataBI/refs/heads/master/images/architecture.png" width="1024">

Plateforme pédagogique Docker Compose pour Windows (Docker Desktop/WSL2), Linux et macOS. Le projet Compose est nommé **bigdata-training**. Le profil **full** démarre les serveurs de formation et le producteur API continu. Les producteurs fichiers, SQL et logs sont des tâches à exécution unique : le test d’intégration les exécute automatiquement ; ils peuvent aussi être lancés séparément avec `run --rm`. Les outils du profil **checks** s’exécutent séparément.

Les données du dépôt sont dans **`donnees/`**. Les chemins internes Spark `/opt/spark/donnees` et `/home/spark/donnees` restent inchangés et accessibles à l'utilisateur `spark`. Airflow les monte en lecture seule. L'initialisation copie les fichiers dans les volumes Spark et dans **`s3://lakehouse/donnees/`**, avec un manifeste SHA-256 `donnees-manifest.json`. Les fichiers de `donnees/db_ebrasil/` sont aussi copiés avec contrôle SHA-256 vers `s3://lakehouse/bronze/ebrasil/` au démarrage ; les modèles silver/Druid sont ensuite initialisés dans l’ordre des dépendances Compose. Les fichiers météo restent disponibles pour les notebooks ; aucun serveur SQL météo spécifique n'est installé.

## 1. Préparation d'une machine neuve

Installer Git, Docker Engine avec le plugin Compose v2 (Linux) ou Docker Desktop (Windows/macOS ; sous Windows, activer l'intégration WSL2). Démarrer Docker et choisir le moteur **Linux containers**. La construction et les tests utilisent des images Linux. Prévoir **32 Go de RAM, 8 CPU et 50 Go d'espace libre** pour l'ensemble de la plateforme ; le cœur seul fonctionne avec moins de ressources. Une connexion Internet est nécessaire au premier pull des images.

Pour les commandes de journalisation et les tests locaux, installer Python 3.12 et les dépendances du dépôt indiquées ci-dessous. Python n'est pas nécessaire aux commandes Docker Compose non enveloppées par `checks/run_logged.py`.

```bash
git clone https://github.com/rbizoi/FormationBigDataBI.git
cd FormationBigDataBI

# Choisir le moteur qui hébergera la formation.
docker context ls
docker context show

# Exécuter UNE seule commande selon le moteur choisi :
# Docker Desktop sous Linux :
docker context use desktop-linux
# Docker Engine Linux (si c'est bien ce moteur que vous utiliserez) :
# docker context use default

# Vérifier le client ET le serveur du même moteur.
docker version
docker compose version
docker info --format '{{.OSType}}'

# Dépendances facultatives pour enregistrer aussi les sorties de commandes
# hôte dans reports/ et lancer les tests unitaires locaux.
python --version
python -m pip install -r requirements-checks.txt
```

Sur Ubuntu/Debian, Python 3 est généralement appelé `python3`. Pour éviter le blocage PEP 668 lors d'une installation globale, créer et activer un environnement virtuel avant d'installer les dépendances :

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-checks.txt
```

Sous Windows, utiliser `py -3.12` à la place de `python` si nécessaire. Pour créer l'environnement virtuel sous Windows :

```powershell
py -3.12 -m venv .venv
.venv\\Scripts\\Activate.ps1
py -m pip install -r requirements-checks.txt
```

Python n'est pas requis pour la construction et le démarrage Docker. Les commandes Compose peuvent être lancées directement ; les contrôles de la section 5 écrivent leurs rapports dans `reports/` depuis leurs conteneurs. L'environnement Python hôte ajoute un journal horodaté autour des commandes et permet les tests unitaires locaux.

Le dépôt contient déjà `.env`, `ports.env` et les huit fichiers Parquet de `donnees/db_ebrasil/` : aucune copie manuelle ni création préalable de volume n'est requise. Les valeurs de `.env` sont des comptes de formation ; le Compose publie les interfaces sur `127.0.0.1`. Pour un serveur partagé, changer les identifiants avant le démarrage et ne pas exposer directement ces ports à Internet.

Toutes les commandes suivantes s'exécutent à la racine du dépôt. **Toujours charger `.env`, puis `ports.env` dans cet ordre** : `.env` contient les paramètres et comptes ; `ports.env` impose les ports hôte cohérents du portail. Modifier un mot de passe dans `.env` ne modifie pas un compte déjà créé dans un volume existant.

## 2. Construction complète et démarrage

```bash
# Vérifier la configuration résolue
python checks/run_logged.py --name compose-config -- docker compose --env-file .env --env-file ports.env --profile full --profile checks config --quiet

# Télécharger les images et construire les images locales, outils de test compris
python checks/run_logged.py --name image-pull -- docker compose --env-file .env --env-file ports.env --profile full --profile seed --profile logs --profile checks pull --ignore-buildable
python checks/run_logged.py --name image-build -- docker compose --env-file .env --env-file ports.env --profile full --profile seed --profile logs --profile checks build

# Vérifier les ports sur le moteur Docker réel
python checks/run_logged.py --name ports-check -- docker compose --env-file .env --env-file ports.env run --rm ports-check

# Démarrer tous les composants et attendre leur disponibilité
python checks/run_logged.py --name stack-start -- docker compose --env-file .env --env-file ports.env --profile full up -d --remove-orphans --wait --wait-timeout 900

# Contrôler les services, y compris les initialisations terminées
python checks/run_logged.py --name services-status -- docker compose --env-file .env --env-file ports.env --profile full ps -a
```

Le contrôle des ports est aussi une dépendance automatique des services qui publient un port. Il détecte les collisions internes et les ports occupés par d'autres conteneurs ou processus. Si un port est occupé, modifier la variable correspondante dans `ports.env`, puis relancer. Les ports internes entre services ne doivent pas être changés pour résoudre une collision sur l'hôte.

Les services d’initialisation sont à exécution unique : **Exited (0)** est normal. Chaque initialisation du profil full est attendue par une dépendance `service_completed_successfully`. Les producteurs fichiers, SQL et logs sont exclus de `up --wait`, car leur sortie normale peut faire échouer cette attente. Pour les serveurs, attendre **running**, et **healthy** lorsqu'un healthcheck est configuré. `--wait` ne prouve pas l'intégration fonctionnelle : exécuter les contrôles de la section 5.

Portail : **http://localhost:25021**. Les comptes et tokens actifs sont affichés dans les cartes du portail ; les identifiants proviennent de `.env`.

## 3. Composants et flux

Sources CSV / JSON / XLSX / PostgreSQL / OpenData / logs → producteurs → Kafka → Spark → Parquet / Delta / Iceberg sur RustFS S3 → Trino → Superset.

Airflow orchestre les jobs Spark. Druid ingère les ventes depuis Kafka et S3 ; PostgreSQL stocke ses métadonnées, ZooKeeper coordonne ses services et RustFS conserve les segments. Superset interroge PostgreSQL, Trino et Druid. Voir [la matrice des intégrations](docs/INTEGRATION_MATRIX.md).

| Ensemble | Services Compose | Rôle |
|---|---|---|
| Portail et ports | `dashboard`, `ports-check` | Interfaces, rapports, détection des collisions |
| PostgreSQL | `postgres-source`, `postgres-bootstrap`, `pgadmin-config`, `pgadmin` | Sources SQL, données initiales, administration |
| Kafka | `kafka`, `kafka-init`, `kafka-ui` | Broker, topics avec leaders vérifiés, consultation |
| Producteurs | `file-producer`, `postgres-producer`, `web-log-producer`, `api-producer`, `mock-opendata` | Fichiers, SQL, logs et API locale vers Kafka |
| Stockage S3 | `objectstore`, `objectstore-permissions`, `objectstore-init` | RustFS, droits, buckets et copie des données |
| Iceberg | `iceberg-rest`, `iceberg-catalog-permissions` | Catalogue REST et stockage des tables |
| Spark | `workspace-init`, `spark-master`, `spark-worker-1`, `spark-worker-2`, `spark-history`, `ebrasil-silver-init` | Droits, calcul distribué, historique S3 |
| Notebooks | `notebooks-init`, `spark-jupyter` | Copie des notebooks et JupyterLab |
| SQL fédéré | `trino` | PostgreSQL et Iceberg / S3 |
| Orchestration | `airflow-db`, `airflow` | Métadonnées et orchestration Spark |
| Druid | `druid-db`, `zookeeper`, `druid-coordinator`, `druid-broker`, `druid-historical`, `druid-middlemanager`, `druid-router`, `ebrasil-druid-init` | Ingestion batch/streaming, modèle eBrasil et analyse OLAP |
| Superset | `superset-db`, `superset-init`, `superset` | Métadonnées, connexions SQL et BI |
| Contrôles | `static-check`, `integration-check`, `airflow-check` | Configuration et intégrations réelles |

Les profils spécialisés restent disponibles : `orchestration` (Airflow), `analytics` (Druid/Superset), `demo` (API locale), `seed` (producteurs fichiers/SQL), `logs` (producteur logs). Les services sans profil constituent le cœur.

## 4. Ports et interfaces

Ports par défaut de `ports.env`, accessibles seulement depuis la machine locale :

| Service | Variable | Port hôte | Port interne |
|---|---|---:|---:|
| `postgres-source` | `POSTGRES_PORT` | 25001 | 5432 |
| `kafka` | `KAFKA_EXTERNAL_PORT` | 25002 | 9092 |
| `objectstore` | `S3_API_PORT` | 25003 | 9000 |
| `objectstore` | `S3_CONSOLE_PORT` | 25004 | 9001 |
| `iceberg-rest` | `ICEBERG_REST_PORT` | 25005 | 8181 |
| `spark-master` | `SPARK_MASTER_UI_PORT` | 25006 | 8080 |
| `spark-worker-1` | `SPARK_WORKER1_UI_PORT` | 25007 | 8081 |
| `spark-worker-2` | `SPARK_WORKER2_UI_PORT` | 25008 | 8081 |
| `spark-history` | `SPARK_HISTORY_UI_PORT` | 25009 | 18080 |
| `spark-jupyter` | `JUPYTER_PORT` | 25010 | 8888 |
| `spark-jupyter` | `SPARK_JOBS_UI_PORT` | 25011 | 4040 |
| `trino` | `TRINO_PORT` | 25012 | 8080 |
| `mock-opendata` | `MOCK_OPENDATA_PORT` | 25013 | 8000 |
| `airflow` | `AIRFLOW_PORT` | 25014 | 8080 |
| `pgadmin` | `PGADMIN_PORT` | 25017 | 80 |
| `kafka-ui` | `KAFKA_UI_PORT` | 25018 | 8080 |
| `druid-router` | `DRUID_PORT` | 25019 | 8888 |
| `superset` | `SUPERSET_PORT` | 25020 | 8088 |
| `dashboard` | `DASHBOARD_PORT` | 25021 | 8090 |

Les interfaces les plus utilisées : [Jupyter](http://localhost:25010), [pgAdmin](http://localhost:25017), [Kafka UI](http://localhost:25018), [RustFS](http://localhost:25004), [Trino](http://localhost:25012), [Airflow](http://localhost:25014), [Druid](http://localhost:25019), [Superset](http://localhost:25020). Le port 25011 correspond à l'interface d'une SparkSession Jupyter active et n'est pas un serveur permanent.

## 5. Tests d'intégration

Après le démarrage complet :

```bash
# Contrats statiques : chemins, dépendances, données et syntaxe
# Contrôle du modèle eBrasil : bronze RustFS, silver Spark et datasources Druid
python checks/run_logged.py --name ebrasil-check -- docker compose --env-file .env --env-file ports.env --profile full --profile checks run --rm ebrasil-check

python checks/run_logged.py --name static-check -- docker compose --env-file .env --env-file ports.env --profile checks run --rm static-check

# Flux de tous les composants de formation
python checks/run_logged.py --name integration-check -- docker compose --env-file .env --env-file ports.env --profile checks run --rm integration-check --full

# Chargement des DAGs et pilote Airflow vers les workers Spark
python checks/run_logged.py --name airflow-check -- docker compose --env-file .env --env-file ports.env --profile checks run --rm airflow-check

# Tests unitaires Python
python checks/run_logged.py --name unit-tests -- python -m pytest -q tests
```

Ces contrôles doivent retourner **0**. Le contrôle complet exige les interfaces du cœur et du profil full. Si une interface ne répond pas, il écrit un rapport FAIL avant de lancer les pipelines. Il vérifie ensuite :

- Les producteurs fichiers, SQL et logs vers Kafka, puis un aller-retour producteur/consommateur.
- La copie SHA-256 de `donnees` vers S3, les accès de Spark, et la lecture/écriture exacte d'un fichier S3.
- Les traitements distribués Spark vers Parquet, Delta et Iceberg ; les comptes, sommes et identifiants des sources existantes.
- Trino vers PostgreSQL et Iceberg/S3, y compris une jointure fédérée.
- Les journaux Spark dans S3 et leur consultation par History Server.
- L'API OpenData locale vers Kafka, les ingestions S3 et Kafka vers Druid, puis les segments Druid dans S3.
- Les vraies requêtes SQL Lab Superset vers PostgreSQL, Trino et Druid.

Le contrôle Airflow importe les DAGs et lance des jobs avec le pilote de son image vers les workers. Il ne simule pas une exécution complète déclenchée par le scheduler. Les contrôles HTTP des interfaces ne simulent pas une session graphique.

Tous les fichiers générés par les contrôles et tests Docker sont écrits dans le répertoire hôte `reports/` (fichiers JSON et logs Spark). Le dossier est partagé en écriture par les services Docker concernés ; ses fichiers générés sont ignorés par Git, seul `.gitkeep` reste suivi. Les tests réutilisent les données existantes, remplacent certaines sorties pédagogiques et laissent des tables et un supervisor Druid.

Pour le cœur seulement, démarrer sans `--profile full` et exécuter `integration-check` sans `--full`. Le contrôle Airflow nécessite son profil. Les logs des workflows GitHub sont aussi téléchargeables comme artefacts `*-reports` pendant 14 jours (30 jours pour l'intégration complète).

### Exécuter les producteurs ponctuels séparément

Le contrôle d’intégration les lance automatiquement. Pour un exercice manuel, après le démarrage des serveurs :

```bash
docker compose --env-file .env --env-file ports.env --profile seed run --rm file-producer
docker compose --env-file .env --env-file ports.env --profile seed run --rm postgres-producer
docker compose --env-file .env --env-file ports.env --profile logs run --rm web-log-producer
```

Une sortie `0` signifie que la tâche a réussi. Ne pas ajouter `--profile seed` ou `--profile logs` à `up --wait` : ces tâches terminent normalement au lieu de rester actives.

## Modèle dimensionnel eBrasil

Au démarrage du profil `full` (ou `analytics`), trois étapes s’exécutent dans l’ordre. La commande dédiée de vérification eBrasil active aussi les dépendances nécessaires via `--profile checks` :

1. `objectstore-init` copie les huit fichiers Parquet de `donnees/db_ebrasil/` vers `s3://lakehouse/bronze/ebrasil/`. Les octets et les empreintes SHA-256 sont contrôlés ; le manifeste source est enregistré dans le préfixe.
2. `ebrasil-silver-init` lance Spark et construit les dimensions conformes et les tables de faits Parquet dans `s3://lakehouse/silver/ebrasil/`. Chaque table dispose aussi d’une sortie JSON dédiée à l’ingestion Druid.
3. `ebrasil-druid-init` ingère les dimensions et les faits en datasources Druid. Les tâches sont séquentielles et l’initialisation attend leur succès.

Le modèle suit les six étoiles du document de modélisation : ventes (`fact_order_item`), commandes (`fact_order`), finance (`fact_payment`), logistique (`fact_logistics`), satisfaction (`fact_review`) et CRM/RFM (`mart_customer_rfm`). Les dimensions conformes comprennent date, heure, client, produit, vendeur, géographie, statut de commande et type de paiement. La dimension Date expose année, semestre, trimestre, mois, semaine ISO et jour ; la dimension Temps expose partie du jour, heure, minute et seconde. `order_id` reste présent dans les faits pour le drill-through.

Les fichiers déjà préparés agrègent les paiements par commande et pivotent les avis par commande. Le traitement conserve ces grains disponibles : Finance est au niveau commande × type de paiement, et Satisfaction conserve le nombre de notes par score au niveau commande. Le script ne recrée pas de séquences de paiement ou d’avis individuels que les fichiers Parquet ne contiennent plus. Le calcul de distance utilise les coordonnées de géolocalisation fournies ; lorsqu’elles sont absentes, la distance reste nulle.

Les sorties de vérification sont consultables dans `reports/`. Pour relancer les contrôles après le démarrage :

```bash
docker compose --env-file .env --env-file ports.env --profile full --profile checks run --rm ebrasil-check
```

Ce contrôle compare les huit fichiers bronze aux empreintes source, vérifie la présence des tables Parquet silver, puis compare le nombre de lignes de chaque table aux datasources Druid correspondantes. Le rapport horodaté est écrit dans `reports/`.

## 6. Arrêter, redémarrer, reconstruire

```bash
# Arrêter tous les serveurs en conservant conteneurs et données
docker compose --env-file .env --env-file ports.env --profile full stop

# Redémarrer les conteneurs existants sans reconstruire les images
docker compose --env-file .env --env-file ports.env --profile full start

# Relancer après un changement de configuration ou de code
docker compose --env-file .env --env-file ports.env --profile full --profile seed --profile logs --profile checks build
docker compose --env-file .env --env-file ports.env --profile full up -d --remove-orphans --wait --wait-timeout 900

# Redémarrer un serveur, par exemple Trino
docker compose --env-file .env --env-file ports.env restart trino

# Arrêter et supprimer conteneurs et réseau ; conserver les volumes
docker compose --env-file .env --env-file ports.env --profile '*' down --remove-orphans
```

`start` reprend les conteneurs existants. `up` crée les conteneurs manquants et applique la configuration actuelle. Les services d'initialisation sont à exécution unique ; consulter leur code de sortie après une relance.

## 7. Effacement complet de cette installation

**Ces commandes effacent les données des volumes de formation**, notamment les bases SQL, Kafka, S3, les notebooks modifiés dans Jupyter et les rapports. Sauvegarder ce qui doit être conservé. Le répertoire hôte `donnees/` et les fichiers Git restent présents.

Pour supprimer les conteneurs, le réseau, les volumes déclarés et les images de la configuration actuelle :

```bash
docker compose --env-file .env --env-file ports.env --profile '*' down --remove-orphans --volumes --rmi all
```

Pour inclure les anciens volumes de ce projet laissés par une version précédente, utiliser le script Python (Python 3 sur l'hôte) :

```bash
# Inventaire sans suppression
python tools/cleanup_project.py

# Supprimer les ressources de ce projet, y compris ses anciens volumes
python tools/cleanup_project.py --execute
```

Le script sélectionne conteneurs et volumes par le label Compose du projet, récupère les images utilisées avant de supprimer les conteneurs, puis retire les images sans forcer leur suppression. Les images encore référencées par d'autres conteneurs sont conservées et signalées. Il n'utilise aucun `docker system prune`, `volume prune` ou suppression globale. Il ne vide pas le cache de construction partagé de Docker.

Après effacement, reprendre la construction et le démarrage de la section 2, puis tous les tests de la section 5 pour contrôler une installation vierge des étudiants.

## 8. Mettre à jour une ancienne installation

Si le dépôt contient des modifications locales, les sauvegarder avant `git pull`.

```bash
docker compose --env-file .env --env-file ports.env --profile '*' down --remove-orphans
git pull --ff-only
docker compose --env-file .env --env-file ports.env --profile full --profile seed --profile logs --profile checks build
docker compose --env-file .env --env-file ports.env --profile full up -d --remove-orphans --wait --wait-timeout 900
```

Git renomme les fichiers suivis de `data` vers `donnees`. Déplacer manuellement vers `donnees` les fichiers personnels non suivis restés dans l'ancien répertoire. Les volumes persistants ne sont pas effacés par cette mise à jour ; le script de nettoyage de la section 7 permet une réinstallation vierge. Dans pgAdmin, une ancienne connexion enregistrée dans son volume peut rester visible : la supprimer dans l'interface si elle n'est plus utilisée.

## 9. Diagnostic

```bash
docker compose --env-file .env --env-file ports.env --profile full ps -a
docker compose --env-file .env --env-file ports.env --profile full logs --tail=150 trino kafka airflow druid-router superset
docker compose --env-file .env --env-file ports.env --profile full exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe
docker compose --env-file .env --env-file ports.env --profile full run --rm kafka-init
```

Un leader Kafka `-1` ou un timeout de livraison impose d'examiner les logs, la mémoire et l'espace disque du broker. Un refus de connexion vers `trino:8080` concerne le serveur interne, pas son port hôte. Aucun contrôle ne supprime automatiquement les données pour tenter de réparer un serveur.

### Docker Desktop renvoie une erreur API 500

Vérifier d'abord le contexte et les versions du client et du serveur :

```bash
docker context show
docker version
docker compose version
docker --context desktop-linux ps -a
```

Sur Linux avec Docker Desktop, le contexte normalement utilisé est `desktop-linux`. N'alterner pas de contexte pendant une installation : chaque moteur possède ses propres conteneurs, images et volumes. Une erreur 500 sur `/v1.xx/containers/.../json` signifie que le serveur Docker n'a pas répondu correctement à l'inspection ; le suffixe `/json` fait partie de l'API HTTP. Avec la commande Docker, inspecter uniquement le nom ou l'identifiant :

```bash
docker --context desktop-linux inspect NOM_OU_ID_DU_CONTENEUR
```

Si Docker Desktop renvoie encore 500, le redémarrer depuis **Troubleshoot → Restart**, puis vérifier de nouveau `docker version` et `docker context show`. Un message `no such object` signifie que le conteneur n'existe plus dans ce moteur ; les conteneurs lancés avec `--rm` sont supprimés après leur exécution. Cette erreur API ne permet pas à elle seule de conclure à un échec Spark ou à un manque de mémoire.

Les contrôles statiques/unitaires et les workflows Docker du dépôt fournissent des validations distinctes. Le workflow manuel **Full integration** exécute la plateforme et les deux suites fonctionnelles sur un runner Docker suffisamment dimensionné ; un succès statique seul ne certifie pas une installation étudiante.
