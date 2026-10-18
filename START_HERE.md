# Installation complète — démarrage rapide

Cette procédure installe la plateforme pédagogique sur une machine neuve. Exécutez les commandes depuis un terminal à la racine du dépôt.

## 1. Prérequis

Installez Git et Docker Compose v2 (Docker Engine sous Linux ou Docker Desktop sous Windows/macOS). Démarrez Docker et utilisez des conteneurs Linux. Prévoyez une connexion Internet, 32 Go de RAM recommandés pour le profil complet et 50 Go d'espace disque libre.

Clonez le dépôt :

```bash
git clone https://github.com/rbizoi/FormationBigDataBI.git
cd FormationBigDataBI
```

Le dépôt comprend déjà `.env`, `ports.env` et les fichiers de données eBrasil. Aucun fichier d'environnement ni volume n'est à créer manuellement.

## 2. Vérifier Docker

```bash
docker context ls
docker context show
docker version
docker compose version
docker info --format '{{.OSType}}'
```

Choisissez le contexte qui correspond au moteur que vous voulez utiliser. Pour Docker Desktop sous Linux, ce contexte est généralement `desktop-linux` :

```bash
docker context use desktop-linux
```

Si vous utilisez Docker Engine Linux, conservez/sélectionnez `default` à la place. Utilisez le même contexte pour toutes les commandes ; les moteurs ont leurs propres conteneurs et volumes.

## 3. Télécharger, construire, vérifier les ports et démarrer

Chargez les deux fichiers d'environnement dans l'ordre indiqué. `ports.env` applique les ports locaux documentés dans le README.

```bash
docker compose --env-file .env --env-file ports.env --profile full --profile seed --profile logs --profile checks pull --ignore-buildable
docker compose --env-file .env --env-file ports.env --profile full --profile seed --profile logs --profile checks build
docker compose --env-file .env --env-file ports.env run --rm ports-check
docker compose --env-file .env --env-file ports.env --profile full up -d --remove-orphans --wait --wait-timeout 900
docker compose --env-file .env --env-file ports.env --profile full ps -a
```

La première construction peut prendre du temps. Les services d'initialisation terminés avec le code 0 sont normaux. Les interfaces sont publiées uniquement sur la machine locale. Ouvrez le portail : [http://localhost:25021](http://localhost:25021).

## 4. Valider l'installation

Suivez la section [Tests d'intégration du README](README.md#5-tests-dintégration) pour lancer les contrôles statiques, les flux d'intégration, Airflow et eBrasil. Les rapports seront écrits dans `reports/`.

Python 3.12 est facultatif pour construire et démarrer avec Docker. Il est nécessaire pour les journaux horodatés des commandes hôte et les tests unitaires locaux. Le [README complet](README.md) documente les comptes, ports, diagnostic Docker Desktop, redémarrage, mise à jour et suppression propre des volumes.
