# Partie A – Questions A.3 et A.4 (couche Kafka)

## A.3 – Partitions et clé de chaque topic

| Topic | Partitions | Clé | Justification |
|---|---|---|---|
| vehicle_events | 6 | vehicle_id | Kafka ne garantit l'ordre qu'à l'intérieur d'une partition. Même clé = même partition = ordre préservé par véhicule. 6 partitions permettent 6 lectures parallèles (Spark lit une partition par tâche) sans surcharge excessive pour 40 véhicules. |
| fleet_catalog | 3 | vehicle_id | La clé est obligatoire pour la compaction (Kafka garde la dernière valeur par clé). 40 enregistrements seulement : peu de partitions suffisent. |
| alerts | 3 | vehicle_id (à confirmer) | Les alertes d'un même véhicule restent dans l'ordre. 3 partitions permettent jusqu'à 3 notifiers actifs ; le volume est faible. |
| region_kpis_1min | 3 | région (à confirmer) | En mode update, une même fenêtre est republiée plusieurs fois : la clé région garde ces mises à jour dans l'ordre. |
| vehicle_events_dlq | 3 | vehicle_id si lisible, sinon aucune | Les messages corrompus n'ont pas toujours de vehicle_id ; leur ordre n'a pas d'importance. |
| daily_reports | 1 | date du rapport | Un rapport par jour : une partition donne un ordre total, aucun besoin de parallélisme. |

### Mesures sur notre broker

**Mesure 1 – simulateur pendant 15 s (321 messages) :** chacun des 40 véhicules se trouve dans une seule partition.

| Partition | Véhicules | Messages |
|---|---|---|
| 0 | 7 | 50 |
| 1 | 4 | 26 |
| 2 | 8 | 51 |
| 3 | 8 | 74 |
| 4 | 5 | 52 |
| 5 | 8 | 68 |

La répartition est inégale (4 à 8 véhicules par partition, 26 à 74 messages) même sans véhicule « chaud » : 40 clés hachées sur 6 partitions ne se répartissent jamais parfaitement. Conséquence pour Spark : certaines tâches de lecture auront plus de données que d'autres.

**Mesure 2 – 30 messages d'un même véhicule sur un topic temporaire à 6 partitions :**
- clé aléatoire : messages éparpillés sur les 6 partitions (3, 4, 6, 3, 9 et 5 messages) ;
- clé = V0001 : les 30 messages sont dans la partition 2, la même que lors de la mesure 1 (le hachage de la clé est déterministe).

### Que se passe-t-il si vehicle_events est clé au hasard ?
Les événements d'un même véhicule sont répartis sur plusieurs partitions (mesure 2). Chaque partition étant lue indépendamment, l'ordre d'arrivée par véhicule n'est plus garanti pour un consommateur qui suit l'état d'un véhicule. Nuance : Spark travaille en temps d'événement et refait de toute façon un redécoupage (shuffle) pour ses agrégations, donc il est peu gêné ; l'ordre par véhicule compte surtout pour un consommateur simple. Inconvénient de la clé par véhicule : avec --hot-vehicle, un véhicule concentre 40 % du trafic sur une partition (skew, étudié en partie C).

### Que se passe-t-il avec une seule partition ?
Il n'y a plus de parallélisme : au plus un consommateur par groupe est actif, un second notifier resterait inactif, Spark lirait avec une seule tâche, et le débit est limité à une partition. Ajouter des partitions plus tard change aussi la correspondance clé/partition.

### Limite de notre installation
Replication factor = 1 et un seul broker : une seule copie de chaque partition, donc aucune tolérance à la panne du broker. En production : 3 copies.

## A.4 – Garanties de livraison par maillon

| Maillon | Garantie | Justification |
|---|---|---|
| Producteur → broker | Au moins une fois, sans doublon de retry | Producteur idempotent : le broker attribue un identifiant (PID) et numérote les messages par partition, donc un renvoi après timeout est ignoré. acks=all attend toutes les copies synchronisées (ici une seule). Limite : après redémarrage le producteur a un nouveau PID, pas de déduplication entre deux exécutions (d'où le run id dans event_id). |
| Broker → Spark | Au moins une fois | Spark note les offsets lus dans son checkpoint ; après un crash il rejoue le dernier lot non terminé. |
| Spark → alerts | Au moins une fois | Si Spark plante après avoir publié un lot mais avant de le valider, le lot est rejoué : alertes en double possibles (voir B.4). Atténuation : alert_id déterministe. |
| alerts → notifier | Au moins une fois | Commit manuel après traitement : un crash entre les deux fait retraiter l'alerte. Committer avant ferait perdre des alertes. Le traitement doit être idempotent. |

## À vérifier avec le code final
- clés réelles de alerts et region_kpis_1min (choisies dans le job Spark) ;
- présence d'un alert_id dans les alertes ;
- stratégie de commit finale du notifier.
