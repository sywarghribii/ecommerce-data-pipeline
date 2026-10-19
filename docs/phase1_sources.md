# Phase 1 : Connaissance des sources

Dataset : Olist E-Commerce (5 CSV dans `data/raw/`). Exploration avec PySpark dans
`notebooks/01_explore_sources.ipynb`.

## Vue d'ensemble

| Table | Lignes | Une ligne = | Clé |
|---|---|---|---|
| orders | 99 441 | une commande | order_id (unique) |
| order_items | 112 650 | un article d'une commande | (order_id, order_item_id) |
| products | 32 951 | un produit | product_id |
| customers | 99 441 | un client par commande | customer_id |
| payments | 103 886 | un paiement | à vérifier |

## Constats

**orders**
- Statuts : delivered 96 478, shipped 1 107, canceled 625, unavailable 609,
  invoiced 314, processing 301, created 5, approved 2.
- Dates manquantes : 160 (approbation), 1 783 (transporteur), 2 965 (livraison).
  Cohérent avec le métier : une commande non terminée n'a pas toutes ses dates.
- Anomalies : 8 commandes `delivered` sans date de livraison ; 6 commandes `canceled`
  avec une date de livraison.

**order_items**
- Une commande peut contenir plusieurs articles (9 803 commandes en ont plus d'un).
- Joindre avec `orders` multiplie les lignes de `orders`.
- Les montants sont lus en texte (ex. `58.90`).

**products**
- 610 produits sans catégorie, nom, description ni nombre de photos ; 2 sans poids
  ni dimensions.
- La source contient une faute dans les noms de colonnes (`lenght`).

**customers**
- `customer_zip_code_prefix` est un identifiant : lu comme entier, `09790` devient
  `9790`. Il doit rester en texte.

**payments**
- Aucune valeur manquante.

## Intégrité entre tables
- 775 commandes n'ont aucun article : unavailable 603, canceled 164, created 5,
  invoiced 2, shipped 1. Normal pour les statuts unavailable et canceled ;
  le cas `shipped` est une anomalie.
- 0 article sans commande.

## Contrôle Bronze
Nombre de lignes CSV = nombre de lignes Bronze pour les 5 tables.

## Décisions
1. Bronze garde les données brutes, sans correction.
2. Les anomalies sont signalées en Silver, jamais supprimées en silence.
3. Les types sont décidés par écrit en Silver (pas d'`inferSchema`).
4. Les produits sans catégorie sont conservés (catégorie « inconnue »).

## À vérifier
- Grain de `payments` et risque de doublons à la jointure avec `order_items`.
- Min et max des prix et des paiements.
- Cohérence des dates de `orders`.