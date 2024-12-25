# Mini SGBD

Un projet universitaire de création d'un mini système de gestion de bases de données (SGBD) en Java, réalisé entièrement from scratch.

---

## Contributeurs
- **Maëlys Adamczak**
- **Maël Lecene**

---

## Description
Ce projet a été réalisé dans le cadre universitaire pour explorer et comprendre les mécanismes de base d’un SGBD.  
Le projet permet :  
- La gestion de bases de données multiples.  
- La manipulation des tables via des opérations telles que **INSERT**, **BULKINSERT**, et **SELECT** (avec ou sans jointures, incluant des conditions internes ou de jointures).  
- La création et l'utilisation d'index pour améliorer les performances.  

Le SGBD prend en charge les types de données suivants : **CHAR**, **INT**, **REAL**, et **VARCHAR**. Une compatibilité partielle a été développée pour le type **DATE**, mais ce dernier n'est pas utilisable actuellement (à envisager comme piste d’amélioration).

Les requêtes peuvent être exécutées sur plusieurs tables avec tout type de conditions. Une piste d'amélioration consisterait à optimiser l’arbre algébrique existant.

---

## Installation et Compilation

### Prérequis
- **Java** (version 11 ou supérieure) installé sur votre machine.

### Étapes
1. **Télécharger le projet** : Clonez ou téléchargez le projet depuis ce dépôt.
   ```bash
   git clone https://github.com/username/minisgbd.git
   ```
2. **Compilation** : Deux options s'offrent à vous :  
   - **Via le script** : Exécutez `launch.sh`.  
   - **Manuellement** : Compilez le fichier principal directement. Le point d’entrée du programme est dans `src/main/SGBD.java`.

---

## Utilisation

Les requêtes suivent une syntaxe SQL simplifiée. Voici les commandes principales disponibles :

### CREATE TABLE
Créer une table avec des colonnes définies.  
```sql
CREATE TABLE NomTable (NomCol1:TypeCol1, NomCol2:TypeCol2, ..., NomColN:TypeColN(size))
```
**Règles :**
- Les noms et types des colonnes sont séparés par un `:` sans espaces.
- Un espace sépare les mots-clés et les noms de tables.
- Exemple :
  ```sql
  CREATE TABLE Clients (ID:INT, Nom:STRING(50), Age:INT)
  ```

### INSERT
Insérer un tuple dans une table.  
```sql
INSERT INTO NomTable VALUES (val1, val2, ..., valn)
```
**Règles :**
- Les chaînes de caractères doivent être entourées de guillemets (`"valeur"`).
- Exemple :
  ```sql
  INSERT INTO Clients VALUES (1, "Alice", 25)
  ```

### BULKINSERT
Insérer des données à partir d’un fichier CSV.  
```sql
BULKINSERT INTO NomTable nomFichier.csv
```

### SELECT
Récupérer des données avec ou sans conditions.  
```sql
SELECT aliasRel.col1, aliasRel.col2, ... FROM NomTable aliasRel [WHERE condition]
```
**Règles :**
- Les conditions suivent le format `colonne1 OP colonne2`, où `OP` peut être `=`, `<`, `>`, `<=`, `>=`, `<>`.
- Exemple avec jointure :
  ```sql
  SELECT c.Nom, o.Montant FROM Clients c, Commandes o WHERE c.ID = o.ClientID
  ```

### CREATEINDEX
Créer un index sur une table.  
```sql
CREATEINDEX ON NomTable KEY=NomColonne ORDER=ordre
```
- Cette commande crée un **B+Tree** en mémoire pour accélérer les recherches.

### SELECTINDEX
Effectuer une recherche rapide grâce à un index.  
```sql
SELECTINDEX * FROM NomTable WHERE NomColonne=Valeur
```

### Commandes supplémentaires

- **Créer une base de données** :
  ```sql
  CREATE DATABASE NomDB
  ```
- **Sélectionner une base de données** :
  ```sql
  SET DATABASE NomDB
  ```
- **Lister les tables dans la base de données actuelle** :
  ```sql
  LIST TABLES
  ```
- **Supprimer une table** :
  ```sql
  DROP TABLE NomTable
  ```
- **Lister toutes les bases de données** :
  ```sql
  LIST DATABASES
  ```
- **Supprimer une base de données** :
  ```sql
  DROP DATABASE NomDB
  ```

---

## Notes Importantes
- **Limitation des commandes CREATEINDEX et SELECTINDEX** : Ces fonctionnalités peuvent ne plus fonctionner correctement après une nouvelle insertion de données.

---

## Contributions
Les contributions sont les bienvenues pour améliorer ou étendre ce projet !  
Veuillez soumettre vos pull requests ou ouvrir une issue pour signaler des bugs ou proposer des améliorations.