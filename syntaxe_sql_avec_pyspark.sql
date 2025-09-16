-- Ce fichier montre l'utilisation de la syntaxe SQL avec les fonctions spark.(compatible uniquement dans un editeur sql pour spark)

USE CATALOG workspace;
DROP SCHEMA IF EXISTS exo_sql_files CASCADE;
CREATE SCHEMA IF NOT EXISTS exo_sql_files;   -- pas de LOCATION sous UC Free
USE SCHEMA exo_sql_files;

--- array_append , current_date

USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS exo_sql_files;

-- crée un volume nommé 'files' dans le schéma
CREATE VOLUME IF NOT EXISTS exo_sql_files.files;

----
SELECT array_append(array('Laptop', 'Mouse'), 'Keyboard') AS produits;

SELECT split('Alice,Bob,Chai', ',') AS noms;

SELECT current_date() AS aujourd_hui,
       date_format(current_date(), 'yyyy-MM-dd') AS formattee;

DESCRIBE FUNCTION array_append;

SELECT
    upper('alice') AS upper_case,
    concat_ws('-', 'Alice', 'Dupont') AS concatene,
    size(array(1,2,3,4)) AS taille_array,
    array_contains(array(1,2,3), 2) AS contient_2;
----
USE CATALOG workspace;
USE SCHEMA exo_sql_files;

-- Affiche les fichiers présents dans le dossier CSV du Volume
LIST '/Volumes/workspace/exo_sql_files/files/pyspark_sql_files_exo/files_csv';
---
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS exo_sql_files;
---map_concat exemple
CREATE OR REPLACE TABLE sales AS
SELECT 1 AS id, map('a', 10) AS map1, map('b', 20) AS map2
UNION ALL
SELECT 2 AS id, map('x', 100), map('y', 200);

select * from sales

select map_concat(map1,map2) AS NEWMAP from sales
