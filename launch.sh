#!/bin/bash

# Compilation des classes principales (SGBD et ses dépendances)
javac -d bin -sourcepath src/main src/main/*.java

# Exécution du programme SGBD
java -cp bin SGBD ./