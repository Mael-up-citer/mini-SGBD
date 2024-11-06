#!/bin/bash

# Compilation des classes principales
javac -d bin -sourcepath src/main src/main/*.java

# Compilation des classes de test avec le fichier JAR de JUnit Platform
javac -d bin -sourcepath src/tests -cp "bin:lib/junit-platform-console-standalone-1.11.3.jar" src/tests/*.java

# Exécution des tests unitaires en utilisant explicitement 'execute'
java -jar lib/junit-platform-console-standalone-1.11.3.jar execute --classpath bin --scan-class-path