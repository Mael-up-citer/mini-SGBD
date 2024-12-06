
#!/bin/bash

# Défini la variable du nom de la classe de test
nom="TestRelation"

# Défini si on lance tout les tests ensemble ou non: oui si: "--scan-class-path" non si: "--select-class""$nom"
# option="--scan-class-path"

option="--select-class ""$nom"

# Compilation des classes principales
javac -d bin -sourcepath src/main src/main/*.java

# Compilation des classes de test avec le fichier JAR de JUnit Platform
javac -d bin -sourcepath src/tests -cp "bin:lib/junit-platform-console-standalone-1.11.3.jar" src/tests/*.java

# Exécution des tests
java -jar lib/junit-platform-console-standalone-1.11.3.jar execute --classpath bin $option

cd src/tests/db/BinData
rm *