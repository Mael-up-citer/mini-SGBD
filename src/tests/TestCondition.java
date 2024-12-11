import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class TestCondition {

    private MyRecord record;
    private Relation relation;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialisation des objets nécessaires pour les tests
        DiskManager dskM = DiskManager.getInstance();
        DBConfig dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Charger la configuration
        BufferManager bm = new BufferManager(dbConfig, dskM); // Initialiser le BufferManager

        // Allocation d'une nouvelle page d'en-tête
        PageId headerPageId = dskM.AllocPage(); // Nouvelle page d'en-tête

        // Modification du buffer de la nouvelle page d'en-tête
        ByteBuffer buffer = bm.getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Mettre -1 pour indiquer qu'il n'y a pas de page suivante
        bm.freePage(headerPageId, true);

        // Initialisation des attributs et création de la relation
        ArrayList<Pair<String, Data>> attributs = new ArrayList<>();
        attributs.add(new Pair<>("id", new Data(DataType.INT)));
        attributs.add(new Pair<>("nom", new Data(DataType.VARCHAR, 32)));
        attributs.add(new Pair<>("prenom", new Data(DataType.CHAR, 32)));
        attributs.add(new Pair<>("note", new Data(DataType.REAL)));

        relation = new Relation("Personne", attributs, headerPageId, dskM, bm);

        // Créer un enregistrement de test avec des valeurs fictives pour les colonnes
        record = new MyRecord();
        record.add(1, DataType.INT);                 // ID (INT)
        record.add("Doe", DataType.VARCHAR);          // nom (VARCHAR)
        record.add("John", DataType.CHAR);            // prénom (CHAR)
        record.add(15.5f, DataType.REAL);             // note (REAL)
    }

    @AfterEach
    public void clean() throws Exception {
        // Nettoyage des fichiers après chaque itération
        for (int i = 0; i < 100; i++)
            Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb"));
    }

    @ParameterizedTest
    @CsvSource({
        // Comparaisons entre attributs et constantes
        "id, =, 1, 0, -1, true",         // Attribut (colonne 0) = constante
        "id, =, 2, 0, -1, false",        // Attribut (colonne 0) != constante
        "note, >, note, 3, 3, false",    // Comparaison entre attributs (note à colonne 3)
        "note, >, 10, 3, -1, true",    // Attribut > constante
        "note, <, 20, 3, -1, true",    // Attribut < constante
        "note, <=, 15.5, 3, -1, true",   // Attribut <= constante
        "note, >=, 15.5, 3, -1, true",   // Attribut >= constante
        "note, >=, 20, 3, -1, false",  // Attribut >= constante
        "nom, =, \"'Doe'\", 1, -1, true",    // Attribut = constante (nom)
        "nom, =, \"'Smith'\", 1, -1, false", // Attribut != constante (nom)
        "nom, <>, \"'Smith'\", 1, -1, true", // Attribut != constante (nom)
        "prenom, =, \"'John'\", 2, -1, true",   // Attribut = constante (prenom)
        "prenom, =, \"'Doe'\", 2, -1, false",   // Attribut != constante (prenom)
        "note, =, 15.5, 3, -1, true",    // Attribut = constante (note)
    
        // Comparaison entre deux attributs
        "nom, =, prenom, 1, 2, false",    // Comparaison entre attributs (nom == prenom)
        "prenom, <, nom, 2, 1, false",     // Comparaison entre attributs (prenom < nom)
        "prenom, >, nom, 2, 1, true",     // Comparaison entre attributs (prenom < nom)
    
        // Comparaison avec des doubles
        "note, =, 15.5, 3, -1, true",     // Attribut = constante (double)
        "note, >, 10.5, 3, -1, true",     // Attribut > constante (double)
        "note, <, 20.5, 3, -1, true",     // Attribut < constante (double)
        "note, <=, 15.5, 3, -1, true",    // Attribut <= constante (double)
        "note, >=, 15.5, 3, -1, true",    // Attribut >= constante (double)
        "note, >=, 20, 3, -1, false",   // Attribut >= constante (double)
    })
    public void testEvaluateOperatorWithAllTypes(String attribute, String operator, String value, int term1Index, int term2Index, boolean expectedResult) throws Exception {
        // Retirer les guillemets doubles externes pour récupérer la vraie constante
        if (value.startsWith("\"") && value.endsWith("\""))
            value = value.substring(1, value.length() - 1); // Supprimer les guillemets doubles externes
    
        // Créer les termes pour la condition
        Pair<String, Integer> terme1 = new Pair<>(attribute, term1Index); // Attribut en tant que terme1
    
        // Vérifier si la valeur est une constante (chaîne ou numérique) ou un attribut
        Pair<String, Integer> terme2;
        if (value.startsWith("'") && value.endsWith("'")) {
            // Constante string (les guillemets simples indiquent que c'est une chaîne)
            terme2 = new Pair<>(value, term2Index); // Constante string
        } else {
            // Sinon, c'est soit un attribut, soit une constante numérique
            terme2 = new Pair<>(value, term2Index); // Constante numérique ou attribut
        }
    
        // Créer la condition
        Condition condition = new Condition(terme1, operator, terme2);
    
        // Évaluer la condition
        boolean result = condition.evaluate(record);
    
        // Assertion du résultat attendu
        assertEquals(expectedResult, result, "L'opération " + operator + " pour l'attribut " + attribute + " avec la valeur " + value + " a échoué.");
    }    

    @Test
    public void testEvaluateRealComparison() throws Exception {
        // Teste une comparaison pour le type réel
        Pair<String, Integer> terme1 = new Pair<>("note", 3);
        Pair<String, Integer> terme2 = new Pair<>("10.0", -1); // Constante

        Condition condition = new Condition(terme1, ">", terme2);
        assertTrue(condition.evaluate(record), "La condition '> 10.0' doit être vraie.");

        terme2 = new Pair<>("20.0", -1); // Constante
        condition = new Condition(terme1, "<", terme2);
        assertTrue(condition.evaluate(record), "La condition '< 20.0' doit être vraie.");

    }
}
