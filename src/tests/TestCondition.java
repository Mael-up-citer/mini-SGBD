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
        "id, =, 1, true",         // Test égalité pour ID (1 == 1)
        "id, =, 2, false",        // Test égalité pour ID (1 != 2)
        "note, >, 10.0, true",    // Test supérieur pour note (15.5 > 10)
        "note, >, 20.0, false",   // Test supérieur pour note (15.5 !> 20)
        "note, <, 20.0, true",    // Test inférieur pour note (15.5 < 20)
        "note, <, 10.0, false",   // Test inférieur pour note (15.5 !< 10)
        "note, <=, 15.5, true",   // Test inférieur ou égal pour note (15.5 <= 15.5)
        "note, <=, 10.0, false",  // Test inférieur ou égal pour note (15.5 !<= 10)
        "note, >=, 15.5, true",   // Test supérieur ou égal pour note (15.5 >= 15.5)
        "note, >=, 20.0, false",  // Test supérieur ou égal pour note (15.5 !>= 20)
        "nom, =, \"'Doe'\", true",    // Test égalité pour nom ('Doe' == 'Doe')
        "nom, =, \"'Smith'\", false", // Test égalité pour nom ('Doe' != 'Smith')
        "nom, <>, \"'Smith'\", true", // Test différent pour nom ('Doe' != 'Smith')
        "nom, <>, \"'Doe'\", false"   // Test différent pour nom ('Doe' != 'Doe')
    })
    public void testEvaluateOperatorWithAllTypes(String attribute, String operator, String value, boolean expectedResult) throws Exception {
        // Retirer les guillemets doubles externes pour récupérer la vraie constante
        if (value.startsWith("\"") && value.endsWith("\""))
            value = value.substring(1, value.length() - 1); // Supprimer les guillemets doubles externes

        // Créer les termes pour la condition
        Pair<String, Relation> terme1 = new Pair<>(attribute, relation);
        Pair<String, Relation> terme2;

        // Si la valeur est une constante string (commence et se termine par des quotes simples)
        if (value.startsWith("'") && value.endsWith("'"))
            terme2 = new Pair<>(value, null); // Constante string
        else
            terme2 = new Pair<>(value, null); // Constante numérique ou autre

        // Créer la condition
        Condition condition = new Condition(terme1, operator, terme2);

        // Évaluer la condition
        boolean result = condition.evaluate(record, record);

        // Assertion du résultat attendu
        assertEquals(expectedResult, result, "L'opération " + operator + " pour l'attribut " + attribute + " avec la valeur " + value + " a échoué.");
    }

    @Test
    public void testEvaluateRealComparison() throws Exception {
        // Teste une comparaison pour le type réel
        Pair<String, Relation> terme1 = new Pair<>("note", relation);
        Pair<String, Relation> terme2 = new Pair<>("10.0", null); // Constante

        Condition condition = new Condition(terme1, ">", terme2);
        assertTrue(condition.evaluate(record, record), "La condition '> 10.0' doit être vraie.");

        terme2 = new Pair<>("20.0", null); // Constante
        condition = new Condition(terme1, "<", terme2);
        assertTrue(condition.evaluate(record, record), "La condition '< 20.0' doit être vraie.");
    }
}
