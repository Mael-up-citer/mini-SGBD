import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.ArrayList;
import java.nio.ByteBuffer;

public class TestCondition {

    private MyRecord record;
    private Relation relation;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialisation des objets nécessaires pour les tests
        DiskManager dskM = DiskManager.getInstance();
        DBConfig dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
        BufferManager bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

        // Allocation d'une nouvelle page d'en-tête (header page)
        PageId headerPageId = dskM.AllocPage(); // Nouvelle page d'en-tête

        // Modification du buffer de la nouvelle page d'en-tête
        ByteBuffer buffer = bm.getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Mettre -1 pour indiquer qu'il n'y a pas de page suivante
        bm.freePage(headerPageId, true);

        // Réinitialisation des attributs et création d'une nouvelle relation
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
    public void clean() throws Exception{
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
        "nom, <>, \"'Doe'\", false"  // Test différent pour nom ('Doe' != 'Doe')
    })
    public void testEvaluateOperatorWithAllTypes(String attribute, String operator, String value, boolean expectedResult) throws Exception {            
        // Si la valeur est une chaîne (comprise entre guillemets doubles), enlever les guillemets externes
        if (value.startsWith("\"") && value.endsWith("\""))
            value = value.substring(1, value.length() - 1); // Supprimer les guillemets externes
        // Affichage de la valeur modifiée pour débogage
        System.out.println("Attribut: " + attribute + ", Valeur: " + value);
        
        // Créer la condition et l'évaluer
        Condition condition = new Condition(attribute, operator, value);
        boolean result = condition.evaluate(relation, record);
        
        // Assertion du résultat attendu
        assertEquals(expectedResult, result, "L'opération " + operator + " pour l'attribut " + attribute + " avec la valeur " + value + " a échoué.");
    }        

    @Test
    public void testEvaluateRealComparison() throws Exception {
        // Tester une comparaison pour le type réel
        Condition condition = new Condition("note", ">", "10.0");
        assertTrue(condition.evaluate(relation, record), "La condition '> 10.0' doit être vraie.");

        condition = new Condition("note", "<", "20.0");
        assertTrue(condition.evaluate(relation, record), "La condition '< 20.0' doit être vraie.");
    }
}