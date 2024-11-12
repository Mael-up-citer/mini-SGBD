import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;

// Classe de test unitaire pour MyRecord
public class MyRecordTest {

    private MyRecord record;
    private Relation relation;

    /**
     * Prépare une nouvelle instance de Relation et de MyRecord avant chaque test,
     * en définissant des attributs pour tester les contraintes de type et de taille.
     */
    @BeforeEach
    public void setUp() {
        // Initialisation des attributs de la Relation pour imposer des contraintes
        ArrayList<Pair<String, Data>> attributs = new ArrayList<>();
        attributs.add(new Pair<>("id", new Data(DataType.INT)));                 // INT sans limite de taille
        attributs.add(new Pair<>("nom", new Data(DataType.VARCHAR, 10)));        // VARCHAR avec limite de 10
        attributs.add(new Pair<>("prenom", new Data(DataType.CHAR, 5)));         // CHAR avec limite de 5
        attributs.add(new Pair<>("note", new Data(DataType.REAL)));              // REAL sans limite de taille
        
        relation = new Relation("TestRelation", attributs, null, null, null);
        record = new MyRecord(relation);
    }

    /**
     * Teste le constructeur de MyRecord, vérifiant que la taille initiale est 0.
     */
    @Test
    public void testConstructor() {
        assertEquals(0, record.getSize(), "La taille initiale devrait être 0.");
    }

    /**
     * Teste l'ajout d'une valeur avec des types et tailles valides, et vérifie
     * la taille, la valeur, et le type des éléments ajoutés.
     */
    @Test
    public void testAddValidValues() {
        record.addValue(1, DataType.INT); // INT
        record.addValue("Dupont", DataType.VARCHAR); // VARCHAR de taille <= 10
        record.addValue("Alice", DataType.CHAR); // CHAR de taille <= 5
        record.addValue(3.5f, DataType.REAL); // REAL

        assertEquals(4, record.getSize(), "La taille devrait être 4 après l'ajout de 4 éléments.");
        assertEquals(1, record.getValue(0), "La valeur ajoutée devrait être 1.");
        assertEquals("Dupont", record.getValue(1), "La valeur ajoutée devrait être 'Dupont'.");
        assertEquals("Alice", record.getValue(2), "La valeur ajoutée devrait être 'Alice'.");
        assertEquals(3.5f, record.getValue(3), "La valeur ajoutée devrait être 3.5.");
    }

    /**
     * Teste le dépassement de taille pour un VARCHAR.
     * Vérifie que l'ajout échoue avec une exception.
     */
    @Test
    public void testAddValueExceedingVarcharSize() {
        record.addValue(1, DataType.INT); // INT
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            record.addValue("UneChaineTropLongue", DataType.VARCHAR); // Limite de 10
        });
        assertEquals("La chaîne dépasse la taille maximale autorisée pour l'attribut.", exception.getMessage());
    }

    /**
     * Teste l'ajout d'un type incompatible avec l'attribut spécifié.
     * Vérifie que l'ajout échoue avec une exception.
     */
    @Test
    public void testAddValueWithIncorrectType() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            record.addValue(45.67, DataType.INT); // Ajoute un REAL à un champ INT
        });
        assertEquals("La valeur doit être un entier pour le type INT.", exception.getMessage());
    }

    /**
     * Teste la méthode getSize, pour vérifier que la taille change correctement après chaque ajout.
     */
    @Test
    public void testGetSize() {
        assertEquals(0, record.getSize(), "La taille initiale devrait être 0.");
        record.addValue(100, DataType.INT);
        assertEquals(1, record.getSize(), "La taille devrait être 1 après l'ajout d'un élément.");
        record.addValue("Dupont", DataType.VARCHAR);
        assertEquals(2, record.getSize(), "La taille devrait être 2 après l'ajout de deux éléments.");
    }
}