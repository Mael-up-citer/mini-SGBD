import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TestMyRecord {

    private MyRecord record;

    /**
     * Initialisation avant chaque test.
     * Crée une instance de MyRecord et y ajoute des exemples valides pour tester.
     */
    @BeforeEach
    public void setUp() {
        record = new MyRecord();
    }

    /**
     * Teste le constructeur pour vérifier que la liste est initialisée vide.
     */
    @Test
    public void testConstructor() {
        assertEquals(0, record.size(), "La taille initiale de MyRecord devrait être 0.");
    }

    /**
     * Teste l'ajout d'éléments valides dans MyRecord et leur accès via getValue et getType.
     */
    @Test
    public void testAddValidValues() {
        record.add(1, DataType.INT); // Ajout d'un entier
        record.add(3.14, DataType.REAL); // Ajout d'un nombre réel
        record.add("Hello", DataType.VARCHAR); // Ajout d'une chaîne VARCHAR
        record.add(new Date(10, 02, 20), DataType.DATE); // Ajout d'une date

        assertEquals(4, record.size(), "La taille devrait être 4 après l'ajout de 4 éléments.");
        assertEquals(1, record.getValue(0), "La valeur ajoutée devrait être 1.");
        assertEquals(3.14, record.getValue(1), "La valeur ajoutée devrait être 3.14.");
        assertEquals("Hello", record.getValue(2), "La valeur ajoutée devrait être 'Hello'.");
        assertTrue(record.getValue(3) instanceof Date, "La dernière valeur devrait être une instance de Date.");
    }

    /**
     * Teste les types incompatibles : vérifie que des exceptions sont levées correctement.
     */
    @Test
    public void testAddValueWithIncorrectType() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            record.add("Invalid", DataType.INT); // Tente d'ajouter une chaîne dans un champ INT
        });
        assertEquals(
            "Type mismatch: expected INT but was String",
            exception.getMessage(),
            "Le message d'erreur devrait correspondre pour un type INT invalide."
        );

        exception = assertThrows(IllegalArgumentException.class, () -> {
            record.add(45.67, DataType.DATE); // Tente d'ajouter un REAL dans un champ DATE
        });
        assertEquals(
            "Type mismatch: expected DATE but was Double",
            exception.getMessage(),
            "Le message d'erreur devrait correspondre pour un type DATE invalide."
        );
    }

    /**
     * Teste l'accès à une valeur hors limites avec getValue.
     */
    @Test
    public void testGetValueOutOfBounds() {
        Exception exception = assertThrows(IndexOutOfBoundsException.class, () -> {
            record.getValue(0); // Tente d'accéder à un élément dans une liste vide
        });
        assertEquals(
            "Index 0 out of bounds for length 0",
            exception.getMessage(),
            "Le message d'erreur devrait signaler un index hors limites."
        );
    }

    /**
     * Teste la méthode toString pour s'assurer qu'elle retourne une chaîne bien formée.
     */
    @Test
    public void testToString() {
        record.add(42, DataType.INT);
        record.add("Test", DataType.VARCHAR);

        String expected = "Value = 42     Type = INT\nValue = Test     Type = VARCHAR\n";
        assertEquals(expected, record.toString(), "La représentation sous forme de chaîne devrait correspondre.");
    }

    /**
     * Teste les cas limites pour les types (exemple avec REAL et FLOAT).
     */
    @Test
    public void testEdgeCasesForTypes() {
        record.add(3.14159265359, DataType.REAL); // Nombre à virgule
        assertEquals(3.14159265359, record.getValue(0), "La valeur ajoutée devrait être acceptée comme REAL.");
        assertEquals(DataType.REAL, record.getType(0), "Le type devrait être REAL.");
    }

    /**
     * Teste l'ajout d'une Date et sa validité.
     */
    @Test
    public void testAddDate() {
        Date currentDate = new Date(18, 9, 2004);
        record.add(currentDate, DataType.DATE);
        assertEquals(currentDate, record.getValue(0), "La valeur ajoutée devrait correspondre à la Date fournie.");
    }

    /**
     * Teste le mélange de types pour vérifier la flexibilité de la structure.
     */
    @Test
    public void testMixedTypes() {
        record.add(123, DataType.INT);
        record.add(3.14f, DataType.REAL);
        record.add("Hello", DataType.VARCHAR);
        record.add(new Date(20, 10, 28), DataType.DATE);

        assertEquals(4, record.size(), "La taille de MyRecord devrait être 4.");
        assertEquals(123, record.getValue(0), "La première valeur devrait être un entier.");
        assertEquals(3.14f, record.getValue(1), "La deuxième valeur devrait être un réel.");
        assertEquals("Hello", record.getValue(2), "La troisième valeur devrait être une chaîne.");
        assertTrue(record.getValue(3) instanceof Date, "La dernière valeur devrait être une Date.");
    }
}