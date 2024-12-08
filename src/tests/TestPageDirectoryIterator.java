import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class TestPageDirectoryIterator {
    private DiskManager dskM;       // Gestionnaire de disque (mock ou simulé)
    private BufferManager bm;       // Gestionnaire de buffer (mock ou simulé)
    private Relation relation;      // Relation à tester
    private PageDirectoryIterator iterator;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialisation des gestionnaires simulés ou mockés
        dskM = DiskManager.getInstance();
        DBConfig dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
        BufferManager bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

        // Création de la structure de la relation
        PageId headerPageId = dskM.AllocPage();  // Allocation d'une page d'en-tête
        ByteBuffer buffer = bm.getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Mettre -1 pour indiquer qu'il n'y a pas de page suivante
        bm.freePage(headerPageId, true);

        // Création de la relation avec des attributs
        ArrayList<Pair<String, Data>> attributs = new ArrayList<>();
        attributs.add(new Pair<>("id", new Data(DataType.INT)));
        attributs.add(new Pair<>("nom", new Data(DataType.VARCHAR, 32)));
        attributs.add(new Pair<>("prenom", new Data(DataType.CHAR, 32)));
        attributs.add(new Pair<>("note", new Data(DataType.REAL)));
        attributs.add(new Pair<>("birthdate", new Data(DataType.DATE)));

        relation = new Relation("Personne", attributs, headerPageId, dskM, bm);

/*        
        // 3. Crée les record
        // Création et insertion d'un premier enregistrement
        MyRecord record1 = new MyRecord();
        record1.add(1, DataType.INT); // Valeur de type INT
        record1.add("Dupont", DataType.VARCHAR); // Valeur de type VARCHAR
        record1.add("Alice", DataType.CHAR); // Valeur de type CHAR
        record1.add(2.1f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record1);

        // Création et insertion d'un deuxième enregistrement
        MyRecord record2 = new MyRecord();
        record2.add(2, DataType.INT); // Valeur de type INT
        record2.add("Martin", DataType.VARCHAR); // Valeur de type VARCHAR
        record2.add("Jean", DataType.CHAR); // Valeur de type CHAR
        record2.add(0.1f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record2);

        // Création et insertion d'un troisième enregistrement
        MyRecord record3 = new MyRecord();
        record3.add(3, DataType.INT); // Valeur de type INT
        record3.add("Durand", DataType.VARCHAR); // Valeur de type VARCHAR
        record3.add("Paul", DataType.CHAR); // Valeur de type CHAR
        record3.add(5.75f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record3);

        // Création et insertion d'un quatrième enregistrement
        MyRecord record4 = new MyRecord();
        record4.add(4, DataType.INT); // Valeur de type INT
        record4.add("Alice", DataType.VARCHAR); // Valeur de type VARCHAR
        record4.add("A", DataType.CHAR); // Valeur de type CHAR
        record4.add(3.14f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record4);

        // Création et insertion d'un cinquième enregistrement
        MyRecord record5 = new MyRecord();
        record5.add(42, DataType.INT); // Valeur de type INT
        record5.add("Bob", DataType.VARCHAR); // Valeur de type VARCHAR
        record5.add("B", DataType.CHAR); // Valeur de type CHAR
        record5.add(99.99f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record5);

        // Création et insertion d'un sixième enregistrement
        MyRecord record6 = new MyRecord();
        record6.add(7, DataType.INT); // Valeur de type INT
        record6.add("Charlie", DataType.VARCHAR); // Valeur de type VARCHAR
        record6.add("C", DataType.CHAR); // Valeur de type CHAR
        record6.add(0.75f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record6);
*/

        // Initialisation de l'itérateur
        iterator = new PageDirectoryIterator(relation, bm);
    }

    @Test
    public void testGetNextDataPageId() throws Exception {
        int nb = 10000;
        ArrayList<PageId> id = new ArrayList<>();

        for (int i = 0; i < nb; i++)
            id.add(relation.addDataPage());

        int i = 0;

        while (iterator.GetNextDataPageId() != null) {
            assertEquals(id.get(i), iterator.GetNextDataPageId(), "Première page de données incorrecte.");
            i++;
        }
        assertNull(iterator.GetNextDataPageId(), "Aucune page ne devrait être disponible après la dernière.");
    }

    @Test
    public void testReset() throws Exception {
        int nb = 10000;
        ArrayList<PageId> id = new ArrayList<>();

        for (int i = 0; i < nb; i++)
            id.add(relation.addDataPage());

        int i = 0;

        while (iterator.GetNextDataPageId() != null);
        iterator.Reset();

        while (iterator.GetNextDataPageId() != null) {
            assertEquals(id.get(i), iterator.GetNextDataPageId(), "Première page de données incorrecte.");
            i++;
        }
    }

    @Test
    public void testClose() {
        // Appel de la méthode Close
        iterator.Close();

        // Vérification que l'itérateur a libéré les ressources (simulation)
        // Comme la méthode close ne fait rien ici, on vérifie juste qu'elle peut être appelée sans erreur.
        assertDoesNotThrow(() -> iterator.Close(), "La fermeture de l'itérateur ne devrait pas provoquer d'exception.");
    }
}