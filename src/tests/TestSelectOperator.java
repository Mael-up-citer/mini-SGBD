import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class TestSelectOperator {
    private DiskManager dskM;
    private BufferManager bm;
    private DBConfig dbConfig;
    private Relation relation;
    private selectOperator selectOperator;

    private ArrayList<MyRecord> originalRec;

    @BeforeEach
    void setup() throws Exception {
        // Initialisation des gestionnaires
        dskM = DiskManager.getInstance();
        dskM.RAZ();
        dbConfig = DBConfig.loadConfig("src/tests/config.txt");
        DBConfig.pagesize = 250;
        DBConfig.dm_maxfilesize = 500;
        DBConfig.bm_buffercount = 1028;

        bm = new BufferManager(dbConfig, dskM); // Initialiser le BufferManager

        originalRec = new ArrayList<>();

        // Création de la relation et ajout de tuples
        relation = createRelation("TestRelation");
        addTuplesToRelation(relation, 100);

    }

    private Relation createRelation(String relationName) throws Exception {
        // Allocation d'une page d'en-tête pour la relation
        PageId headerPageId = dskM.AllocPage();
        ByteBuffer buffer = bm.getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Indique qu'il n'y a pas de page suivante
        bm.freePage(headerPageId, true);

        // Définition des attributs de la relation
        ArrayList<Pair<String, Data>> attributes = new ArrayList<>();
        attributes.add(new Pair<>("id", new Data(DataType.INT)));
        attributes.add(new Pair<>("name", new Data(DataType.VARCHAR, 32)));
        attributes.add(new Pair<>("value", new Data(DataType.REAL)));

        // Création et retour de la relation
        return new Relation(relationName, attributes, headerPageId, dskM, bm);
    }

    private void addTuplesToRelation(Relation relation, int numTuples) throws Exception {
        for (int i = 1; i <= numTuples; i++) {
            MyRecord record = new MyRecord();
            record.add(i, DataType.INT); // ID
            record.add("Name" + i, DataType.VARCHAR); // Name
            record.add((float) i * 1.1f, DataType.REAL); // Value

            relation.InsertRecord(record);
            originalRec.add(record);
        }
    }

    @Test
    void testSelectOperatorIter() throws Exception {
        // Initialisation de l'opérateur de sélection
        selectOperator = new selectOperator(new PageDirectoryIterator(relation, bm), relation, new ArrayList<>(), bm);
        // Récupérer les enregistrements qui satisfont les conditions
        ArrayList<MyRecord> selectedRecords = new ArrayList<>();
        MyRecord record;
  
        while ((record = selectOperator.GetNextRecord()) != null)
            selectedRecords.add(record);

        // Comparaison des listes triées
        assertEquals(originalRec.size(), selectedRecords.size(), "Les tailles des listes ne correspondent pas.");

        for (int i = 0; i < originalRec.size(); i++)
            if (! originalRec.contains(selectedRecords.get(i)))
                assertTrue(false, "Le record "+i+" est une halluciantion: "+record);
    }

    @Test
    void testSelectOperatorFiltersCorrectly() throws Exception {
        // Ajout des conditions de sélection
        ArrayList<Condition> conditions = new ArrayList<>();
        conditions.add(new Condition(new Pair<>("id", 0), ">", new Pair<>("10", -1))); // ID > 10
        conditions.add(new Condition(new Pair<>("value", 2), "<", new Pair<>("15.5", -1))); // Value < 15.5
        // Initialisation de l'opérateur de sélection
        selectOperator = new selectOperator(new PageDirectoryIterator(relation, bm), relation, conditions, bm);

        // Récupérer les enregistrements qui satisfont les conditions
        ArrayList<MyRecord> selectedRecords = new ArrayList<>();
        MyRecord record;

        while ((record = selectOperator.GetNextRecord()) != null)
            selectedRecords.add(record);

        // Vérifications : seuls les enregistrements avec ID > 10 et Value < 15.5 doivent être sélectionnés
        for (MyRecord selectedRecord : selectedRecords) {
            // Extraction des valeurs en fonction de leur index ou nom
            int id = (int) selectedRecord.get(0).getFirst();
            float value = (float) selectedRecord.get(2).getFirst();

            assertTrue(id > 10, "L'ID devrait être supérieur à 10");
            assertTrue(value < 15.5f, "La valeur devrait être inférieure à 15.5");
        }

        // Vérifier le nombre attendu de résultats
        assertEquals(4, selectedRecords.size(), "Le nombre de résultats sélectionnés devrait être de 4");
    }

    @Test
    void testSelectOperatorHandlesNoMatchingRecords() throws Exception {
        // Modifier les conditions pour ne correspondre à aucun enregistrement
        ArrayList<Condition> conditions = new ArrayList<>();
        conditions.add(new Condition(new Pair<>("id", 0), ">", new Pair<>("100", -1))); // ID > 100
        // Initialisation de l'opérateur de sélection
        selectOperator = new selectOperator(new PageDirectoryIterator(relation, bm), relation, conditions, bm);

        // Récupérer les enregistrements
        MyRecord record = selectOperator.GetNextRecord();

        // Aucune correspondance attendue
        assertNull(record, "Aucun enregistrement ne devrait être sélectionné");
    }

    @Test
    void testSelectOperatorReset() throws Exception {
        // Initialisation de l'opérateur de sélection
        selectOperator = new selectOperator(new PageDirectoryIterator(relation, bm), relation, new ArrayList<>(), bm);

        MyRecord record;

        while ((record = selectOperator.GetNextRecord()) != null);

        // Réinitialiser l'opérateur
        selectOperator.Reset();

        int cpt = 0;

        while ((record = selectOperator.GetNextRecord()) != null) {
            // Parcourir à nouveau et vérifier que les enregistrements sont relus depuis le début
            assertTrue(originalRec.contains(record), "Les enregistrements après réinitialisation devrait être identique aux enregistrements initiaux: itération: "+cpt);
            cpt++;
        }
        assertTrue(originalRec.size() == cpt, "Les listes devraient faire la meme taille");
    }

    @Test
    void testSelectOperatorClose() throws Exception {
        // Initialisation de l'opérateur de sélection
        selectOperator = new selectOperator(new PageDirectoryIterator(relation, bm), relation, new ArrayList<>(), bm);

        // Fermer l'opérateur
        selectOperator.Close();

        assertNull(selectOperator.GetNextRecord());        
    }
}