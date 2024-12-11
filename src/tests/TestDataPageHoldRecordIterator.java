import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class TestDataPageHoldRecordIterator {
    private DiskManager dskM;       // Gestionnaire de disque (mock ou simulé)
    private BufferManager bm;       // Gestionnaire de buffer (mock ou simulé)
    private Relation relation;      // Relation à tester
    private DBConfig dbConfig;
    private PageDirectoryIterator pageIterator;

    private ArrayList<MyRecord> originalRec;


    @BeforeEach
    public void setUp() throws Exception {
        // Initialisation des gestionnaires simulés ou mockés
        dskM = DiskManager.getInstance();
        dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
        DBConfig.pagesize = 4000;
        DBConfig.dm_maxfilesize = 1500;
        DBConfig.bm_buffercount = 1028;

        bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

        // Création de la structure de la relation
        PageId headerPageId = dskM.AllocPage();  // Allocation d'une page d'en-tête
        ByteBuffer buffer = bm.getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Mettre -1 pour indiquer qu'il n'y a pas de page suivante
        bm.freePage(headerPageId, true);

        // Création de la relation avec des attributs
        ArrayList<Pair<String, Data>> attributs = new ArrayList<>();
        attributs.add(new Pair<>("id", new Data(DataType.INT)));
        attributs.add(new Pair<>("nom", new Data(DataType.CHAR, 32)));
        attributs.add(new Pair<>("note", new Data(DataType.REAL)));

        relation = new Relation("Personne", attributs, headerPageId, dskM, bm);
        originalRec = new ArrayList<>();

        addTuplesToRelation(relation, 41);

        pageIterator = new PageDirectoryIterator(relation, bm);
    }

    private void addTuplesToRelation(Relation relation, int numTuples) throws Exception {
        for (int i = 1; i <= numTuples; i++) {
            MyRecord record = new MyRecord();
            record.add(i, DataType.INT); // ID
            record.add("N" + i, DataType.CHAR); // Name
            record.add((float) i * 1.1f, DataType.REAL); // Value

            relation.InsertRecord(record);
            originalRec.add(record);
        }
    }

    @Test
    void testGetNextRecord() {
        try {
            PageId datPageId = pageIterator.GetNextDataPageId();
            DataPageHoldRecordIterator iterator = new DataPageHoldRecordIterator(relation, bm.getPage(datPageId), bm, datPageId);
            MyRecord record;
            int cpt = 0;

            while ((record = iterator.GetNextRecord()) != null) {
                assertEquals(originalRec.get(cpt), record);
                cpt++;
            }

            // Vérifie qu'après les records, null est retourné
            assertNull(iterator.GetNextRecord());
            assertEquals(41, cpt);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    void testReset() throws Exception{
        PageId datPageId = pageIterator.GetNextDataPageId();
        DataPageHoldRecordIterator iterator = new DataPageHoldRecordIterator(relation, bm.getPage(datPageId), bm, datPageId);

        // Consomme 3 record
        iterator.GetNextRecord();
        iterator.GetNextRecord();
        iterator.GetNextRecord();

        iterator.Reset();

        MyRecord res;
        int cpt = 0;

        while ((res = iterator.GetNextRecord()) != null) {
            assertEquals(originalRec.get(cpt), res);
            cpt++;
        }

        // Vérifie qu'après les records, null est retourné
        assertNull(iterator.GetNextRecord());
        assertEquals(41, cpt);
    }

    @Test
    void testClose() throws Exception{
        PageId datPageId = pageIterator.GetNextDataPageId();
        DataPageHoldRecordIterator iterator = new DataPageHoldRecordIterator(relation, bm.getPage(datPageId), bm, datPageId);

        iterator.Close();

        assertNull(iterator.GetNextRecord(), "après le close le next record doit etre null");
    }
}