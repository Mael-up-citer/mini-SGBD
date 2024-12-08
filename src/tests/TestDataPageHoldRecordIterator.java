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

    private PageDirectoryIterator fils;

    private ArrayList<MyRecord> records = new ArrayList<>();


    @BeforeEach
    public void setUp() throws Exception {
        // Initialisation des gestionnaires simulés ou mockés
        dskM = DiskManager.getInstance();
        dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
        DBConfig.pagesize = 250;
        DBConfig.dm_maxfilesize = 500;
        DBConfig.bm_buffercount = 256;

        bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

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

        relation = new Relation("Personne", attributs, headerPageId, dskM, bm);

    
        // 3. Crée les record
        MyRecord record1 = new MyRecord();
        // Création et insertion d'un premier enregistrement
        record1.add(1, DataType.INT); // Valeur de type INT
        record1.add("Dupont", DataType.VARCHAR); // Valeur de type VARCHAR
        record1.add("Alice", DataType.CHAR); // Valeur de type CHAR
        record1.add(2.1f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record1);
        records.add(record1);

        // Création et insertion d'un deuxième enregistrement
        MyRecord record2 = new MyRecord();
        record2.add(2, DataType.INT); // Valeur de type INT
        record2.add("Martin", DataType.VARCHAR); // Valeur de type VARCHAR
        record2.add("Jean", DataType.CHAR); // Valeur de type CHAR
        record2.add(0.1f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record2);
        records.add(record2);

        // Création et insertion d'un troisième enregistrement
        MyRecord record3 = new MyRecord();
        record3.add(3, DataType.INT); // Valeur de type INT
        record3.add("DurandEtNon", DataType.VARCHAR); // Valeur de type VARCHAR
        record3.add("Paul", DataType.CHAR); // Valeur de type CHAR
        record3.add(5.75f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record3);
        records.add(record3);

        // Création et insertion d'un quatrième enregistrement
        MyRecord record4 = new MyRecord();
        record4.add(4, DataType.INT); // Valeur de type INT
        record4.add("AliceEtNon", DataType.VARCHAR); // Valeur de type VARCHAR
        record4.add("A", DataType.CHAR); // Valeur de type CHAR
        record4.add(3.14f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record4);
        records.add(record4);

        // Création et insertion d'un cinquième enregistrement
        MyRecord record5 = new MyRecord();
        record5.add(42, DataType.INT); // Valeur de type INT
        record5.add("Bobboooobbbbb", DataType.VARCHAR); // Valeur de type VARCHAR
        record5.add("B", DataType.CHAR); // Valeur de type CHAR
        record5.add(99.99f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record5);
        records.add(record5);

        // Création et insertion d'un sixième enregistrement
        MyRecord record6 = new MyRecord();
        record6.add(7, DataType.INT); // Valeur de type INT
        record6.add("Charleeeeeeeeeee", DataType.VARCHAR); // Valeur de type VARCHAR
        record6.add("C", DataType.CHAR); // Valeur de type CHAR
        record6.add(0.75f, DataType.REAL); // Valeur de type REAL
        relation.InsertRecord(record6);
        records.add(record6);

        System.out.println("\n\n\n");
        fils = new PageDirectoryIterator(relation, bm);
    }

    @Test
    void testGetNextRecord() throws Exception {
        PageId datPageId = fils.GetNextDataPageId();
        DataPageHoldRecordIterator iterator = new DataPageHoldRecordIterator(relation, bm.getPage(datPageId), bm, datPageId);

        for (MyRecord record : records) {
            MyRecord res = iterator.GetNextRecord();

            if (res == null) {
                System.out.println("null");
                datPageId = fils.GetNextDataPageId();
                iterator.Close();
                iterator = new DataPageHoldRecordIterator(relation, bm.getPage(datPageId), bm, datPageId);
                res = iterator.GetNextRecord();
            }
            assertEquals(record, res);
            System.out.println();
        }
        // Vérifie qu'après les records, null est retourné
        assertNull(iterator.GetNextRecord());
    }

    @Test
    void testReset() throws Exception{
        PageId datPageId = fils.GetNextDataPageId();
        DataPageHoldRecordIterator iterator = new DataPageHoldRecordIterator(relation, bm.getPage(datPageId), bm, datPageId);

        // Consomme 1 record
        iterator.GetNextRecord();
        iterator.Reset();

        MyRecord res;
        int cpt = 0;

        while ((res = iterator.GetNextRecord()) != null) {
            assertEquals(records.get(cpt), res);
            cpt++;
        }
    }

    @Test
    void testClose() throws Exception{
        PageId datPageId = fils.GetNextDataPageId();
        DataPageHoldRecordIterator iterator = new DataPageHoldRecordIterator(relation, bm.getPage(datPageId), bm, datPageId);

        iterator.Close();

        iterator.GetNextRecord();

        assertNull(iterator.GetNextRecord(), "après le close le nex record doit etre null");
    }
}