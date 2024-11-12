import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

class TestRelation {

    private Relation relation;
    private MyRecord record;

    @BeforeEach
    void setUp() throws Exception{
        // Initialisation des attributs et objets nécessaires pour les tests
        String relationName = "table";
        ArrayList<Pair<String, Data>> attributs = new ArrayList<>();
        PageId headerPageId = new PageId(0, 1);
        DiskManager dsmk = DiskManager.getInstance();

        HashMap<String, String> configValues = new HashMap<>();
        configValues.put("dbpath", "src/tests/db/"); // Chemin vers la base de données
        configValues.put("pagesize", "4096"); // Taille de la page en octets
        configValues.put("dm_maxfilesize", "10485760"); // Taille maximale du fichier en octets
        configValues.put("bm_buffercount", "10"); // Nombre de buffers gérés par le BufferManager
        configValues.put("bm_policy", "LRU"); // Politique de remplacement du BufferManager
        DBConfig dbConfig = new DBConfig(configValues); // Création d'une instance de DBConfig avec les valeurs de configuration
        BufferManager bm = new BufferManager(dbConfig, dsmk);

        attributs.add(new Pair<>("id", new Data(DataType.INT)));
        attributs.add(new Pair<>("nom", new Data(DataType.VARCHAR, 32)));
        attributs.add(new Pair<>("prenom", new Data(DataType.CHAR, 32)));
        attributs.add(new Pair<>("note", new Data(DataType.REAL)));
        attributs.add(new Pair<>("birthdate", new Data(DataType.DATE))); // Ajout d'un champ DATE

        relation = new Relation(relationName, attributs, headerPageId, dsmk, bm);
        relation.setAttribut(attributs);

        record = new MyRecord(relation);

        // Création de l'enregistrement avec des valeurs pour tous les types
        record.addValue(1, DataType.INT); // INT
        record.addValue("Dupont", DataType.VARCHAR); // VARCHAR
        record.addValue("Alice", DataType.CHAR); // CHAR
        float val = (float)(2.1);
        record.addValue(val, DataType.REAL); // REAL
        record.addValue(new Date(20, 5, 2000), DataType.DATE); // DATE (année, mois, jour)
    }

    @Test
    void testAddAttribut() {
        // Ajout d'un nouvel attribut
        relation.addAttribut(new Pair<>("age", new Data(DataType.INT)));

        // Vérification de l'ajout
        assertEquals(6, relation.getAttribut().size()); // Devrait être 5, car on a ajouté un nouvel attribut "age"
        assertEquals("age", relation.getNameAttribut(5)); // Vérifie que le nouvel attribut est "age"
    }

    @Test
    void testWriteAndReadRecordToBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int pos = 3;

        // Écriture de l'enregistrement dans le buffer
        int writtenSize = relation.writeRecordToBuffer(record, buffer, pos);

        // Vérification de la taille écrite
        assertTrue(writtenSize > 0, "La taille écrite doit être positive");

        // Lecture de l'enregistrement depuis le buffer
        MyRecord newRecord = new MyRecord(relation);
        relation.readRecordFromBuffer(newRecord, buffer, pos);

        for(int i = 0; i < record.getSize(); i++)
            // Comparaison des résultats entre les valeurs originales et lues
            assertEquals(record.getValue(i).toString(), newRecord.getValue(i).toString());

    }

    @Test
    void testGetDataPages() throws Exception {
        // Ajout de données et vérification des pages
        List<PageId> dataPages = relation.getDataPages();
        assertNotNull(dataPages);
        assertTrue(dataPages.size() > 0, "Il devrait y avoir au moins une page de données");
    }

    @Test
    void testAddDataPage() throws Exception {
        // Ajout d'une nouvelle page de données
        relation.addDataPage();
        List<PageId> dataPages = relation.getDataPages();

        // Vérification que la nouvelle page a bien été ajoutée
        assertTrue(dataPages.size() > 0, "Une page de données a dû être ajoutée");
    }

    @Test
    void testInsertRecord() throws Exception {
        // Insertion de l'enregistrement
        RecordId recordId = relation.InsertRecord(record);

        // Vérification de l'insertion
        assertNotNull(recordId);
        assertTrue(recordId.pageIdx != null, "Le RecordId doit contenir un PageId");
    }

    @Test
    void testGetAllRecords() throws Exception {
        // Ajout de plusieurs enregistrements
        record.addValue(1, DataType.INT);
        record.addValue("Alice", DataType.VARCHAR);
        relation.InsertRecord(record);

        MyRecord secondRecord = new MyRecord(relation);
        secondRecord.addValue(2, DataType.INT);
        secondRecord.addValue("Bob", DataType.VARCHAR);
        relation.InsertRecord(secondRecord);

        // Récupération de tous les enregistrements
        ArrayList<MyRecord> records = relation.GetAllRecords();

        // Vérification du nombre d'enregistrements
        assertEquals(2, records.size(), "Il doit y avoir 2 enregistrements dans la base de données");
    }

    @Test
    void testInvalidAddAttribut() {
        // Tentative d'ajouter un attribut nul (devrait échouer)
        assertThrows(IllegalArgumentException.class, () -> {
            relation.addAttribut(null);
        });
    }

    @Test
    void testSetRelationName() {
        // Définir un nom pour la relation
        relation.setRelationName("NewRelation");
        assertEquals("NEWRELATION", relation.getRelationName());
    }

    @Test
    void testInvalidSetRelationName() {
        // Tentative de définir un nom invalide (null ou vide)
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setRelationName("");
        });

        // Tentative de définir un nom invalide avec autre que des lettres
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setRelationName("salut1");
        });

        // Tentative de définir un nom invalide avec autre que des lettres
        assertDoesNotThrow(() -> {
            relation.setRelationName("salut");
        });
    }

    @Test
    void testGetTypeAndLength() {
        // Vérifier les types et longueurs des attributs
        assertEquals(DataType.INT, relation.getType(0));
        assertEquals(4, relation.getLength(0));
        assertEquals(DataType.VARCHAR, relation.getType(1));
        assertEquals(20, relation.getLength(1));
        assertEquals(DataType.REAL, relation.getType(2));
        assertEquals(4, relation.getLength(2));
        assertEquals(DataType.CHAR, relation.getType(3));
        assertEquals(5, relation.getLength(3));
        assertEquals(DataType.DATE, relation.getType(4));
        assertEquals(12, relation.getLength(4));
    }
}