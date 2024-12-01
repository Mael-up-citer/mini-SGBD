import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

class TestRelation {

    private static PageId headerPageId;

    private static BufferManager bm;
    private DiskManager dskM;
    private Relation relation;
    private MyRecord record;


    @BeforeEach
    void setUp() throws Exception {
        // Initialisation des objets partagés (DiskManager, BufferManager, etc.)
        dskM = DiskManager.getInstance();

        DBConfig dbConfig =  DBConfig.loadConfig("src/tests/config.txt"); // Création de l'instance de DBConfig avec les valeurs de configuration
        bm = new BufferManager(dbConfig, dskM); // Création du BufferManager

        // Allocation d'une page d'en-tête (header page)
        headerPageId = dskM.AllocPage(); // Crée une page d'en-tête pour cette nouvelle relation

        // Modification du buffer de la page d'en-tête (pour indiquer qu'il n'y a pas de page suivante)
        ByteBuffer buffer = bm.getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Mettre -1 à la fin de la page pour indiquer qu'il n'y a pas de page suivante

        // Libérer la page d'en-tête après modification
        bm.freePage(headerPageId, true);

        // Initialisation des attributs et objets spécifiques à chaque test
        ArrayList<Pair<String, Data>> attributs = new ArrayList<>();
        attributs.add(new Pair<>("id", new Data(DataType.INT)));
        attributs.add(new Pair<>("nom", new Data(DataType.VARCHAR, 32)));
        attributs.add(new Pair<>("prenom", new Data(DataType.CHAR, 32)));
        attributs.add(new Pair<>("note", new Data(DataType.REAL)));
        attributs.add(new Pair<>("birthdate", new Data(DataType.DATE)));

        // Création de la relation (table) et de l'enregistrement
        relation = new Relation("Personne", attributs, headerPageId, dskM, bm);
        record = new MyRecord();

        // Création de l'enregistrement avec des valeurs pour tous les types
        record.add(1, DataType.INT); // Valeur de type INT
        record.add("Dupont", DataType.VARCHAR); // Valeur de type VARCHAR
        record.add("Alice", DataType.CHAR); // Valeur de type CHAR
        record.add(2.1f, DataType.REAL); // Valeur de type REAL
        record.add(new Date(20, 5, 2000), DataType.DATE); // Valeur de type DATE
    }

    @AfterEach
    private void tearDown() throws IOException{
        // Nettoyer après les tests en supprimant les fichiers générés
        for (int i = 0; i < 10; i++) // Nettoyer tous les fichiers de test
            Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb"));
    }
/*
    @Test
    void testAddAttribut() {
        // Ajout d'un nouvel attribut valide
        relation.addAttribut(new Pair<>("age", new Data(DataType.INT)));

        // Vérification de l'ajout
        assertEquals(6, relation.getAttribut().size()); // Devrait être 5, car on a ajouté un nouvel attribut "age"
        assertEquals("AGE", relation.getNameAttribut(5)); // Vérifie que le nouvel attribut est "age"

        // Add un attribut null
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            // Tente d'ajouter un attribut avec un nom invalide
            relation.addAttribut(null); // Nom vide
        });

         // Add un attribut invalide
        exception = assertThrows(IllegalArgumentException.class, () -> {
            // Tente d'ajouter un attribut avec un nom invalide
            relation.addAttribut(new Pair<>("", new Data(DataType.INT))); // Nom vide
        });
        // Add un attribut invalide
        exception = assertThrows(IllegalArgumentException.class, () -> {
            // Tente d'ajouter un attribut avec un nom invalide
            relation.addAttribut(new Pair<>("1Personne", new Data(DataType.INT))); // Nom vide
        });
        // Add un attribut invalide
        exception = assertThrows(IllegalArgumentException.class, () -> {
            // Tente d'ajouter un attribut avec un nom invalide
            relation.addAttribut(new Pair<>("Create", new Data(DataType.INT))); // Nom vide
        });
    }

    @Test
    void testWriteAndReadRecordToBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int pos = 3;

        // Écriture de l'enregistrement dans le buffer
        int writtenSize = relation.writeRecordToBuffer(record, buffer, pos);
        // Vérification de la taille écrite
        assertTrue(writtenSize > 0, "La taille écrite doit être positive non null");

        // Lecture de l'enregistrement depuis le buffer
        MyRecord newRecord = new MyRecord();

        int readSize = relation.readRecordFromBuffer(newRecord, buffer, pos);
        // Vérification de la taille écrite
        assertTrue(writtenSize == readSize, "La taille lu doit être égal à la taille écrite");

        for(int i = 0; i < record.size(); i++)
            // Comparaison des résultats entre les valeurs originales et lues
            assertEquals(record.getValue(i).toString(), newRecord.getValue(i).toString());


        // Ecriture / Lecture depuis une position imposible
        ByteBuffer buffer2 = ByteBuffer.allocate(DBConfig.pagesize);
        pos = DBConfig.pagesize;

        // Écriture de l'enregistrement dans le buffer
        writtenSize = relation.writeRecordToBuffer(record, buffer2, pos);
        // Vérification de la taille écrite
        assertTrue(writtenSize == 0, "La taille écrite doit être null but was "+writtenSize);

        // Lecture de l'enregistrement depuis le buffer
        readSize = relation.readRecordFromBuffer(newRecord, buffer, pos);
        // Vérification de la taille lu
        assertTrue(readSize == 0, "La taille lu doit être null but was "+readSize);
    }

    @Test
    void testAddDataPageANDGetDataPages() throws Exception {
        // Liste des pageId alloue a la relation
        ArrayList<PageId> allocatePage = new ArrayList<>();

        // Ajoute 1000 data Page
        for (int i = 0; i < 1000; i++)
            // Ajoute la nouvelle data Page à la liste
            allocatePage.add(relation.addDataPage());

        // On fait un get data Page
        ArrayList<PageId> dataPages = (ArrayList<PageId>)relation.getDataPages();

        // Test qu'il y ai autant de page alloué que trouvé
        assertTrue(dataPages.size() == allocatePage.size(), "Il devrait y avoir exactement: "+allocatePage.size()+" data Page or on en a: "+dataPages.size());

        // Test que les 2 listes soient identique
        assertIterableEquals(allocatePage, dataPages);
    }
*/
    @Test
    void testInsertRecord() {
        try {
          // Boucle pour insérer plusieurs records
            for (int i = 0; i < 1000; i++) {
                System.out.println("\n\nstep: "+i);

                // Insertion de l'enregistrement
                RecordId recordId = relation.InsertRecord(record);

                // Vérification que l'insertion à retourné un RecordId valide
                assertNotNull(recordId, "Le RecordId ne doit pas être null pour le record " + i);
                assertNotNull(recordId.pageIdx, "Le RecordId doit contenir un PageId valide pour le record " + i);
                assertTrue(recordId.pageIdx.FileIdx >= 0, "Le PageId doit être un index valide de page pour le record " + i);
                assertTrue(recordId.pageIdx.PageIdx >= 0, "Le PageId doit être un index valide de page pour le record " + i);

                // Lire la page contenant l'enregistrement inséré
                ByteBuffer buffer = bm.getPage(recordId.pageIdx);
                // Calcul la position en octet du tuple
                int pos = buffer.getInt(DBConfig.pagesize - (recordId.slotIdx * 8 + 12));

                System.out.println("pos lecture = "+pos);

                // Lire le record depuis le buffer à la position indiquée par le RecordId
                MyRecord readRecord = new MyRecord();
                relation.readRecordFromBuffer(readRecord, buffer, pos);

                bm.freePage(recordId.pageIdx, false);

                // Vérification que le record lu correspond à celui inséré
                assertIterableEquals(record, readRecord, "Le record inséré ne correspond pas au record lu pour l'index " + i);
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }
/*
    @Test
    void testGetAllRecords() throws Exception {
        MyRecord record2 = new MyRecord();
        // Ajout de plusieurs enregistrements
        // Création de l'enregistrement avec des valeurs pour tous les types
        record2.add(1, DataType.INT); // Valeur de type INT
        record2.add("Funes", DataType.VARCHAR); // Valeur de type VARCHAR
        record2.add("Jean", DataType.CHAR); // Valeur de type CHAR
        record2.add(2.0f, DataType.REAL); // Valeur de type REAL
        record2.add(new Date(21, 6, 2001), DataType.DATE); // Valeur de type DATE
        // Insere le record
        relation.InsertRecord(record2);

        MyRecord secondRecord = new MyRecord();
        // Création de l'enregistrement avec des valeurs pour tous les types
        secondRecord.add(1, DataType.INT); // Valeur de type INT
        secondRecord.add("Delamar", DataType.VARCHAR); // Valeur de type VARCHAR
        secondRecord.add("Pauk", DataType.CHAR); // Valeur de type CHAR
        secondRecord.add(1.9f, DataType.REAL); // Valeur de type REAL
        secondRecord.add(new Date(22, 7, 2002), DataType.DATE); // Valeur de type DATE
        // Insere le record
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
        // Tentative de définir un nom invalide (null ou vide)
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setRelationName("");
        });

        // Tentative de définir un nom invalide
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setRelationName(null);
        });

        // Tentative de définir un nom invalide
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setRelationName("1Personne");
        });
    }

    @Test
    void testGetTypeAndLength() {
        // Vérifier les types et longueurs des attributs
        assertEquals(DataType.INT, relation.getType(0));
        assertEquals(4, relation.getLength(0));

        assertEquals(DataType.VARCHAR, relation.getType(1));
        assertEquals(32, relation.getLength(1));

        assertEquals(DataType.CHAR, relation.getType(2));
        assertEquals(32, relation.getLength(2));

        assertEquals(DataType.REAL, relation.getType(3));
        assertEquals(4, relation.getLength(3));

        assertEquals(DataType.DATE, relation.getType(4));
        assertEquals(12, relation.getLength(4));
    }
    */
}