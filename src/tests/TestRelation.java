import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

class TestRelation {
    DiskManager dskM;
    DBConfig dbConfig;
    BufferManager bm;

    Relation relation;
    Relation r1;
    Relation r2;
    Relation r3;

    ArrayList<MyRecord> originalRec = new ArrayList<>();

    @BeforeEach
    void init() throws Exception {
        dskM = DiskManager.getInstance();
        dskM.RAZ();
        dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
        bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

        relation = createRelation("relation", bm);
    }

    private Relation createRelation(String relationName, BufferManager bm) throws Exception {
        // Allocation d'une page d'en-tête pour la relation
        PageId headerPageId = dskM.AllocPage();
        ByteBuffer buffer = bm.getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Indique qu'il n'y a pas de page suivante
        bm.freePage(headerPageId, true);

        // Définition des attributs de la relation
        ArrayList<Pair<String, Data>> attributes = new ArrayList<>();
        attributes.add(new Pair<>("id", new Data(DataType.INT)));
        attributes.add(new Pair<>("name", new Data(DataType.VARCHAR, 32)));
        attributes.add(new Pair<>("pseudo", new Data(DataType.CHAR, 32)));
        attributes.add(new Pair<>("value", new Data(DataType.REAL)));

        // Création et retour de la relation
        return new Relation(relationName, attributes, headerPageId, dskM, bm);
    }

    private void addTuplesToRelation(Relation relation, int numTuples) throws Exception {
        for (int i = 1; i <= numTuples; i++) {
            MyRecord record = new MyRecord();
            record.add(i, DataType.INT); // ID
            record.add(relation.getRelationName()+" Name" + i, DataType.VARCHAR); // Name
            record.add(relation.getRelationName()+" pseudo" + i, DataType.CHAR); // Name
            record.add((float) i * 1.1f, DataType.REAL); // Value

            relation.InsertRecord(record);
            originalRec.add(record);
        }
    }

    @Test
    void testAddAttribut() {
        // Ajout d'un nouvel attribut valide
        relation.setOneAttribut(new Pair<>("age", new Data(DataType.INT)));

        // Vérification de l'ajout
        assertEquals(5, relation.getAttribut().size()); // Devrait être 5, car on a ajouté un nouvel attribut "age"
        assertEquals("AGE", relation.getNameAttribut(4)); // Vérifie que le nouvel attribut est "age"

        // Add un attribut null
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            // Tente d'ajouter un attribut avec un nom invalide
            relation.setOneAttribut(null);
        });

        // Add un attribut invalide
        exception = assertThrows(IllegalArgumentException.class, () -> {
            // Tente d'ajouter un attribut avec un nom invalide
            relation.setOneAttribut(new Pair<>("", new Data(DataType.INT)));
        });
        // Add un attribut invalide
        exception = assertThrows(IllegalArgumentException.class, () -> {
            // Tente d'ajouter un attribut avec un nom invalide
            relation.setOneAttribut(new Pair<>("1Personne", new Data(DataType.INT)));
        });
        // Add un attribut invalide
        exception = assertThrows(IllegalArgumentException.class, () -> {
            // Tente d'ajouter un attribut avec un nom invalide
            relation.setOneAttribut(new Pair<>("Create", new Data(DataType.INT)));
        });
    }

    @Test
    void testWriteAndReadRecordToBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(DBConfig.pagesize);
        int pos = 3;

        // Écriture de l'enregistrement dans le buffer
        int writtenSize = relation.writeRecordToBuffer(originalRec.get(0), buffer, pos);
        // Vérification de la taille écrite
        assertTrue(writtenSize > 0, "La taille écrite doit être positive non null");

        // Lecture de l'enregistrement depuis le buffer
        MyRecord newRecord = new MyRecord();

        int readSize = relation.readRecordFromBuffer(newRecord, buffer, pos);
        // Vérification de la taille écrite
        assertTrue(writtenSize == readSize, "La taille lu doit être égal à la taille écrite");

        for(int i = 0; i < originalRec.get(0).size(); i++)
            // Comparaison des résultats entre les valeurs originales et lues
            assertEquals(originalRec.get(0).getValue(i).toString(), newRecord.getValue(i).toString());

        // Ecriture / Lecture depuis une position imposible
        ByteBuffer buffer2 = ByteBuffer.allocate(DBConfig.pagesize);
        pos = DBConfig.pagesize;

        // Écriture de l'enregistrement dans le buffer
        writtenSize = relation.writeRecordToBuffer(originalRec.get(0), buffer2, pos);

        // Vérification de la taille écrite
        assertTrue(writtenSize == 0, "La taille écrite doit être null but was "+writtenSize);

        // Lecture de l'enregistrement depuis le buffer
        readSize = relation.readRecordFromBuffer(newRecord, buffer, pos);
        // Vérification de la taille lu
        assertTrue(readSize == 0, "La taille lu doit être null but was "+readSize);
    }

    @Test
    void testAddDataPageANDGetDataPagesANDGetFreeDataPages() {
        try {
            int cpt = 100;

            for (int j = (cpt / 2)-1; j < cpt+1; j++) {
                dskM.RAZ();
                DBConfig.pagesize = j;
                DBConfig.dm_maxfilesize = DBConfig.pagesize *2;
                BufferManager bm = new BufferManager(dbConfig, dskM);

                r1 = createRelation("relation1", bm);
                r2 = createRelation("relation2", bm);
                r3 = createRelation("relation3", bm);

                // Initialisation des listes pour la vérification
                ArrayList<PageId> allocatePageR1 = new ArrayList<>();
                ArrayList<PageId> allocatePageR2 = new ArrayList<>();
                ArrayList<PageId> allocatePageR3 = new ArrayList<>();

                // Ajoute 10 000 data Page
                for (int i = 0; i < 10000; i++) {
                    // Ajoute la nouvelle data Page à la liste
                    allocatePageR1.add(r1.addDataPage());
                    allocatePageR2.add(r2.addDataPage());
                    allocatePageR3.add(r3.addDataPage());
                }

                // On fait un get data Page et free data
                ArrayList<PageId> dataPagesR1 = (ArrayList<PageId>) r1.getDataPages();
                ArrayList<PageId> freeDataPagesR1 = (ArrayList<PageId>) r1.getFreeDataPages();

                ArrayList<PageId> dataPagesR2 = (ArrayList<PageId>) r2.getDataPages();
                ArrayList<PageId> freeDataPagesR2 = (ArrayList<PageId>) r2.getFreeDataPages();

                ArrayList<PageId> dataPagesR3 = (ArrayList<PageId>) r3.getDataPages();
                ArrayList<PageId> freeDataPagesR3 = (ArrayList<PageId>) r3.getFreeDataPages();


                // Test qu'il y ait autant de page allouée que trouvée
                assertTrue(dataPagesR1.size() == allocatePageR1.size(), "Il devrait y avoir exactement: " + allocatePageR1.size() + " data Page or on en a: " + (dataPagesR1.size()));
                assertTrue(freeDataPagesR1.size() == allocatePageR1.size(), "Il devrait y avoir exactement: " + allocatePageR1.size() + " data Page or on en a: " + (freeDataPagesR1.size()));

                assertTrue(dataPagesR2.size() == allocatePageR2.size(), "Il devrait y avoir exactement: " + allocatePageR2.size() + " data Page or on en a: " + (dataPagesR2.size()));
                assertTrue(freeDataPagesR2.size() == allocatePageR2.size(), "Il devrait y avoir exactement: " + allocatePageR2.size() + " data Page or on en a: " + (freeDataPagesR2.size()));

                assertTrue(dataPagesR3.size() == allocatePageR3.size(), "Il devrait y avoir exactement: " + allocatePageR3.size() + " data Page or on en a: " + (dataPagesR3.size()));
                assertTrue(freeDataPagesR3.size() == allocatePageR3.size(), "Il devrait y avoir exactement: " + allocatePageR3.size() + " data Page or on en a: " + (freeDataPagesR3.size()));


                // Test que les 2 listes soient identiques
                for (int i = 0; i < 10000; i++) {
                    assertTrue(dataPagesR1.get(i).equals(allocatePageR1.get(i)), "erreur au tour "+i+" car: "+dataPagesR1.get(i)+" != "+allocatePageR1.get(i));
                    assertTrue(freeDataPagesR1.get(i).equals(allocatePageR1.get(i)), "erreur au tour "+i+" car: "+freeDataPagesR1.get(i)+" != "+allocatePageR1.get(i));

                    assertTrue(dataPagesR2.get(i).equals(allocatePageR2.get(i)), "erreur au tour "+i+" car: "+dataPagesR2.get(i)+" != "+allocatePageR2.get(i));
                    assertTrue(freeDataPagesR2.get(i).equals(allocatePageR2.get(i)), "erreur au tour "+i+" car: "+freeDataPagesR2.get(i)+" != "+allocatePageR2.get(i));

                    assertTrue(dataPagesR3.get(i).equals(allocatePageR3.get(i)), "erreur au tour "+i+" car: "+dataPagesR3.get(i)+" != "+allocatePageR3.get(i));
                    assertTrue(freeDataPagesR3.get(i).equals(allocatePageR3.get(i)), "erreur au tour "+i+" car: "+freeDataPagesR3.get(i)+" != "+allocatePageR3.get(i));
                }
            }
            // Nettoyage des fichiers
            int i = 0;
            while (Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb")))
                i++;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
/*
    @Test
    void testInsertRecord() throws Exception {
        try {
            DBConfig.dm_maxfilesize = 1000;

            DiskManager dskM = DiskManager.getInstance();

            for (int j = 200; j < DBConfig.dm_maxfilesize; j++) {
                dskM.RAZ();

                // Réinitialisation des objets partagés pour chaque itération
                DBConfig dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
                DBConfig.pagesize = j;
                DBConfig.dm_maxfilesize = 600;
                DBConfig.bm_buffercount = 8;

                BufferManager bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

                // Allocation d'une nouvelle page d'en-tête (header page)
                PageId headerPageId = dskM.AllocPage(); // Nouvelle page d'en-tête
                // Modification du buffer de la nouvelle page d'en-tête
                ByteBuffer buffer2 = bm.getPage(headerPageId);
                buffer2.putInt(DBConfig.pagesize - 4, -1); // Mettre -1 pour indiquer qu'il n'y a pas de page suivante
                bm.freePage(headerPageId, true);

                // Réinitialisation des attributs et création d'une nouvelle relation
                ArrayList<Pair<String, Data>> attributs = new ArrayList<>();
                attributs.add(new Pair<>("id", new Data(DataType.INT)));
                attributs.add(new Pair<>("nom", new Data(DataType.VARCHAR, 32)));
                attributs.add(new Pair<>("prenom", new Data(DataType.CHAR, 32)));
                attributs.add(new Pair<>("note", new Data(DataType.REAL)));
                attributs.add(new Pair<>("birthdate", new Data(DataType.DATE)));

                Relation relation = new Relation("Personne", attributs, headerPageId, dskM, bm);

                // Création d'un enregistrement
                MyRecord record = new MyRecord();
                // Création de l'enregistrement avec des valeurs pour tous les types
                record.add(1, DataType.INT); // Valeur de type INT
                record.add("Dupont", DataType.VARCHAR); // Valeur de type VARCHAR
                record.add("Alice", DataType.CHAR); // Valeur de type CHAR
                record.add(2.1f, DataType.REAL); // Valeur de type REAL
                record.add(new Date(20, 5, 2000), DataType.DATE); // Valeur de type DATE

                // Boucle pour insérer plusieurs records
                for (int i = 0; i < 5000; i++) {
                    RecordId recordId = null;
                    ByteBuffer buffer = null;

                    // Insertion de l'enregistrement
                    recordId = relation.InsertRecord(record);

                    // Charge la page contenant l'enregistrement inséré
                    buffer = bm.getPage(recordId.pageIdx);

                    // Vérification que l'insertion à retourné un RecordId valide
                    assertNotNull(recordId, "Le RecordId ne doit pas être null pour le record " + i);
                    assertNotNull(recordId.pageIdx, "Le RecordId doit contenir un PageId valide pour le record " + i);
                    assertTrue(recordId.pageIdx.FileIdx >= 0, "Le PageId doit être un index valide de page pour le record " + i);
                    assertTrue(recordId.pageIdx.PageIdx >= 0, "Le PageId doit être un index valide de page pour le record " + i);

                    // Calcul la position en octet du tuple
                    int pos = buffer.getInt(DBConfig.pagesize - ((recordId.slotIdx-1) * 8 + 16));

                    // Lire le record depuis le buffer à la position indiquée par le RecordId
                    MyRecord readRecord = new MyRecord();
                    relation.readRecordFromBuffer(readRecord, buffer, pos);

                    bm.freePage(recordId.pageIdx, false);

                    // Vérification que le record lu correspond à celui inséré
                    assertIterableEquals(record, readRecord, "Le record inséré ne correspond pas au record lu pour l'index " + i);
                }
                // Nettoyage des fichiers après chaque itération
                for (int i = 0; i < DBConfig.pagesize*3; i++)
                    Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb"));
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

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
        ArrayList<Pair<MyRecord,RecordId>> records = relation.GetAllRecords();

        // Vérification du nombre d'enregistrements
        assertEquals(2, records.size(), "Il doit y avoir 2 enregistrements dans la base de données");
    }

    @Test
    void testInvalidAddAttribut() {
        // Tentative de définir un nom invalide car vide
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setOneAttribut(new Pair<>("", new Data(DataType.INT)));
        });

        // Tentative de définir un nom invalide car null
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setOneAttribut(new Pair<>(null, new Data(DataType.INT)));
        });

        // Tentative de définir un nom invalide a cause de sa syntaxe
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setOneAttribut(new Pair<>("1Personne", new Data(DataType.INT)));
        });

        // Tentative de définir un nom invalide a cause de sa syntaxe
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setOneAttribut(new Pair<>("create", new Data(DataType.INT)));
        });
    }

    @Test
    void testSetRelationName() {
        // Tentative de définir un nom invalide car vide
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setRelationName("");
        });

        // Tentative de définir un nom invalide car null
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setRelationName(null);
        });

        // Tentative de définir un nom invalide a cause de sa syntaxe
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setRelationName("1Personne");
        });

        // Tentative de définir un nom invalide a cause de sa syntaxe
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setRelationName("create");
        });
    }

    @Test
    void testNameToIndex() {
        ArrayList<Pair<String, Data>> attrb = relation.getAttribut();

        for (int i = 0; i < relation.getNbAttribut(); i++)
            assertTrue(relation.getNameToIndex().get(relation.getAttribut(i).getFirst()) == i, "erreur l'attribut "+relation.getAttribut(i).getFirst()+" n'est pas au bon indice dans la map: expected "+i+" but was "+relation.getNameToIndex().get(relation.getAttribut(i).getFirst()));
    }
*/
}