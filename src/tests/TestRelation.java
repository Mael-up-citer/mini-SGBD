import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

class TestRelation {
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
        DBConfig.dm_maxfilesize = 100;
        DiskManager dskM = DiskManager.getInstance();

        for (int j = (DBConfig.dm_maxfilesize / 2) - 1; j < DBConfig.dm_maxfilesize+1; j++) {
            dskM.RAZ();

            DBConfig dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
            DBConfig.pagesize = j;
            DBConfig.dm_maxfilesize = 100;

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
            attributs.add(new Pair<>("birthdate", new Data(DataType.DATE)));

            Relation relation = new Relation("Personne", attributs, headerPageId, dskM, bm);

            // Initialisation des listes pour la vérification
            ArrayList<PageId> allocatePage = new ArrayList<>();

            System.out.println("\n\n\npagesize = " + DBConfig.pagesize + "    dm_maxfilesize = " + DBConfig.dm_maxfilesize);

            // Ajoute 5000 data Page
            for (int i = 0; i < 1000; i++)
                // Ajoute la nouvelle data Page à la liste
                allocatePage.add(relation.addDataPage());

            //System.out.println("\n\nget time \n\n");

            // On fait un get data Page
            ArrayList<PageId> dataPages = (ArrayList<PageId>) relation.getDataPages();

            // Test qu'il y ait autant de page allouée que trouvée
            assertTrue(dataPages.size() == allocatePage.size(), "Il devrait y avoir exactement: " + allocatePage.size() + " data Page or on en a: " + (dataPages.size()));

            // Test que les 2 listes soient identiques
            assertIterableEquals(allocatePage, dataPages);
        }
        // Nettoyage des fichiers
        for (int i = 0; i < 1300; i++)
            Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb"));
    }

    @Test
    void testAddDataPageANDGetFreeDataPages() throws Exception {
        DBConfig.dm_maxfilesize = 100;
        DiskManager dskM = DiskManager.getInstance();

        for (int j = (DBConfig.dm_maxfilesize / 2) - 1; j < DBConfig.dm_maxfilesize+1; j++) {
            // Réinitialisation des objets partagés pour chaque itération
            dskM.RAZ();

            DBConfig dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
            DBConfig.pagesize = j;
            DBConfig.dm_maxfilesize = 100;

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
            attributs.add(new Pair<>("birthdate", new Data(DataType.DATE)));

            Relation relation = new Relation("Personne", attributs, headerPageId, dskM, bm);

            // Initialisation des listes pour la vérification
            ArrayList<PageId> allocatePage = new ArrayList<>();

            System.out.println("\n\n\npagesize = " + DBConfig.pagesize + "    dm_maxfilesize = " + DBConfig.dm_maxfilesize);

            // Ajoute 5000 data Page
            for (int i = 0; i < 1000; i++)
                // Ajoute la nouvelle data Page à la liste
                allocatePage.add(relation.addDataPage());

            // On fait un get data Page
            ArrayList<PageId> dataPages = (ArrayList<PageId>) relation.getFreeDataPages();

            // Test qu'il y ait autant de page allouée que trouvée
            assertTrue(dataPages.size() == allocatePage.size(), "Il devrait y avoir exactement: " + allocatePage.size() + " data Page or on en a: " + (dataPages.size()));

            // Test que les 2 listes soient identiques
            assertIterableEquals(allocatePage, dataPages);
        }
        // Nettoyage des fichiers après chaque itération
        for (int i = 0; i < 1300; i++)
        Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb"));
    }
*/

    @Test
    void testInsertRecord() throws Exception {
        try {
            DBConfig.dm_maxfilesize = 1000;
            DiskManager dskM = DiskManager.getInstance();

            for (int j = 150; j < DBConfig.dm_maxfilesize; j++) {
                dskM.RAZ();

                // Réinitialisation des objets partagés pour chaque itération
                DBConfig dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
                DBConfig.pagesize = j;
                DBConfig.dm_maxfilesize = 300;

                BufferManager bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

                // Allocation d'une nouvelle page d'en-tête (header page)
                PageId headerPageId = dskM.AllocPage(); // Nouvelle page d'en-tête
                System.out.println(headerPageId);
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

                System.out.println("\n\n\n\n\nconfig: pagesize = "+DBConfig.pagesize);

                // Boucle pour insérer plusieurs records
                for (int i = 0; i < 1000; i++) {
                    RecordId recordId = null;
                    ByteBuffer buffer = null;

                    System.out.println("\n\n\nstep: "+i);

                    // Insertion de l'enregistrement
                    recordId = relation.InsertRecord(record);
                    
                    /*

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

                    */
                }
                // Nettoyage des fichiers après chaque itération
                for (int i = 0; i < DBConfig.pagesize*3; i++)
                    Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb"));
            }
        } catch(Exception e) {
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
/*
    @Test
    void testInvalidAddAttribut() {
        // Tentative de définir un nom invalide car vide
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setAttribut("");
        });

        // Tentative de définir un nom invalide car null
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setAttribut(null);
        });

        // Tentative de définir un nom invalide a cause de sa syntaxe
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setAttribut("1Personne");
        });

        // Tentative de définir un nom invalide a cause de sa syntaxe
        assertThrows(IllegalArgumentException.class, () -> {
            relation.setAttribut("create");
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
*/
}