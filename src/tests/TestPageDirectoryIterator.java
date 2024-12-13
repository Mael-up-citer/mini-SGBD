import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;


import static org.junit.jupiter.api.Assertions.*;

public class TestPageDirectoryIterator {
    private DiskManager dskM;       // Gestionnaire de disque
    private BufferManager bm;       // Gestionnaire de buffer
    DBConfig dbConfig;
    private Relation relation;      // Relation à tester

    @BeforeEach
    public void setUp() {
        try {
            // Initialisation des gestionnaires simulés ou mockés
            dskM = DiskManager.getInstance();
            dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Recharger la configuration
            DBConfig.pagesize = 1000;
            DBConfig.dm_maxfilesize = 1000;

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
            attributs.add(new Pair<>("birthdate", new Data(DataType.DATE)));

            relation = new Relation("Personne", attributs, headerPageId, dskM, bm);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
/*
    @Test
    public void testGetNextDataPageId() {
        int nb = 10000;
        ArrayList<PageId> id = new ArrayList<>();

        for (int i = 0; i < nb; i++)
            id.add(relation.addDataPage());

        int i = 0;

        PageDirectoryIterator iterator = null;
        try {
            iterator = new PageDirectoryIterator(relation, bm);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {            
            PageId tmp;

            while ((tmp = iterator.GetNextDataPageId()) != null) {
                assertEquals(id.get(i), tmp, "Page de données "+i+" incorrecte.");
                i++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        assertNull(iterator.GetNextDataPageId(), "Aucune page ne devrait être disponible après la dernière.");
        assertTrue(i == nb, "on aurais du avoir "+nb+" itération or on en a "+i);
    }
*/
    @Test
    public void testReset() {
        int nb = 250;
        ArrayList<PageId> id = new ArrayList<>();

        for (int i = 0; i < nb; i++)
            id.add(relation.addDataPage());

        PageDirectoryIterator iterator = null;
        try {
            iterator = new PageDirectoryIterator(relation, bm);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int i = 0;
        PageId tmp;

        try {
            while ((tmp = iterator.GetNextDataPageId()) != null);

            iterator.Reset();

            while ((tmp = iterator.GetNextDataPageId()) != null) {
                assertEquals(id.get(i), tmp, "Page de données "+i+"incorrecte.");
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(i == nb, "on aurais du avoir "+nb+" itération or on en a "+i);
    }

    @Test
    public void testClose() {
        PageDirectoryIterator iterator = null;
        try {
            iterator = new PageDirectoryIterator(relation, bm);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Appel de la méthode Close
        iterator.Close();

        assertNull(iterator.GetNextDataPageId(), "le get sur un iterateur fermé doit etre null");
    }
}