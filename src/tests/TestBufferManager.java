import org.junit.jupiter.api.AfterEach; // Importation de l'annotation pour exécuter du code après chaque test
import org.junit.jupiter.api.BeforeEach; // Importation de l'annotation pour exécuter du code avant chaque test
import org.junit.jupiter.api.Test; // Importation de l'annotation pour marquer une méthode comme un test
import static org.junit.jupiter.api.Assertions.*; // Importation des assertions statiques pour effectuer des tests

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestBufferManager {

	private DiskManager dskM;
	private DBConfig dbConfig;
	private BufferManager bm;
	private ByteBuffer buffer1;
	private ByteBuffer buffer2;
	private ByteBuffer buffer3;
	
	@BeforeEach
    private void setup() throws Exception{
        // Initialiser DiskManager et charger la configuration avant chaque test
        dskM = DiskManager.getInstance();
        dbConfig = DBConfig.loadConfig("src/tests/config.txt"); // Charger la configuration
        bm = new BufferManager(dbConfig, dskM);

        buffer1 = ByteBuffer.allocate(DBConfig.pagesize);
        buffer2 = ByteBuffer.allocate(DBConfig.pagesize);
        buffer3 = ByteBuffer.allocate(DBConfig.pagesize);
    }
	
	@AfterEach
    private void tearDown() throws IOException{
        // Nettoyer après les tests en supprimant les fichiers générés
        Files.deleteIfExists(Paths.get(DBConfig.dbpath + "dm.save"));

        int i = 0;
        while (Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb"))) // Nettoyer tous les fichiers de test
            i++;
    }
	
    @Test // Test si le constructeur initialise suffisamment de buffer
    void testConstructeur() throws Exception {
        assertTrue(bm.getEmptyBufferSize() == DBConfig.bm_buffercount);
    }
    
    @Test // Test l'accès aux pages
    void testGetPage() throws Exception{
    	// Crée une page
        PageId id = dskM.AllocPage();

        // La remplie du caractère A
    	for (int j = 0; j < buffer1.capacity(); j++)
            buffer1.put((byte)('A')); // Remplir le buffer de 'A'

    	// L'écrit
    	dskM.WritePage(id, buffer1);

    	//Charge la page dans un autre buffer
        buffer2 = bm.getPage(id);

        //Prepare les buffers pour la lecture
        buffer1.flip();
        buffer2.flip();

        // Verifie que le noeud est crée
        assertTrue(bm.getCadre().search(id).id == id, "Le noeud n'est pas implémenté");
        // Verifie que le fichier est lu
        assertTrue(buffer2.hasRemaining(), "Le fichier n'est pas lu");
        // Verifie que le pin count est incrémenté
        assertTrue(bm.getCadre().search(id).pin_count == 1, "Le pin count n'est pas incrémenter");
        // Verifie que le compteur de page chargée est incrémenté
        assertTrue(bm.getNbAllocFrame() == 1, "Le compteur n'a pas incrémenter");
        // Verifie que la liste de buffer vide est décrémenter de 1
        assertTrue(bm.getEmptyBufferSize() == DBConfig.bm_buffercount-1, "Le compteur n'ait pas incrémenter");

        // Vérifie byte par byte que le contenu chargé est identique à celui lu
        while(buffer1.hasRemaining()) {
            byte a = buffer1.get(); // Lire un octet du buffer d'écriture
            byte b = buffer2.get(); // Lire un octet du buffer de lecture
            assertTrue(a == b, "Les octets lus ne correspondent pas.");
        }
        
        // Charge la même page dans un autre buffer
        buffer3 = bm.getPage(id);
        // Prépare le buffer pour la lecture
        buffer3.flip();
        // Vérifie byte par byte que le contenu chargé est identique a celui lu
        while(buffer3.hasRemaining()) {
            byte a = buffer3.get(); // Lire un octet du buffer d'écriture
            byte b = buffer2.get(); // Lire un octet du buffer de lecture
            assertTrue(a == b, "Les octets lus ne correspondent pas.");
        }
        // Verifie que le pin count est bien incrémenté
        assertEquals(bm.getCadre().search(id).pin_count, 2, "Le pin count n'est pas incrémenter");
    }

    @Test //Test la libération de page
    void testFreePageAddSuppJunk() throws Exception{
    	// Crée une première page 
        PageId id = dskM.AllocPage();

        // La charge
        bm.getPage(id);
        // La libère en dirty true
        bm.freePage(id, true);

        AVLNode node = bm.getCadre().search(id);

        // Verifie le flag dirty
        assertTrue(node.dirtyFlag, "Le flage n'est pas mis à dirty");
        // Verifie le pin count
        assertEquals(node.pin_count, 0, "Le pin count n'est pas bon");
        // Verifie la mise en JunkFile
        assertNotNull(node.pointeurListe, "Le fichier n'est pas ajouté dans la junkFile");
        // Verifie l'initialisation de la root
        assertEquals(bm.getJunkFile().getRoot().getSuivant(), node.pointeurListe, "Le pointeur junkFile ne s'initialise pas");
        // Verifie l'initialisation de la junkFile
        assertEquals(bm.getJunkFile().get(id).getValue(), node.pointeurListe.getValue(), "Le pointeur junkFile ne s'initialise pas");
        // Verifie l'initialisation du pointeur last
        assertEquals(bm.getJunkFile().getTail(), node.pointeurListe, "Le pointeur last ne s'initialise pas");

        // Charge la page
        bm.getPage(id);

        // Verifie que la page a été supprimé de la junkFile
        assertNull(bm.getCadre().search(id).pointeurListe, "La page n'est pas supprimée de la junkFile");
        assertEquals(bm.getJunkFile().getRoot(), bm.getJunkFile().getRoot().getSuivant(), "La junkFile n'est pas vide");
        assertEquals(bm.getJunkFile().getTail(), bm.getJunkFile().getRoot(), "Le pointeur last n'est pas vide");

        // Charge la page à nouveau (simulation d'un deuxième utilisateur)
        bm.getPage(id);
        // Libère la page (un utilisateur l'utilise toujours)
        bm.freePage(id, false);

        // Verifie que le flag dirty n'est pas changé
        assertTrue(bm.getCadre().search(id).dirtyFlag, "Le flage est remis à clean entre deux accès");
        // Vérifie le pin count
        assertEquals(bm.getCadre().search(id).pin_count, 1, "Le pin count n'est pas bon");
        // Vérifie que la page n'est pas dans la junkFile
        assertNull(bm.getCadre().search(id).pointeurListe, "Le fichier est ajouté dans la JunkFile et le pin count n'est pas nul");

        // Libère la page totalement (plus d'utilisateur en accès)
        bm.freePage(id, false);

        // Crée une deuxième page
        PageId id2 = dskM.AllocPage();

        // Verifie qu'on ne peut pas supprimer une page non chargée
        assertFalse(bm.freePage(id2, false));

        // Charge la page 2
        bm.getPage(id2);
        //Libère la page 2
        bm.freePage(id2, false);

        node = bm.getCadre().search(id2);

        assertFalse(node.dirtyFlag, "Le dirty n'est pas bon");
        // Verifie que la page 2 est bien en last Junk
        assertEquals(bm.getJunkFile().getTail(), node.pointeurListe, "Le pointeur last n'a pas été modifié");
        // Verifie que le pointeur de junkFile n'a pas été modifié
        assertEquals(bm.getJunkFile().getRoot().getSuivant().getSuivant(), node.pointeurListe, "Le pointeur junkFile a été modifié");
    }

    @Test //Test le nettoyage de tous les buffers et l'écriture des pages modifiées
    void TestFlushBuffer() throws Exception {
    	// Crée une page
        PageId id = dskM.AllocPage();
        // Charge la page
        buffer1 = bm.getPage(id);
        // Prepare le buffer1 à l'écriture
        buffer1.flip();
        
        // La remplie du caractère A
    	for (int j = 0; j < buffer1.capacity(); j++) {
            buffer1.put((byte) ('A')); // Remplir le buffer une lettre
        }
    	// Libère la page en dirty
    	bm.freePage(id, true);
    	// Vide les buffers
    	bm.flushBuffers();
    	// Charge la page
    	buffer1 = bm.getPage(id);
    	
        // Vérifie byte par byte que le contenu chargé est identique a celui écrit
        while(buffer1.hasRemaining()) {
            byte a = buffer1.get(); // Lire un octet du buffer d'écriture
            assertEquals(a, (byte) 'A', "Les octets lus ne correspondent pas.");
        }
        //Prepare le buffer2 à l'écriture
    	buffer1.flip();
    	// La rempli du caractère 'B'
    	for (int j = 0; j < buffer1.capacity(); j++) {
            buffer1.put((byte) ('B')); // Remplir le buffer une lettre
        }
    	// Libère la page en clean (sans modification)
    	bm.freePage(id, false);
    	// Vide les buffers
    	bm.flushBuffers();
    	// Charge la page
    	buffer1 = bm.getPage(id);
    	// Vérifie byte par byte que le contenu chargé est identique a celui lu
        while(buffer1.hasRemaining()) {
            byte a = buffer1.get(); // Lire un octet du buffer d'écriture
            assertNotEquals(a, (byte) 'B', "Les octets lus ne correspondent pas.");
        }
    }
/*
    @Test // Le nettoyage d'un buffer selon la police de remplacement MRU
    void testMakeSpaceMRU() throws Exception {
    	// Passe la police de remplacement en MRU
    	bm.SetCurrentReplacementPolicy("MRU");

        // Nombre de frame à allouer pour remplire le BufferManager
    	int nbPage = DBConfig.bm_buffercount;

        // Charge et Libère toutes les pages sauf la derniere
        for (int i = 0; i< nbPage-1; i++) {
        	PageId id = dskM.AllocPage();
        	bm.getPage(id);
            bm.freePage(id, false);
        }

        // Identifiant de la page qui sera libérée en MRU
        PageId most = dskM.AllocPage();
        // Charge la page cible
        bm.getPage(most);
        // Libère la page cible
        bm.freePage(most, false);

        // Charge une dernière page pour faire déborder le BM
        bm.getPage(dskM.AllocPage());

        // Vérifie que la page supprimé de l'AVL est bien most
    	assertTrue(bm.getCadre().search(most) == null, "La page supprimé n'est pas most");
        // Vérifie que la page supprimé de la junkFile est bien most
    	assertTrue(bm.getJunkFile().get(most) == null, "La page supprimé n'est pas most");
    	// Verifie que tous les buffers sont utilisé
    	assertEquals(bm.getEmptyBufferSize(), 0, "Les buffers ne sont pas tous pleins");
    }
 */
    @Test // Le nettoyage d'un buffer selon la police de remplacement MRU
    void testMakeSpaceLRU() throws Exception { 
    	bm.SetCurrentReplacementPolicy("LRU");

        // Nombre de frame à allouer pour remplire le BufferManager
    	int nbPage = DBConfig.bm_buffercount;
        // Identifiant de la page qui sera libérée
        PageId least = dskM.AllocPage();
        bm.getPage(least);
        bm.freePage(least, false);

        // Charge et Libère toutes les pages sauf la derniere
        for (int i = 0; i< nbPage-1; i++) {
        	PageId id = dskM.AllocPage();
        	bm.getPage(id);
            bm.freePage(id, false);
        }

        // Charge une dernière page pour faire déborder le BM
        bm.getPage(dskM.AllocPage());

        // Vérifie que la page supprimé de l'AVL est bien most
    	assertTrue(bm.getCadre().search(least) == null, "La page supprimé de l'AVL n'est pas most");
        // Vérifie que la page supprimé de la junkFile est bien most
    	assertTrue(bm.getJunkFile().get(least) == null, "La page supprimé de la junkFill n'est pas most");
    	// Verifie que tous les buffers sont utilisé
    	assertEquals(bm.getEmptyBufferSize(), 0, "Les buffers ne sont pas tous pleins");
    }

    @Test
    void testSetCurrentReplacementPolicy() {
    	switch(DBConfig.bm_policy) {
    	case "LRU":
    		bm.SetCurrentReplacementPolicy("MRU");
    		assertEquals(DBConfig.bm_policy, "MRU");
    		break;
    	case "MRU":
    		bm.SetCurrentReplacementPolicy("LRU");
    		assertEquals(DBConfig.bm_policy, "LRU");
		}
    }

    @Test
    void testMakeSpace() {
    	DBConfig.bm_policy = "MRU";
        int i = 0;
        try {
            for (i = 0; i < 10000; i++) {
                PageId id = dskM.AllocPage();
                bm.getPage(id);

                PageId tmp = id;
                bm.freePage(tmp, false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(i == 10000);


        DBConfig.bm_policy = "LRU";
        i = 0;
        try {
            for (i = 0; i < 10000; i++) {
                PageId id = dskM.AllocPage();
                bm.getPage(id);

                PageId tmp = id;
                bm.freePage(tmp, false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(i == 10000);


        DBConfig.bm_policy = "MRU";
        i = 0;
        try {
            for (i = 0; i < 10000; i++) {
                PageId id = dskM.AllocPage();
                bm.getPage(id);

                PageId tmp = id;
                bm.freePage(tmp, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(i == 10000);


        DBConfig.bm_policy = "LRU";
        i = 0;
        try {
            for (i = 0; i < 10000; i++) {
                PageId id = dskM.AllocPage();
                bm.getPage(id);

                PageId tmp = id;
                bm.freePage(tmp, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(i == 10000);
    }

    @Test
    void testMakeSpacePlant() {
    	DBConfig.bm_policy = "MRU";
        int i = 0;
        try {
            for (i = 0; i < 10000; i++) {
                PageId id = dskM.AllocPage();
                bm.getPage(id);
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
        assertTrue(i != 10000);


        DBConfig.bm_policy = "LRU";
        i = 0;
        try {
            for (i = 0; i < 10000; i++) {
                PageId id = dskM.AllocPage();
                bm.getPage(id);
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
        assertTrue(i != 10000);
    }
}