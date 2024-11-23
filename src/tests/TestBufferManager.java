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
	private DBConfig dbc;
	private BufferManager bm;
	
	@BeforeEach
    private void setup() throws Exception{
        // Initialiser DiskManager et charger la configuration avant chaque test
        dskM = DiskManager.getInstance();
        dbc = DBConfig.loadConfig("./config.txt");
        DBConfig.dbpath = "././";
        bm = new BufferManager(dbc, dskM);
    }
	
	@AfterEach
    private void tearDown() throws IOException{
        // Nettoyer après les tests en supprimant les fichiers générés
        Files.deleteIfExists(Paths.get(DBConfig.dbpath + "dm.save"));
        for (int i = 0; i < 10; i++) // Nettoyer tous les fichiers de test
            Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb"));
    }
	
    @Test // Test le constructeur de DiskManager
    void testConstructeur() throws Exception {
        assertTrue(bm.emptyBuffer.size() == DBConfig.bm_buffercount);
    }
    
    @Test
    void testGetPage() throws Exception{
    	//1.Test récupère une page
        PageId id = new PageId(); // Instance de PageId pour stocker l'identifiant de la page
        id = dskM.AllocPage();
    	ByteBuffer buffer = ByteBuffer.allocate(DBConfig.pagesize);
    	for (int j = 0; j < buffer.capacity(); j++) {
            buffer.put((byte) ('A')); // Remplir le buffer une lettre
        }
    	dskM.WritePage(id, buffer); // Écrire la page dans le disque
        ByteBuffer buffer2 = bm.getPage(id);
        
        while(buffer.hasRemaining() && buffer2.hasRemaining()) {
            byte a = buffer.get(); // Lire un octet du buffer d'écriture
            byte b = buffer2.get(); // Lire un octet du buffer de lecture
            // Vérifier que les octets sont identiques
            assertEquals(a, b, "Les octets lus ne correspondent pas."); // Vérifier que les octets lus sont identiques
        }
        
        //2. Test récupère la même page
        ByteBuffer buffer3 = bm.getPage(new PageId(0,0));
        assertEquals(buffer2, buffer3, "Les buffer sont différents");
    }
    
    @Test
    void testfreePage() throws Exception{
    	int nbPage = 2; // Nombre de fichiers à allouer
        PageId id = new PageId(); // Instance de PageId pour stocker l'identifiant de la page
        for (int i = 0; i> nbPage; i++) {
        	id = dskM.AllocPage();
        	ByteBuffer buffer = ByteBuffer.allocate(DBConfig.pagesize);
        	for (int j = 0; j < buffer.capacity(); j++) {
                buffer.put((byte) ('A' + (i % 26))); // Remplir le buffer une lettre
            }
        	dskM.WritePage(id, buffer); // Écrire la page dans le disque
        }
        bm.getPage(id);
        bm.freePage(id, true);
        assertEquals(bm.cadre.search(id).dirtyFlag, true, "Le flage n'est pas mis à dirty");
        assertTrue(bm.cadre.search(id).pointeurListe != null);
    }
    
    @Test
    void testSuppJunk() throws Exception{
    	PageId id = new PageId(); // Instance de PageId pour stocker l'identifiant de la page
        id = dskM.AllocPage();
    	ByteBuffer buffer = ByteBuffer.allocate(DBConfig.pagesize);
    	for (int j = 0; j < buffer.capacity(); j++) {
            buffer.put((byte) ('A')); // Remplir le buffer une lettre
        }
    	dskM.WritePage(id, buffer); // Écrire la page dans le disque
        bm.getPage(id);
        bm.freePage(id, true);
        bm.getPage(id);
        assertTrue(bm.cadre.search(id).pointeurListe == null);
    }
    
    @Test
    void testMakeSpace() throws Exception{
    	int nbPage = DBConfig.bm_buffercount +1; // Nombre de fichiers à allouer
        PageId id = new PageId(); // Instance de PageId pour stocker l'identifiant de la page
        PageId least = new PageId(0, 0);
        PageId most = new PageId(1, 0);
        for (int i = 0; i> nbPage; i++) {
        	id = dskM.AllocPage();
        	ByteBuffer buffer = ByteBuffer.allocate(DBConfig.pagesize);
        	for (int j = 0; j < buffer.capacity(); j++) {
                buffer.put((byte) ('A' + (i % 26))); // Remplir le buffer une lettre
            }
        	dskM.WritePage(id, buffer); // Écrire la page dans le disque
        	bm.getPage(id);
        	if(id == least) {
                bm.freePage(least, false);
        	}else if(id == most){
            	bm.getPage(id);
            	bm.getPage(id);
                bm.freePage(most, false);
        	}else {
        		bm.getPage(id);
        	}
        }
        String actual = dbc.bm_policy;
    	switch(actual) {
    	case "LRU":
    		assertTrue(bm.cadre.search(least) == null);
    		break;
    	case "MRU":
    		assertTrue(bm.cadre.search(most) == null);
		}
    }
    
    @Test
    void testSetCurrentReplacementPolicy() {
    	String actual = dbc.bm_policy;
    	switch(actual) {
    	case "LRU":
    		bm.SetCurrentReplacementPolicy("MRU");
    		assertEquals(dbc.bm_policy, "MRU");
    		break;
    	case "MRU":
    		bm.SetCurrentReplacementPolicy("LRU");
    		assertEquals(dbc.bm_policy, "LRU");
		}
    }
}