import org.junit.jupiter.api.BeforeEach; // Importation de l'annotation pour exécuter du code avant chaque test
import org.junit.jupiter.api.Test; // Importation de l'annotation pour marquer une méthode comme un test
import static org.junit.jupiter.api.Assertions.*; // Importation des assertions statiques pour effectuer des tests

import java.util.ArrayList;

public class TestDBManager {
	private DBManager DBM;
	private DiskManager dskM;
	private BufferManager bm;
	
	@BeforeEach
    private void setup() throws Exception{
        // Initialiser DiskManager et charger la configuration avant chaque test
        dskM = DiskManager.getInstance();
        DBConfig dbc = DBConfig.loadConfig("./config.txt");
        DBConfig.dbpath = "././";
        bm = new BufferManager(dbc, dskM);
        
        DBM = new DBManager(dbc, dskM, bm);
    }
	
	@Test
	void testCreateDatabase() {
		DBM.CreateDatabase("bddtest");
		assertTrue(DBM.getListeDatabase().containsKey("bddtest"));
	}
	
	@Test
	void testSetCurrentDatabase() {
		DBM.CreateDatabase("bddtest");
		try{
			DBM.SetCurrentDatabase("bddtest");
			assertEquals(DBM.getCurrentDatabase(), DBM.getListeDatabase().get("bddtest"));
		}catch(Exception e) {
			e.getMessage();
		}
		assertThrows(Exception.class, () -> {DBM.SetCurrentDatabase("bdtest");});
	}
	
	@Test
	void testAddAndGetTableToCurrentDatabase() {
		DBM.CreateDatabase("bddtest");
		Relation rel = new Relation("test", new ArrayList<Pair<String, Data>>(), new PageId(0, 0), dskM, bm);
		rel.setRelationName("relTest");
		assertThrows(Exception.class, () -> {DBM.AddTableToCurrentDatabase(rel);});
		assertThrows(Exception.class, () -> {DBM.GetTableFromCurrentDatabase("relTest");});
		try{
			DBM.SetCurrentDatabase("bddtest");
			DBM.AddTableToCurrentDatabase(rel);
			Relation rel2 = DBM.GetTableFromCurrentDatabase("relTest");
			assertEquals(rel, rel2);
			assertThrows(Exception.class, () -> {DBM.GetTableFromCurrentDatabase("relTest");});
		}catch(Exception e) {
			e.getMessage();
		}
	}
	
	@Test
	void testRemoveDataBase() {
		DBM.CreateDatabase("bddtest");
		try{
			DBM.RemoveDatabase("bddtest");
			assertFalse(DBM.getListeDatabase().containsKey("bddtest"));
			assertThrows(Exception.class, () -> {DBM.RemoveDatabase("bddtest");});
		}catch(Exception e) {
			e.getMessage();
		}
	}
	
	@Test
	void testRemoveDataBases() {
		DBM.CreateDatabase("bddtest");
		DBM.CreateDatabase("bddtest2");
		try{
			DBM.RemoveDatabases();
			assertFalse(DBM.getListeDatabase().containsKey("bddtest"));
			assertFalse(DBM.getListeDatabase().containsKey("bddtest2"));
		}catch(Exception e) {
			e.getMessage();
		}
	}
	
	@Test
	void testRemoveTableFromCurrentDatabase() {
		DBM.CreateDatabase("bddtest");
		Relation rel = new Relation("test", new ArrayList<Pair<String, Data>>(), new PageId(0, 0), dskM, bm);
		rel.setRelationName("relTest");
		Relation rel2 = new Relation("tests", new ArrayList<Pair<String, Data>>(), new PageId(0, 0), dskM, bm);
		rel2.setRelationName("relTestdeux");
		assertThrows(Exception.class, () -> {DBM.RemoveTableFromCurrentDatabase("relTest");});
		try{
			DBM.SetCurrentDatabase("bddtest");
			DBM.AddTableToCurrentDatabase(rel);
			DBM.AddTableToCurrentDatabase(rel2);
			DBM.RemoveTableFromCurrentDatabase("relTest");
			assertFalse(DBM.getCurrentDatabase().containsKey("relTest"));
			assertTrue(DBM.getCurrentDatabase().containsKey("relTestdeux"));
			assertThrows(Exception.class, () -> {DBM.RemoveTableFromCurrentDatabase("relTestss");});
			
		}catch(Exception e) {
			e.getMessage();
		}
	}
	
	@Test
	void testRemoveTablesFromCurrentDatabase() {
		DBM.CreateDatabase("bddtest");
		Relation rel = new Relation("test", new ArrayList<Pair<String, Data>>(), new PageId(0, 0), dskM, bm);
		rel.setRelationName("relTest");
		Relation rel2 = new Relation("tests", new ArrayList<Pair<String, Data>>(), new PageId(0, 0), dskM, bm);
		rel2.setRelationName("relTestdeux");
		assertThrows(Exception.class, () -> {DBM.RemoveTablesFromCurrentDatabase();});
		try{
			DBM.SetCurrentDatabase("bddtest");
			DBM.AddTableToCurrentDatabase(rel);
			DBM.AddTableToCurrentDatabase(rel2);
			DBM.RemoveTablesFromCurrentDatabase();
			assertFalse(DBM.getCurrentDatabase().containsKey("relTest"));
			assertFalse(DBM.getCurrentDatabase().containsKey("relTestdeux"));
			
		}catch(Exception e) {
			e.getMessage();
		}
	}
	
	@Test
	void testLoadSaveState(){
		DBM.CreateDatabase("bddtest");
		DBM.CreateDatabase("bddtest2");
		Relation rel = new Relation("relTest", new ArrayList<>(), new PageId(3, 0), dskM, bm);
		rel.setOneAttribut(new Pair<>("coltest", new Data(DataType.CHAR, 20)));
		rel.setOneAttribut(new Pair<>("coltestdeux", new Data(DataType.INT)));
		
		Relation rel2 = new Relation("relTestdeux", new ArrayList<>(), new PageId(0, 2), dskM, bm);
		rel2.setOneAttribut(new Pair<>("coltesttrois", new Data(DataType.DATE)));
		
		Relation rel3 = new Relation("relTesttrois", new ArrayList<>(), new PageId(3, 2), dskM, bm);
		try{
			DBM.SetCurrentDatabase("bddtest");
			DBM.AddTableToCurrentDatabase(rel);
			DBM.AddTableToCurrentDatabase(rel2);
			DBM.SetCurrentDatabase("bddtest2");
			DBM.AddTableToCurrentDatabase(rel3);
		}catch(Exception e) {
			e.getMessage();
		}
		DBM.saveState();
		
		DBM.RemoveDatabases();
		try{
			DBM.loadState();
			assertDoesNotThrow(() -> {DBM.SetCurrentDatabase("bddtest");});
			DBM.ListTablesInCurrentDatabase();
			
			assertTrue(DBM.getCurrentDatabase().containsKey("relTest"));
			Relation relpart2 = DBM.GetTableFromCurrentDatabase("relTest");
			assertEquals(relpart2.getNameAttribut(0), "coltest");
			assertEquals(relpart2.getType(0), DataType.CHAR);
			assertEquals(relpart2.getLength(0), 20);
			assertEquals(relpart2.getNameAttribut(1), "coltestdeux");
			assertEquals(relpart2.getType(1), DataType.INT);
			
			assertTrue(DBM.getCurrentDatabase().containsKey("relTestdeux"));
			Relation rel2part2 = DBM.GetTableFromCurrentDatabase("relTestdeux");
			assertEquals(rel2part2.getNameAttribut(0), "coltesttrois");
			assertEquals(rel2part2.getType(0), DataType.DATE);
			
			assertDoesNotThrow(() -> {DBM.SetCurrentDatabase("bddtest2");});
			
			assertTrue(DBM.getCurrentDatabase().containsKey("relTesttrois"));
			
			DBM.ListDatabases();
		}catch(Exception e) {
			e.getMessage();
		}
	}
	
}
