import org.junit.jupiter.api.*;

public class TestSGBD {

    private DBManager dbM; // Instance réelle de DBManager
    private SGBD sgbd; // Instance du SGBD à tester

    @BeforeEach
    void setUp() throws Exception{
        // Initialisation des objets nécessaires à chaque test
        DBConfig dbc = DBConfig.loadConfig("src/tests/config.txt");
        sgbd = new SGBD(dbc); // Instanciation du SGBD
        dbM = sgbd.getDBManager();
    }

    @Test
    void testCreateAndManageDatabasesAndTables() {
        // Commande 1: CREATE DATABASE Db1
        String createDb1 = "CREATE DATABASE Db1";
        sgbd.assocQuery(createDb1);

        // Commande 2: SET DATABASE Db1
        String setDb1 = "SET DATABASE Db1";
        sgbd.assocQuery(setDb1);

        // Commande 3: CREATE TABLE Tab1 (C1:REAL,C2:INT)
        String createTable1 = "CREATE TABLE Tab1 (C1:REAL,C2:INT)";
        sgbd.assocQuery(createTable1);

        // Commande 4: CREATE DATABASE Db2
        String createDb2 = "CREATE DATABASE Db2";
        sgbd.assocQuery(createDb2);

        // Commande 5: SET DATABASE Db2
        String setDb2 = "SET DATABASE Db2";
        sgbd.assocQuery(setDb2);

        // Commande 6: CREATE TABLE Tab1 (C7:CHAR(5),AA:VARCHAR(2))
        String createTable2 = "CREATE TABLE Tab1 (C7:CHAR(5),AA:VARCHAR(2))";
        sgbd.assocQuery(createTable2);

        // Commande 7: CREATE TABLE Tab2 (Toto:CHAR(120))
        String createTable3 = "CREATE TABLE Tab2 (Toto:CHAR(120))";
        sgbd.assocQuery(createTable3);

        // Commande 8: LIST TABLES
        String listTables = "LIST TABLES";
        sgbd.assocQuery(listTables);

        // Commande 9: DROP TABLE Tab1
        String dropTable1 = "DROP TABLE Tab1";
        sgbd.assocQuery(dropTable1);

        // Commande 10: LIST TABLES
        sgbd.assocQuery(listTables);

        // Commande 11: SET DATABASE Db1
        String setDb1Again = "SET DATABASE Db1";
        sgbd.assocQuery(setDb1Again);

        // Commande 12: LIST TABLES
        sgbd.assocQuery(listTables);

        // Commande 13: LIST DATABASES
        String listDatabases = "LIST DATABASES";
        sgbd.assocQuery(listDatabases);

        // Commande 14: DROP DATABASE Db2
        String dropDb2 = "DROP DATABASE Db2";
        sgbd.assocQuery(dropDb2);

        // Commande 15: LIST DATABASES
        sgbd.assocQuery(listDatabases);
    }
}