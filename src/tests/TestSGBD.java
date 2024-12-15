import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class TestSGBD {
    private DBConfig dbConfig;
    private SGBD sgbd;

    private ArrayList<Relation> relations;

    @BeforeEach
    void setup() throws Exception {
        // Initialisation des gestionnaires simulés ou mockés
        dbConfig = DBConfig.loadConfig("src/tests/config.txt");
        DBConfig.pagesize = 2500;
        DBConfig.dm_maxfilesize = 5000;
        DBConfig.bm_buffercount = 1028;
        sgbd = new SGBD(dbConfig);

        // Création des deux relations avec 10 tuples chacune
        relations.add(createRelation("Relation1"));
        addTuplesToRelation(relations.get(0), 100);

        relations.add(createRelation("Relation2"));
        addTuplesToRelation(relations.get(1), 100);
    }

    private Relation createRelation(String relationName) throws Exception {
        // Allocation d'une page d'en-tête pour la relation
        PageId headerPageId = sgbd.getDskM().AllocPage();
        ByteBuffer buffer = sgbd.getBm().getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Indique qu'il n'y a pas de page suivante
        sgbd.getBm().freePage(headerPageId, true);

        // Définition des attributs de la relation
        ArrayList<Pair<String, Data>> attributes = new ArrayList<>();
        attributes.add(new Pair<>("id", new Data(DataType.INT)));
        attributes.add(new Pair<>("name", new Data(DataType.VARCHAR, 32)));
        attributes.add(new Pair<>("value", new Data(DataType.REAL)));

        // Création et retour de la relation
        return new Relation(relationName, attributes, headerPageId, sgbd.getDskM(), sgbd.getBm());
    }

    private void addTuplesToRelation(Relation relation, int numTuples) throws Exception {
        for (int i = 1; i <= numTuples; i++) {
            MyRecord record = new MyRecord();

            record.add(i, DataType.INT); // ID
            record.add("Name" + i, DataType.VARCHAR); // Name
            record.add((float) i * 1.1f, DataType.REAL); // Value
            relation.InsertRecord(record);
        }
    }

    @Test
    void testProcessSELECTCommand(){
        
    }

    @Test
    void testCreateAndManageDatabasesAndTables() throws Exception {
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

    @Test
    void testSelectMonoTable() {
        
    }
}