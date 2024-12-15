import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestTreeAlgebra {
    private DiskManager dskM;
    private BufferManager bm;
    private DBConfig dbConfig;
    private ArrayList<Relation> relations = new ArrayList<>();

    @BeforeEach
    void setup() throws Exception {
        // Initialisation des gestionnaires simulés ou mockés
        dskM = DiskManager.getInstance();
        dskM.RAZ();
        dbConfig = DBConfig.loadConfig("src/tests/config.txt");
        DBConfig.pagesize = 250;
        DBConfig.dm_maxfilesize = 500;

        bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

        // Création des relations avec 10 tuples chacune
        for (int i = 0; i < 3; i++) {
            relations.add(createRelation("Relation"+i));
            addTuplesToRelation(relations.get(i), 10);
        }
    }

    private Relation createRelation(String relationName) throws Exception {
        // Allocation d'une page d'en-tête pour la relation
        PageId headerPageId = dskM.AllocPage();
        ByteBuffer buffer = bm.getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Indique qu'il n'y a pas de page suivante
        bm.freePage(headerPageId, true);

        // Définition des attributs de la relation
        ArrayList<Pair<String, Data>> attributes = new ArrayList<>();
        attributes.add(new Pair<>("id", new Data(DataType.INT)));
        attributes.add(new Pair<>("name", new Data(DataType.VARCHAR, 32)));
        attributes.add(new Pair<>("value", new Data(DataType.REAL)));

        // Création et retour de la relation
        return new Relation(relationName, attributes, headerPageId, dskM, bm);
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
    void testSingleRelationWithIndices() throws Exception {
        // Tester avec une seule relation, en utilisant les indices des conditions internes
        HashMap<String, ArrayList<Condition>> innerConditions = new HashMap<>();
        ArrayList<Condition> conditions = new ArrayList<>();

        int index2 = relations.get(1).getNbAttribut();
        int index3 = index2+relations.get(2).getNbAttribut();

        conditions.add(new Condition(new Pair<>("id", 0), ">", new Pair<>("5", -1)));
        innerConditions.put("RELATION2", conditions);

        ArrayList<Condition> joinConditions = new ArrayList<>();
        joinConditions.add(new Condition(new Pair<>("id", 0), "=", new Pair<>("id", index2)));

        ArrayList<Integer> attbToPrint = new ArrayList<>(List.of(0, index2, index3));
        ArrayList<String> attrbName = new ArrayList<>(List.of("R1.id", "R2.id", "R3.id"));

        TreeAlgebra tree = new TreeAlgebra(relations, joinConditions, innerConditions, attbToPrint, attrbName, bm);
        tree.execute(); // Vérifie si l'exécution filtre correctement les résultats
    }
}