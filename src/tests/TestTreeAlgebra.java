import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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

        conditions.add(new Condition(new Pair<>("value", 2), "=", new Pair<>("10", -1))); // La colonne "value" a un indice de 2
        innerConditions.put("Relation1", conditions);

        ArrayList<Condition> joinConditions = new ArrayList<>();
        ArrayList<Integer> attbToPrint = new ArrayList<>(List.of(0, 1, 2));
        ArrayList<String> attrbName = new ArrayList<>(List.of("id", "name", "value"));

        treeAlgebra tree = new treeAlgebra(relations, joinConditions, innerConditions, attbToPrint, attrbName, bm);
        tree.execute(); // Vérifie si l'exécution filtre correctement les résultats
    }
/*
    @Test
    void testMultipleRelationsWithJoinConditionsAndIndices() throws Exception {
        // Tester avec plusieurs relations et des conditions de jointure en utilisant des indices absolus
        List<Relation> relations = List.of(relation1, relation2, relation3);

        HashMap<String, ArrayList<Pair<String, Integer>>> innerConditions = new HashMap<>();
        ArrayList<Pair<String, Integer>> conditionsRelation1 = new ArrayList<>();
        conditionsRelation1.add(new Pair<>("id", 0)); // La colonne "id" dans relation1 a un indice de 0
        innerConditions.put("Relation1", conditionsRelation1);

        ArrayList<Pair<Integer, Integer>> joinConditions = new ArrayList<>();
        // La colonne "id" de relation1 correspond à l'indice 0 absolu, et celle de relation2 à l'indice 3 (décalage de 3 attributs)
        joinConditions.add(new Pair<>(0, 3)); 

        ArrayList<Integer> attbToPrint = new ArrayList<>(List.of(0, 1, 2, 3, 4, 5));
        ArrayList<String> attrbName = new ArrayList<>(List.of("id", "name", "value", "id2", "name2", "value2"));

        treeAlgebra tree = new treeAlgebra(relations, joinConditions, innerConditions, attbToPrint, attrbName, bm);
        tree.execute(); // Vérifie si l'exécution applique correctement les jointures et les conditions
    }
*/
}