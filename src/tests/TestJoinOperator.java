import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class TestJoinOperator {
    private DiskManager dskM;
    private BufferManager bm;
    private DBConfig dbConfig;
    private Relation relation1;
    private Relation relation2;

    selectOperator select1;
    selectOperator select2;
    private JoinOperator joinOperator;

    @BeforeEach
    void setup() throws Exception {
        // Initialisation des gestionnaires simulés ou mockés
        dskM = DiskManager.getInstance();
        dskM.RAZ();
        dbConfig = DBConfig.loadConfig("src/tests/config.txt");
        DBConfig.pagesize = 250;
        DBConfig.dm_maxfilesize = 500;

        bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

        // Création des deux relations avec 10 tuples chacune
        relation1 = createRelation("Relation1");
        addTuplesToRelation(relation1, 100);

        relation2 = createRelation("Relation2");
        addTuplesToRelation(relation2, 100);

        select1 = new selectOperator(new PageDirectoryIterator(relation1, bm), relation1, new ArrayList<>(), bm);
        select2 = new selectOperator(new PageDirectoryIterator(relation2, bm), relation2, new ArrayList<>(), bm);
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
    void testTuplesMatchExpected() throws Exception {
        // Création de l'opérateur de jointure
        joinOperator = new JoinOperator(new Pair<>(select1, select2), new ArrayList<>());

        // Crée une liste des tuples attendus en utilisant les itérateurs de select
        ArrayList<MyRecord> expectedTuples = new ArrayList<>();
        MyRecord rec1 = new MyRecord();
        MyRecord rec2 = new MyRecord();

        IRecordIterator newSelect1 = new selectOperator(new PageDirectoryIterator(relation1, bm), relation1, new ArrayList<>(), bm);
        IRecordIterator newSelect2 = new selectOperator(new PageDirectoryIterator(relation2, bm), relation2, new ArrayList<>(), bm);

        // Crée le produit cartésien des deux relations
        while ((rec1 = newSelect1.GetNextRecord()) != null) {
            newSelect2.Reset();
            while ((rec2 = newSelect2.GetNextRecord()) != null) {
                MyRecord expected = new MyRecord();

                // Combine les deux tuples pour le produit cartésien
                expected.addAll(rec1);
                expected.addAll(rec2);

                expectedTuples.add(expected);
            }
        }

        // Parcours des tuples générés par l'opérateur et vérifie qu'ils correspondent aux attendus
        int index = 0;
        MyRecord actualTuple;

        while ((actualTuple = joinOperator.GetNextRecord()) != null) {
            assertEquals(expectedTuples.get(index), actualTuple, "Tuple incorrect à l'index " + index);
            index++;
        }
        // Vérifie qu'il n'y a pas de tuples manquants
        assertEquals(expectedTuples.size(), index, "Nombre incorrect de tuples générés.");
    }

    @Test
    void testReset() throws Exception {
        joinOperator = new JoinOperator(new Pair<>(select1, select2), new ArrayList<>());

        // Parcours initial des tuples
        while (joinOperator.GetNextRecord() != null);
            // Rien à faire, juste consommer tous les tuples

        // Réinitialisation
        joinOperator.Reset();

        // Vérifie que tous les tuples sont à nouveau disponibles
        int count = 0;

        while (joinOperator.GetNextRecord() != null) count++;

        assertEquals(10000, count);
    }

    @Test
    void testClose() throws Exception{
        joinOperator = new JoinOperator(new Pair<>(select1, select2), new ArrayList<>());
        // Ferme l'opérateur
        joinOperator.Close();

        assertNull(joinOperator.GetNextRecord());
    }
}