import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TestProjectOperator {
    private DiskManager dskM;
    private BufferManager bm;
    private DBConfig dbConfig;
    private  Relation relation;
    
    // Attributs pour les tests
    private selectOperator selectOp;
    private ArrayList<Integer> attrbToPrint; // Liste des indices des attributs à projeter

    @BeforeEach
    void setup() throws Exception {
        // Initialisation des gestionnaires simulés ou mockés
        dskM = DiskManager.getInstance();
        dskM.RAZ();
        dbConfig = DBConfig.loadConfig("src/tests/config.txt");
        DBConfig.pagesize = 2500;
        DBConfig.dm_maxfilesize = 5000;

        bm = new BufferManager(dbConfig, dskM); // Réinitialiser le BufferManager

        // Création des relations avec 10 tuples chacune
        relation  =createRelation("Relation1");
        addTuplesToRelation(relation, 10);

        // Initialisation de la projection : on projette uniquement les attributs avec indices 0 et 2
        attrbToPrint = new ArrayList<>();
        attrbToPrint.add(0); // Projeter "id"
        attrbToPrint.add(2); // Projeter "value"

        // Création de l'opérateur SELECT sur relation
        selectOp = new selectOperator(
            new PageDirectoryIterator(relation, bm),
            relation,
            new ArrayList<>(),  // Pas de conditions de sélection dans ce test
            bm
        );
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
    void testProjection() {
        // Crée l'opérateur de projection qui utilise l'opérateur SELECT comme scanner
        ProjectOperator projectOp = new ProjectOperator(selectOp, attrbToPrint);

        // Récupère le premier record projeté
        MyRecord projectedRecord = projectOp.GetNextRecord();

        // Vérifie que le record projeté a seulement deux attributs (id et value)
        assertNotNull(projectedRecord, "Le record projeté ne doit pas être nul.");
        assertEquals(2, projectedRecord.size(), "Le record projeté doit contenir deux attributs.");

        // Vérifie que les attributs dans le record projeté sont ceux que l'on attend
        assertEquals(1, projectedRecord.get(0).getFirst(), "Le premier attribut projeté (id) doit avoir la valeur 1.");
        assertEquals(1.1f, projectedRecord.get(1).getFirst(), "Le deuxième attribut projeté (value) doit avoir la valeur 1.1.");
    }

    @Test
    void testNoMatchingRecords() {
        try {
            ArrayList<Condition> conditions = new ArrayList<>();
            conditions.add(new Condition(new Pair<>("id", 0), "=", new Pair<>("-1000", -1)));

            // Crée un opérateur SELECT avec des conditions qui n'existent pas
            selectOperator faultySelectOp = new selectOperator(
                new PageDirectoryIterator(relation, bm),
                relation,
                conditions,  // Pas de conditions valides
                bm
            );

            ProjectOperator projectOp = new ProjectOperator(faultySelectOp, attrbToPrint);

            // Vérifie qu'aucun record n'est renvoyé
            MyRecord projectedRecord = projectOp.GetNextRecord();
            assertNull(projectedRecord, "Le record projeté ne doit pas être trouvé si les conditions ne correspondent à aucun enregistrement.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testReset() {
        // Crée l'opérateur de projection
        ProjectOperator projectOp = new ProjectOperator(selectOp, attrbToPrint);

        // Récupère le premier record projeté
        MyRecord projectedRecord1 = projectOp.GetNextRecord();

        // Vérifie qu'un record a été renvoyé
        assertNotNull(projectedRecord1, "Le premier record projeté ne doit pas être nul.");

        // Réinitialise l'opérateur
        projectOp.Reset();

        // Récupère à nouveau le premier record projeté après la réinitialisation
        MyRecord projectedRecord2 = projectOp.GetNextRecord();

        // Vérifie que le premier record projeté après réinitialisation est le même que celui avant
        assertNotNull(projectedRecord2, "Le record projeté après réinitialisation ne doit pas être nul.");
        assertEquals(projectedRecord1.get(0).getFirst(), projectedRecord2.get(0).getFirst(), "Le premier attribut doit être le même.");
        assertEquals(projectedRecord1.get(1).getFirst(), projectedRecord2.get(1).getFirst(), "Le deuxième attribut doit être le même.");
    }

    @Test
    void testClose() {
        // Crée l'opérateur de projection
        ProjectOperator projectOp = new ProjectOperator(selectOp, attrbToPrint);

        // Récupère le premier record projeté
        MyRecord projectedRecord1 = projectOp.GetNextRecord();

        // Vérifie qu'un record a été renvoyé
        assertNotNull(projectedRecord1, "Le premier record projeté ne doit pas être nul.");

        // Ferme l'opérateur
        projectOp.Close();

        // Récupère le record après la fermeture
        MyRecord projectedRecord2 = projectOp.GetNextRecord();

        // Vérifie qu'aucun record n'est renvoyé après la fermeture
        assertNull(projectedRecord2, "Aucun record ne doit être renvoyé après la fermeture de l'opérateur.");
    }

    @Test
    void testProjectionWithJoinAndCondition() throws Exception {
        // Création de deux relations (tables) avec des tuples
        Relation relation1 = createSpecialRelation("Relation1");
        addSpecialTuplesToRelation(relation1, 10);

        Relation relation2 = createSpecialRelation("Relation2");
        addSpecialTuplesToRelation(relation2, 10);

        // Conditions pour la jointure (égalité entre les ids)
        ArrayList<Condition> joinConditions = new ArrayList<>();
        joinConditions.add(new Condition(new Pair<>("id", 0), "<", new Pair<>("id", 0 + relation1.getNbAttribut())));

        ArrayList<Condition> table1Conditions = new ArrayList<>();
        table1Conditions.add(new Condition(new Pair<>("value", 2), ">", new Pair<>("'"+relation1.getRelationName() + "_Value_" + 3+"'", -1)));

        // Création de l'opérateur SELECT pour chaque table
        selectOperator selectOp1 = new selectOperator(
            new PageDirectoryIterator(relation1, bm),
            relation1,
            table1Conditions,  // Applique la condition sur la table 1
            bm
        );
        selectOperator selectOp2 = new selectOperator(
            new PageDirectoryIterator(relation2, bm),
            relation2,
            new ArrayList<>(),  // Aucune condition pour la table 2
            bm
        );

        // Création de l'opérateur de jointure entre les deux tables sur la colonne "id"
        IRecordIterator joinOp = new PageOrientedJoinOperator(
            new Pair<>(selectOp1, selectOp2),
            joinConditions
        );

        // Création de l'opérateur de projection pour afficher "id" et "value" des deux tables
        ArrayList<Integer> attrbToPrint = new ArrayList<>();

        // Attributs de la première table (Relation 1)
        attrbToPrint.add(1 + relation1.getNbAttribut()); // value de la deuxième table
        attrbToPrint.add(0); // value de la première table
        attrbToPrint.add(1); // name de la première table
        attrbToPrint.add(2); // value de la première table
        attrbToPrint.add(2 + relation1.getNbAttribut()); // name de la deuxième table

        ProjectOperator projectOp = new ProjectOperator(joinOp, attrbToPrint);

        // Itérer à travers tous les records projetés et vérifier
        MyRecord projectedRecord;
        while ((projectedRecord = projectOp.GetNextRecord()) != null) {
            // Vérifie la taille du record projeté
            int expectedSize = 5; // La taille attendue : 5 attributs projetés
            assertEquals(expectedSize, projectedRecord.size(), "La taille du record projeté n'est pas correcte.");

            // Vérifie l'ordre des attributs dans le record projeté
            // L'ordre attendu des attributs est : 
            // 1. Name de la deuxième table (Relation 2)
            // 2. Id de la première table (Relation 1)
            // 3. Name de la première table (Relation 1)
            // 4. Value de la première table (Relation 1)
            // 5. Value de la deuxième table (Relation 2)

            // Vérifie que les attributs correspondent bien à l'ordre spécifié

            // Name de la table 2
            String table2Name = (String) projectedRecord.get(0).getFirst();
            assertNotNull(table2Name, "Le nom de la table 2 ne doit pas être nul.");
            assertTrue(table2Name.startsWith("RELATION2_NAME_"), "Le nom de la table 2 doit commencer par 'Relation2_Name_' but was "+table2Name);

            // Id de la table 1
            String table1Id = (String) projectedRecord.get(1).getFirst();
            assertNotNull(table1Id, "L'id de la table 1 ne doit pas être nul.");
            assertTrue(table1Id.startsWith("RELATION1_ID_"), "L'id de la table 1 doit commencer par 'Relation1_ID_'.");

            // Name de la table 1
            String table1Name = (String) projectedRecord.get(2).getFirst();
            assertNotNull(table1Name, "Le nom de la table 1 ne doit pas être nul.");
            assertTrue(table1Name.startsWith("RELATION1_NAME_"), "Le nom de la table 1 doit commencer par 'Relation1_Name_'.");

            // Value de la table 1
            String table1Value = (String) projectedRecord.get(3).getFirst();
            assertNotNull(table1Value, "La valeur de la table 1 ne doit pas être nulle.");

            // Value de la table 2
            String table2Value = (String) projectedRecord.get(4).getFirst();
            assertNotNull(table2Value, "La valeur de la table 2 ne doit pas être nulle.");
        }
    }

    private Relation createSpecialRelation(String relationName) throws Exception{
        // Allocation d'une page d'en-tête pour la relation
        PageId headerPageId = dskM.AllocPage();
        ByteBuffer buffer = bm.getPage(headerPageId);
        buffer.putInt(DBConfig.pagesize - 4, -1); // Indique qu'il n'y a pas de page suivante
        bm.freePage(headerPageId, true);

        // Définition des attributs de la relation
        ArrayList<Pair<String, Data>> attributes = new ArrayList<>();
        attributes.add(new Pair<>("id", new Data(DataType.VARCHAR, 32)));
        attributes.add(new Pair<>("name", new Data(DataType.VARCHAR, 32)));
        attributes.add(new Pair<>("value", new Data(DataType.CHAR, 32)));

        // Création et retour de la relation
        return new Relation(relationName, attributes, headerPageId, dskM, bm);
    }

    private void addSpecialTuplesToRelation(Relation relation, int numTuples) throws Exception {
        for (int i = 1; i <= numTuples; i++) {
            MyRecord record = new MyRecord();

            // Ajout d'un ID de relation comme préfixe
            // Ajoute l'ID unique de la relation dans le tuple pour le différencier
            record.add(relation.getRelationName()+ "_ID_" + i, DataType.VARCHAR); // ID de la relation et numéro du tuple
            record.add(relation.getRelationName()+ "_Name_" + i, DataType.VARCHAR); // Nom unique avec le numéro de relation
            record.add(relation.getRelationName() + "_Value_" + (i * 1.1f), DataType.CHAR); // Value unique, basé sur i et la relation

            // Insérer le tuple dans la relation
            relation.InsertRecord(record);
        }
    }
}