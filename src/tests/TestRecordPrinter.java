import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class TestRecordPrinter {
    private DiskManager dskM;
    private BufferManager bm;
    private DBConfig dbConfig;
    private  Relation relation1;
    private  Relation relation2;


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
        relation1 = createSpecialRelation("Relation1");
        addSpecialTuplesToRelation(relation1, 10);

        // Création des relations avec 10 tuples chacune
        relation2 = createSpecialRelation("Relation2");
        addSpecialTuplesToRelation(relation2, 10);
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
/*
    @Test
    void testPrint() throws Exception {
        selectOperator selectOp1 = new selectOperator(
            new PageDirectoryIterator(relation1, bm),
            relation1,
            new ArrayList<>(),  // Pas de conditions de sélection dans ce test
            bm
        );
        // Initialisation de la projection : on projette uniquement les attributs avec indices 0 et 2
        ArrayList<Integer> attrbToPrint = new ArrayList<>();
        attrbToPrint.add(2); // Projeter "value"
        attrbToPrint.add(0); // Projeter "id"

        ArrayList<String> nameAttrb = new ArrayList<>();
        nameAttrb.add("value"); // Projeter "value"
        nameAttrb.add("id"); // Projeter "id"

        // Crée l'opérateur de projection qui utilise l'opérateur SELECT comme scanner
        ProjectOperator projectOp = new ProjectOperator(selectOp1, attrbToPrint);

        RecordPrinter rp = new RecordPrinter(projectOp, nameAttrb);

        rp.printAllRecord();
    }

    @Test
    void testNoMatchingRecords() {
        try {
            ArrayList<Condition> conditions = new ArrayList<>();
            conditions.add(new Condition(new Pair<>("id", 0), "=", new Pair<>("'-1000'", -1)));

            selectOperator s = new selectOperator(new PageDirectoryIterator(relation1, bm), relation1, conditions, bm);

            // Initialisation de la projection : on projette uniquement les attributs avec indices 0 et 2
            ArrayList<Integer> attrbToPrint = new ArrayList<>();
            attrbToPrint.add(2); // Projeter "value"
            attrbToPrint.add(0); // Projeter "id"

            ArrayList<String> nameAttrb = new ArrayList<>();
            nameAttrb.add("value"); // Projeter "value"
            nameAttrb.add("id"); // Projeter "id"

            // Crée l'opérateur de projection qui utilise l'opérateur SELECT comme scanner
            ProjectOperator projectOp = new ProjectOperator(s, attrbToPrint);

            RecordPrinter rp = new RecordPrinter(projectOp, nameAttrb);

            rp.printAllRecord();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
*/
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
        attrbToPrint.add(2 + relation1.getNbAttribut()); // value de la deuxième table
        attrbToPrint.add(2); // value de la première table
        attrbToPrint.add(0); // id de la première table
        attrbToPrint.add(1 + relation1.getNbAttribut()); // name de la deuxième table

        ArrayList<String> nameAttrb = new ArrayList<>();
        nameAttrb.add("R2.value"); // Projeter "value"
        nameAttrb.add("R1.value"); // Projeter "id"
        nameAttrb.add("R1.id"); // Projeter "id"
        nameAttrb.add("R2.name"); // Projeter "id"

        ProjectOperator projectOp = new ProjectOperator(joinOp, attrbToPrint);
        RecordPrinter rp = new RecordPrinter(projectOp, nameAttrb);

        rp.printAllRecord();
    }
}