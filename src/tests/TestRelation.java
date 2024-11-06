import java.nio.ByteBuffer;
import java.util.ArrayList;

public class TestRelation{
    public static void main(String[] args){
        // Initialiser la relation
        Relation relation = new Relation("MaRelation", new ArrayList<>());
        
        // Initialiser des attributs
        initAttributes(relation);
        
        // Creer un enregistrement à ecrire
        MyRecord recordToWrite = createTestRecord();
        
        // Creer un buffer pour stocker l'enregistrement
        ByteBuffer buffer = ByteBuffer.allocate(1000);

        // Ecrire l'enregistrement dans le buffer
        int position = relation.writeRecordToBuffer(recordToWrite, buffer, 123);
        System.out.println("Ocet écrit: " + position);

        // Lire l'enregistrement depuis le buffer
        MyRecord recordRead = new MyRecord();
        relation.readRecordFromBuffer(recordRead, buffer, 123);
        
        // Afficher les résultats
        printRecords(recordToWrite, recordRead, relation);

    }

    // Méthode pour initialiser les attributs de la relation
    private static void initAttributes(Relation relation) {
        relation.addAttribut(new Pair<>("ID", new Data(DataType.INT)));      // ID en tant qu'entier
        relation.addAttribut(new Pair<>("Nom", new Data(DataType.VARCHAR, 51))); // Nom en tant que chaîne
        relation.addAttribut(new Pair<>("Age", new Data(DataType.INT)));      // Age en tant qu'entier
        relation.addAttribut(new Pair<>("DateNaissance", new Data(DataType.DATE))); // Date de naissance
    }

    // Méthode pour créer un enregistrement de test
    private static MyRecord createTestRecord(){
        MyRecord record = new MyRecord();
        record.addValue(1, DataType.INT); // ID = 1
        record.addValue("Alice", DataType.VARCHAR);   // Nom = "Alice"
        record.addValue(30, DataType.INT);    // Age = 30
        record.addValue(new Date(1, 1, 1994), DataType.DATE); // Date de naissance = 1er Janvier 1994
        return record;
    }

    // Methode pour afficher les enregistrements
    private static boolean printRecords(MyRecord recordToWrite, MyRecord recordRead, Relation rela){
        System.out.println("Enregistrement écrit : ");
        for (int i = 0; i < recordToWrite.getSize(); i++)
            System.out.println("  " + rela.getType(i) + ": " + recordToWrite.getValue(i));

        System.out.println("Enregistrement lu : ");
        for (int i = 0; i < recordRead.getSize(); i++) {
            System.out.println("  " + rela.getType(i) + ": " + recordRead.getValue(i));
        }
        
        // Verifier si les enregistrements sont identiques
        for (int i = 0; i < recordToWrite.getSize(); i++){
            if (!recordToWrite.getValue(i).equals(recordRead.getValue(i))){
                return false;
            }
        }
        return true;
    }

    private static boolean cmpSize(MyRecord rec, Relation relation){
        int sum = 0;

        for(int i = 0; i < rec.getSize(); i++)
            sum += relation.getLength(i);

        System.out.println(sum);
        return true;
    }
}
