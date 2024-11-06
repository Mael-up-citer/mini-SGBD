
public class TestRecord{
    private static MyRecord record = new MyRecord();

    public static void main(String[] args){
        initTuple();

        // Afficher tous les elements du tuple
        System.out.println("Tuple: ");
        record.display();   // Affiche la liste des elements

    }

    // Ajouter une valeur de chauqe type au tuple
    private static void initTuple(){
        record.addValue(123, DataType.INT);   // Integer
        record.addValue("Hello World", DataType.CHAR); // String
        record.addValue(new Date(11, 9, 2024),DataType.DATE); // Date
        record.addValue(23.21f, DataType.REAL); // Float
    }
}
