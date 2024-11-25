/*
 * Class pour filtrer dans un record les attribut que l'on veut afficher
 */
public class ProjectOperator implements IRecordIterator{
    RelationScanner scanner;   // l'opérateur fils est un select
    int[] attrbWeWant;   // Correspond aux index des attrb que l'on veut afficher

    ProjectOperator(RelationScanner rs, int[] attrbWeWant) {
        scanner = rs;
        this.attrbWeWant = attrbWeWant;
    }

    public MyRecord GetNextRecord() {
        MyRecord record = scanner.GetNextRecord();
        MyRecord res = new MyRecord();  // Cree un nouveau record resultat

        // Parcourt tout les index que l'on veut garder
        for (int index : attrbWeWant)
            res.add(record.get(index));

        return res; // Retourne le record moins ce qu'on ne veut pas
    }

    public void Close() {
        scanner.Close();
    }

    public void Reset() {
        scanner.Reset();
    }
}