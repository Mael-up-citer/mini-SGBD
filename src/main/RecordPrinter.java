public class RecordPrinter {
    IRecordIterator recordIterator;

    RecordPrinter(IRecordIterator recordIterator) {
        this.recordIterator = recordIterator;
    }

    public void printAllRecord() {
        MyRecord record;    // Record courrant à afficher
        int cpt = 0;    // Compteur du nombre de record afficher

        while ((record = recordIterator.GetNextRecord()) != null) {
            System.out.println(record); // Affiche le record
            cpt++;  // Incrémente le compteur
        }
        System.out.println("Total records="+cpt);
        recordIterator.Close();
    }
}