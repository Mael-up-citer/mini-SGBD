public class RecordPrinter {
    IRecordIterator recordIterator;

    RecordPrinter(IRecordIterator recordIterator) {
        this.recordIterator = recordIterator;
    }

    public void printAllRecord() {
        MyRecord record;

        while ((record = recordIterator.GetNextRecord()) != null)
            System.out.println(record);
    }
}