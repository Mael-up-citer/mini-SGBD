import java.nio.ByteBuffer;

public class DataPageHoldRecordIterator implements IRecordIterator {
    int offsetRecord = DBConfig.pagesize-8;
    int nbRecord;

    Relation relation;
    BufferManager bm;
    ByteBuffer buffer;
    PageId dataPageId;

    DataPageHoldRecordIterator(Relation relation, BufferManager bm, PageId dataPageId) throws Exception {
        this.relation = relation;
        this.bm = bm;
        this.dataPageId = dataPageId;

        // Charge dans le buffer la data Page
        buffer = bm.getPage(dataPageId);
        nbRecord = buffer.getInt(8);
    }

    public MyRecord GetNextRecord() {
        // Si on a fini d'extraire les record de cette page
        if (nbRecord == 0)
            return null;    // Retourne null

        MyRecord res;
        int pos = buffer.getInt(nbRecord);

        return null;
    }

    public void Reset() {
        offsetRecord = 0;
    }

    public void Close() {
        buffer = null;
        bm.freePage(dataPageId, false);
    }
}