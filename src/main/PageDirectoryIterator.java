import java.nio.ByteBuffer;

public class PageDirectoryIterator {
    int nbHeaderPage = 1;
    int offsetDataPage = 4;
    int nbDataPage;
    Relation relation;
    BufferManager bm;

    PageDirectoryIterator(Relation rela, BufferManager bm) throws Exception{
        relation = rela;
        this.bm = bm;
        nbDataPage = bm.getPage(relation.getHeaderPageId()).getInt(0);
    }

    PageId GetNextDataPageId() {
        // Si on a parcouru toute les data Page
        if (nbDataPage == 0)
            return null;    // Retourne null

        PageId current = relation.getHeaderPageId();

        try {
            // Charge la 1er header page
            ByteBuffer buffer = bm.getPage(relation.getHeaderPageId());

            // Si on est sur une autre header Page plus loin dans le chainage
            if (nbHeaderPage > 1) {
                int cpt = nbHeaderPage;

                // Anvance jusqu'a etre sur le bonne header Page
                while (cpt != 0) { 
                    PageId tmp = new PageId(
                        buffer.getInt(DBConfig.pagesize),
                        buffer.getInt(DBConfig.pagesize+4)
                    );
                    // Si on a du chainage
                    if (tmp.PageIdx != -1) {
                        // Libère la page courante
                        bm.freePage(current, false);
                        // Charge la suivante
                        buffer = bm.getPage(current);
                    }
                }
                
            }
            // extrait l'@ de la data page
            PageId res = new PageId(buffer.getInt(offsetDataPage), buffer.getInt(offsetDataPage+4));
            offsetDataPage += 12;   // Augmente le compteur

            // change de header Page
            if (offsetDataPage > (DBConfig.pagesize-(8+12))) {
                offsetDataPage = 0;
                nbHeaderPage++;
            }
            bm.freePage(current, false);
            return res;
        } catch(Exception e) {
            bm.freePage(current, false);
        }
        return null;
    }

    public void reset() {
        nbHeaderPage = 1;
        offsetDataPage = 4;
    }

    public void close() {
        nbHeaderPage = 1;
        offsetDataPage = 4;
    }
}