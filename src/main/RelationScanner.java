import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RelationScanner implements IRecordIterator{
    private Relation relation;  // Pointeur vers une instance de relation
    private List<PageId> dataPageId;    // Contient tout les pages id des data Page de la relation
    private BufferManager bm;   // Pointeur vers une instance de BufferManager pour récuperer les data Page
    private ByteBuffer buffer;  // Contient la data Page courante
    private int index = 0;  // Avancement dans la list des data Page
    private int offset; // Offset de lecture du record dans le buffer

    private ArrayList<Condition> conditions;

    RelationScanner(Relation relation, BufferManager bm, ArrayList<Condition> conditions) throws Exception{
        dataPageId = relation.getDataPages();   // Garde en mémoir les pages de donnée
        this.relation = relation;
        this.bm = bm;
        this.conditions = conditions;
    }

    // Lis les record 1 par 1 et vérifie qu'ils respectent la / les conditions
    public MyRecord GetNextRecord() {
        MyRecord record = new MyRecord();   // Initialise un nouveau record

        do {
            try {
                // TODOO la condition n'est pas bonne
                // Si l'offset sort de la page on est à la fin de la page
                if (offset >= DBConfig.pagesize) {
                    // Libère l'ancienne page
                    bm.freePage(dataPageId.get(index-1), false);
                    offset = 0; // RAZ l'offset
                    index++;    // Change de data Page
                }

                // Si l'offset vaut 0 on est au début d'une page
                if(offset == 0)
                    buffer = bm.getPage(dataPageId.get(index)); // Alloue la page courante
                
                offset += relation.readRecordFromBuffer(record, buffer, offset);  // Remplie le nouveau record avec ce qu'on lit dans la data Page

            } catch (Exception e) {
            }
        } while(satifyConditions(record, relation));

        return record;  // Retourne le record qui respectent les conditions
    }

    private boolean satifyConditions(MyRecord record, Relation relation) {
        boolean flag = true;    // Flag qui va servir à détecter si une condition n'est pas remplie
        // Parcour la liste de conditions
        for (Condition cond : conditions) {
            try {
                // Vérifie si une condition est rempli
                if (! cond.evaluate(relation, record))
                    flag = false;   // Si la condition est invalide

            } catch (Exception e) {
                return false;
            }
        }
        return flag;
    }

    public void Close() {
        dataPageId = null;
        bm = null;
    }

    public void Reset() {
        index = 0;
        offset = 0;
    }
}