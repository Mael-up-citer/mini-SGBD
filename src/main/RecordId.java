/**
 * Représente un identifiant de record dans une page de la base de données.
 * Il contient un index de slot ainsi qu'un identifiant de page, permettant de localiser
 * un enregistrement précis dans une page spécifique.
 */
public class RecordId {
    // L'index du slot dans la page (identifie un enregistrement dans une page)
    public int slotIdx;
    
    // L'identifiant de la page contenant le record
    public PageId pageIdx;

    /**
     * Constructeur pour initialiser un RecordId avec un index de slot et un identifiant de page.
     * 
     * @param sidx L'index du slot dans la page.
     * @param pidx L'identifiant de la page (PageId) dans laquelle le record se trouve.
     */
    RecordId(int sidx, PageId pidx){
        slotIdx = sidx;
        pageIdx = pidx;
    }
}