import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Classe représentant un gestionnaire de buffers pour la gestion des pages dans une base de données.
 * Le BufferManager gère un pool de buffers, fournit des méthodes pour récupérer et libérer des pages, 
 * ainsi que pour gérer les politiques de remplacement des buffers. Il utilise un arbre AVL pour stocker les 
 * pages en mémoire et une liste de junkFile pour gérer les pages à libérer lorsque le pool est plein.
 */
public class BufferManager {
    private DBConfig dbc;  // Configuration de la base de données
    private DiskManager dskM;  // Gestionnaire des disques
    private AVL cadre = new AVL();  // Pool de buffers (structure de l'arbre AVL)
    private MyLinkedList<PageId> junkFile = new MyLinkedList<>(); // Liste des éléments à libérer (pages qui ne sont plus utilisées)
    private ArrayList<ByteBuffer> emptyBuffer = new ArrayList<>();  // Pile des buffers vides disponibles
    private int nbAllocFrame = 0;   // Nombre de frame occupé dans l'AVL

    /**
     * Constructeur pour initialiser le BufferManager avec la configuration de la base de données et le gestionnaire de disques.
     * 
     * @param dbc La configuration de la base de données.
     * @param dskM Le gestionnaire de disques.
     */
    BufferManager(DBConfig dbc, DiskManager dskM){
        this.dbc = dbc;
        this.dskM = dskM;
        initBufferPool(); // Crée tous les buffers nécessaires
    }

    /**
     * Retourne le ByteBuffer d'une page donnée par son PageId.
     * 
     * @param id L'identifiant de la page à récupérer.
     * @return Le ByteBuffer de la page.
     * @throws Exception Si une erreur survient lors de la récupération de la page.
     */
    public ByteBuffer getPage(PageId id) throws Exception {
        AVLNode node = cadre.search(id); // Recherche si la pageId est présente dans le buffer pool

        // Si la page n'est pas dans le buffer pool
        if (node == null) {
        	// Si le cadre est plein, on doit libérer de l'espace selon la politique de remplacement
            if (emptyBuffer.isEmpty())
                makeSpace();    // Libère de l'espace

        	// Recupere le dernier buffer vide
            ByteBuffer tmp = emptyBuffer.remove(emptyBuffer.size()-1);;  // recupère le dernier buffer vide
            dskM.ReadPage(id, tmp);
            cadre.insert(new AVLNode(id, tmp)); // Insère le nouveau noeud dans l'arbre
            nbAllocFrame++; // L'AVL à une frame de plus

            return tmp;
        }
        // Si le noeud est dans l'arbre
        else {
            ByteBuffer buffer = node.buffer; // Récupère le buffer associé au noeud
            node.pin_count++;   // Incrémente le compteur d'utilisation
        	// Si la page était dans junkFile, on l'enlève
            if(node.pointeurListe != null) {
                nbAllocFrame++;
                suppJunk(node); // Retire de junkFile
            }
            return buffer;  // Retourne le buffer
        }
    }

    /**
     * Libère une page en l'ajoutant à la junkFile.
     * 
     * @param id L'identifiant de la page à libérer.
     * @param valdirty Indique si la page a été modifiée (dirty).
     */
    public boolean freePage(PageId id, boolean valdirty) {
        AVLNode noeud = cadre.search(id); // Recherche le noeud à libérer

        // Si le noeud n'est pas trouvé
        if (noeud == null)
            return false; // retourne faux pour signaler que la liberation ne c'est pas faite

        noeud.dirtyFlag = (valdirty || noeud.dirtyFlag); // Déclare si la page a été modifiée et si elle a déjà été modifié on laisse à true

        // Si plus personne ne l'utilise, on l'ajoute à la junkFile
        if (noeud.pin_count > 0) {
            noeud.pin_count--;
            if(noeud.pin_count == 0) {
                nbAllocFrame--; // Une frame de moins qui ne peut pas etre enlevé
                ajoutJunk(noeud);   // L'ajoute à la junkFile
            }
        }
        // Si tout c'est bien passé return true
        return true;
    }

    /**
     * Ajoute un PageId dans la liste de ceux qui peuvent être supprimés.
     * 
     * @param node Le noeud à ajouter à la junkFile.
     */
    private void ajoutJunk(AVLNode node) {
        node.pointeurListe = junkFile.add(node.id);   // Ajoute le PageId dans la junkFile
    }

    /**
     * Enlève un PageId de la liste de ceux qui peuvent être supprimés.
     * 
     * @param elmt La liste des éléments à enlever.
     * @throws Exception Si une erreur survient lors de la suppression de l'élément.
     */
    private void suppJunk(AVLNode node) throws Exception {
        // Retirer la première cellule de la junkFile
        junkFile.remove(node.pointeurListe.getValue());
        node.pointeurListe = null;
    }

    /**
     * Vide les buffers et les écrit en mémoire si "dirty" est vrai.
     * 
     * @throws Exception Si une erreur survient lors de l'écriture des buffers.
     */
    public void flushBuffers() throws Exception{
        cadre.dump(dskM, emptyBuffer);  // Écrit les buffers en mémoire
    }

    /**
     * Selon la politique de remplacement, choisit l'élément à supprimer et écrit son buffer si nécessaire.
     */
    private void makeSpace() throws Exception {
        ByteBuffer buffer = null; // Pointeur vers le buffer
        PageId id = null; // Identifiant de la page à éjecter
        AVLNode noeud = null;   // noeud de la page dans l'AVL

        switch (DBConfig.bm_policy){
            case "MRU":
                id = junkFile.getTail().getValue();  // Prendre le dernier élément de la junkFile
                noeud = cadre.delete(id);  // Enlève la frame associée dans le bufferPool
                junkFile.removeFromTail();  // Supprime le premier élément
                break;
            case "LRU":
                id = junkFile.remove().getValue();  // Prendre le premier élément de la junkFile
                noeud = cadre.delete(id);  // Enlève la frame associée dans le bufferPool
                break;
            default:
                throw new Exception("La politique de remplacement '"+DBConfig.bm_policy+"' n'a pas d'implémentation");
        }
        // Si on ne trouve aucun noeud libérable
        if (noeud == null)
            throw new IllegalStateException("erreur critique, plus d'espace disponible dans la buffer pool: occupation = "+nbAllocFrame+"/"+DBConfig.bm_buffercount);

        buffer = noeud.buffer;
        // Si la page a été modifié
        if(noeud.dirtyFlag)
            dskM.WritePage(id, buffer); // L'écrit en mémoire

        buffer.clear(); // Réinitialise le buffer
        emptyBuffer.add(buffer); // Récupère le buffer vide ici
    }

    /**
     * Alloue un certain nombre de buffers.
     */
    private void initBufferPool(){
        // Alloue n buffers dans la pile
        for(int i = 0; i < DBConfig.bm_buffercount; i++)
            emptyBuffer.add(ByteBuffer.allocate(DBConfig.pagesize));    // Ajoute dans la pile un buffer de la taille d'une page
    }

    /**
     * Met à jour la politique de remplacement utilisée.
     * 
     * @param policy La politique de remplacement à appliquer (par exemple, "LRU", "MRU").
     */
    public void SetCurrentReplacementPolicy(String policy){
        DBConfig.bm_policy = policy;  // Applique la nouvelle politique
    }

    /**
     * Récupère le nombre de Pages allouées
     * 
     * @return le nombre de Pages allouées
     */
    public int getNbAllocFrame() {
        return nbAllocFrame;
    }
    
    /**
     * Récupère le nombre de buffers vides restants
     * 
     * @return le nombre de buffers vides restants
     */
    public int getEmptyBufferSize() {
    	return emptyBuffer.size();
    }
    
    /**
     * Récupère l'AVL du Buffer Manager
     * 
     * @return l'AVL du Buffer Manager
     */
    public AVL getCadre() {
    	return cadre;
    }
    
    /**
     * Récupère le pointeur vers le premier élément de la JunkFile
     * 
     * @return le pointeur vers le premier élément de la JunkFile
     */
    public MyLinkedList<PageId> getJunkFile() {
    	return junkFile;
    }
}