import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Classe représentant un gestionnaire de buffers pour la gestion des pages dans une base de données.
 * Le BufferManager gère un pool de buffers, fournit des méthodes pour récupérer et libérer des pages, 
 * ainsi que pour gérer les politiques de remplacement des buffers. Il utilise un arbre AVL pour stocker les 
 * pages en mémoire et une liste de junkFile pour gérer les pages à libérer lorsque le pool est plein.
 */
public class BufferManager{
    DBConfig dbc;  // Configuration de la base de données
    DiskManager dskm;  // Gestionnaire des disques
    AVL cadre = new AVL();  // Pool de buffers (structure de l'arbre AVL)
    Liste junkFile; // Liste des éléments à libérer (pages qui ne sont plus utilisées)
    Liste last; // Pointeur vers le dernier élément
    ArrayList<ByteBuffer> emptyBuffer = new ArrayList<ByteBuffer>();  // Liste des buffers vides disponibles

    /**
     * Constructeur pour initialiser le BufferManager avec la configuration de la base de données et le gestionnaire de disques.
     * 
     * @param dbc La configuration de la base de données.
     * @param dskm Le gestionnaire de disques.
     */
    BufferManager(DBConfig dbc, DiskManager dskm){
        this.dbc = dbc;
        this.dskm = dskm;
        initBufferPool(); // Crée tous les buffers nécessaires
    }

    /**
     * Retourne le ByteBuffer d'une page donnée par son PageId.
     * 
     * @param id L'identifiant de la page à récupérer.
     * @return Le ByteBuffer de la page.
     * @throws Exception Si une erreur survient lors de la récupération de la page.
     */
    public ByteBuffer getPage(PageId id) throws Exception{
        AVLNode node = cadre.search(id); // Recherche si la pageId est présente dans le buffer pool
        ByteBuffer buffer = node.buffer; // Récupère le buffer associé au noeud

        // Si la page est en mémoire, on la retourne
        if(buffer != null){
            // Si la page était dans junkFile, on l'enlève
            if(node.pointeurListe != null)
                suppJunk(node.pointeurListe); // Retire de junkFile

            node.pin_count++; // Augmente le compteur d'utilisation
            return buffer;  // Retourne le buffer
        }

        // Si le cadre est plein, on doit libérer de l'espace selon la politique de remplacement
        if(emptyBuffer.isEmpty())
            makeSpace();    // Libère de l'espace

        // Ajoute le buffer au cadre si vide. Si l'insertion échoue, une exception sera levée
        ByteBuffer tmp = emptyBuffer.remove(emptyBuffer.size() - 1);
        cadre.insert(new AVLNode(id, tmp)); // Insère le nouveau noeud dans l'arbre

        return buffer;
    }

    /**
     * Libère une page en l'ajoutant à la junkFile.
     * 
     * @param id L'identifiant de la page à libérer.
     * @param valdirty Indique si la page a été modifiée (dirty).
     */
    public void freePage(PageId id, boolean valdirty){
        AVLNode noeud = cadre.search(id); // Recherche le noeud à libérer

        noeud.dirtyFlag = valdirty; // Déclare si la page a été modifiée
        noeud.pin_count -= 1;   // Décrémenter le compteur d'utilisation

        // Si plus personne ne l'utilise, on l'ajoute à la junkFile
        if(noeud.pin_count == 0)
            ajoutJunk(noeud);   // L'ajoute à la junkFile
    }

    /**
     * Ajoute un PageId dans la liste de ceux qui peuvent être supprimés.
     * 
     * @param node Le noeud à ajouter à la junkFile.
     */
    private void ajoutJunk(AVLNode node){
        node.pointeurListe = junkFile.add(new Liste(node.id));   // Ajoute l'élément à enlever dans la junkFile
    }

    /**
     * Vide les buffers et les écrit en mémoire si "dirty" est vrai.
     * 
     * @throws Exception Si une erreur survient lors de l'écriture des buffers.
     */
    public void flushBuffers() throws Exception{
        cadre.dump(dskm, emptyBuffer);  // Écrit les buffers en mémoire
    }

    /**
     * Enlève un PageId de la liste de ceux qui peuvent être supprimés.
     * 
     * @param l La liste des éléments à enlever.
     * @throws Exception Si une erreur survient lors de la suppression de l'élément.
     */
    private void suppJunk(Liste l) throws Exception{
        l.remove();   // Retire l'élément de la JunkFile
        l = null;     // Réinitialise le pointeur
    }

    /**
     * Selon la politique de remplacement, choisit l'élément à supprimer et écrit son buffer si nécessaire.
     */
    private void makeSpace(){
        ByteBuffer buffer = null; // Pointeur vers le buffer

        switch (DBConfig.bm_policy){
            case "LRU":
                buffer = cadre.delete(last.id);  // Enlève la frame associée dans le bufferPool
                last = last.remove();  // Supprime le dernier élément
                break;

            case "MRU":
                buffer = cadre.delete(junkFile.id); // Enlève la frame associée dans le bufferPool
                junkFile = junkFile.remove();  // Supprime le premier élément
                break;
        }
        // D'autres politiques pourraient être ajoutées ici...
        buffer.clear(); // Réinitialise le buffer
        emptyBuffer.add(buffer); // Récupère le buffer vide ici
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
     * Alloue un certain nombre de buffers.
     */
    private void initBufferPool(){
        // Alloue n buffers dans la liste
        for(int i = 0; i < DBConfig.bm_buffercount; i++)
            emptyBuffer.add(ByteBuffer.allocate(DBConfig.pagesize));    // Ajoute dans la liste un buffer de la taille d'une page
    }
}
