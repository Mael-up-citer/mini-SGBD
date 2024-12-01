import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Classe représentant un gestionnaire de buffers pour la gestion des pages dans une base de données.
 * Le BufferManager gère un pool de buffers, fournit des méthodes pour récupérer et libérer des pages, 
 * ainsi que pour gérer les politiques de remplacement des buffers. Il utilise un arbre AVL pour stocker les 
 * pages en mémoire et une liste de junkFile pour gérer les pages à libérer lorsque le pool est plein.
 */
public class BufferManager{
    private DBConfig dbc;  // Configuration de la base de données
    private DiskManager dskM;  // Gestionnaire des disques
    private AVL cadre = new AVL();  // Pool de buffers (structure de l'arbre AVL)
    private Liste junkFile; // Liste des éléments à libérer (pages qui ne sont plus utilisées)
    private Liste last; // Pointeur vers le premier élément de la junkFile
    private ArrayList<ByteBuffer> emptyBuffer = new ArrayList<ByteBuffer>();  // Liste des buffers vides disponibles
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
    public ByteBuffer getPage(PageId id) throws Exception{
        AVLNode node = cadre.search(id); // Recherche si la pageId est présente dans le buffer pool
        //System.out.println("get " + id);

        // Si la page n'est pas dans le buffer pool
        if (node == null) {
        	// Si le cadre est plein, on doit libérer de l'espace selon la politique de remplacement
            if (emptyBuffer.isEmpty()) {
                makeSpace();    // Libère de l'espace
            }
        	// Recupere le dernier buffer vide
            ByteBuffer tmp = emptyBuffer.remove(emptyBuffer.size() - 1);
            dskM.ReadPage(id, tmp);
            cadre.insert(new AVLNode(id, tmp)); // Insère le nouveau noeud dans l'arbre
            nbAllocFrame++;
            //System.out.println("Création du noeud");
            return tmp;
        }
        // Si le noeud est dans l'arbre
        else {
            ByteBuffer buffer = node.buffer; // Récupère le buffer associé au noeud
        	// Si la page était dans junkFile, on l'enlève
            if(node.pointeurListe != null) {
                suppJunk(node.pointeurListe); // Retire de junkFile
            }

            node.pin_count++; // Augmente le compteur d'utilisation
            return buffer;  // Retourne le buffer
        }
    }

    /**
     * Libère une page en l'ajoutant à la junkFile.
     * 
     * @param id L'identifiant de la page à libérer.
     * @param valdirty Indique si la page a été modifiée (dirty).
     */
    public void freePage(PageId id, boolean valdirty) throws RuntimeException{
        AVLNode noeud = cadre.search(id); // Recherche le noeud à libérer

        // Si le noeud n'est pas trouvé
        if (noeud == null) {
            // Lève une exception
            throw new RuntimeException("Aucun noeud ne correspond à "+id);
        }
        noeud.dirtyFlag = (valdirty || noeud.dirtyFlag); // Déclare si la page a été modifiée et si elle a déjà été modifié on laisse à true
        noeud.pin_count--;   // Décrémenter le compteur d'utilisation

        // Si plus personne ne l'utilise, on l'ajoute à la junkFile
        if(noeud.pin_count == 0) {
            ajoutJunk(noeud);   // L'ajoute à la junkFile
        }
    }

    /**
     * Ajoute un PageId dans la liste de ceux qui peuvent être supprimés.
     * 
     * @param node Le noeud à ajouter à la junkFile.
     */
    private void ajoutJunk(AVLNode node){
    	//Si la liste est vide, on crée le premier maillon
    	if(junkFile == null) {
    		junkFile = new Liste(node.id);
    		//Initialise le pointeur du noeud vers son maillon de chaîne
    		node.pointeurListe = junkFile;
    		//Initialise le pointeur du début de la JunkFile
    		last = junkFile;
    	}
        else {
    		node.pointeurListe = junkFile.add(new Liste(node.id));   // Ajoute l'élément à enlever dans la junkFile
    		last = node.pointeurListe;
        }
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
     * Enlève un PageId de la liste de ceux qui peuvent être supprimés.
     * 
     * @param l La liste des éléments à enlever.
     * @throws Exception Si une erreur survient lors de la suppression de l'élément.
     */
    private void suppJunk(Liste l) throws Exception{
    	cadre.search(l.id).pointeurListe = null;
        junkFile = l.remove();   // Retire l'élément de la JunkFile
        //Si l'élement est le premier de la liste
    	if(l == last) {
    		//Met le pointeur au maillon suivant
    		last = junkFile;
    	}
    }

    /**
     * Selon la politique de remplacement, choisit l'élément à supprimer et écrit son buffer si nécessaire.
     */
    private void makeSpace() throws Exception{
        ByteBuffer buffer = null; // Pointeur vers le buffer
        PageId id = null; // Identifiant de la page à éjecter
        AVLNode noeud = null;

        switch (DBConfig.bm_policy){
            case "MRU":
                id = last.id;
                //System.out.println("Supression " + id);
                noeud = cadre.delete(id);  // Enlève la frame associée dans le bufferPool
                last = last.remove();  // Supprime le dernier élément
                break;
            case "LRU":
                id = junkFile.id;
                noeud = cadre.delete(id); // Enlève la frame associée dans le bufferPool
                junkFile = junkFile.remove();  // Supprime le premier élément
                break;
            default:
                throw new Exception("La politique de remplacement '"+DBConfig.bm_policy+"' n'a pas d'implémentation");
        }

        buffer = noeud.buffer;
        // Si la page a été modifié
        if(noeud.dirtyFlag) {
            dskM.WritePage(id, buffer); // L'écrit en mémoire
        }

        buffer.clear(); // Réinitialise le buffer
        emptyBuffer.add(buffer); // Récupère le buffer vide ici
        nbAllocFrame--;
    }

    /**
     * Alloue un certain nombre de buffers.
     */
    private void initBufferPool(){
        // Alloue n buffers dans la liste
        for(int i = 0; i < DBConfig.bm_buffercount; i++)
            emptyBuffer.add(ByteBuffer.allocate(DBConfig.pagesize));    // Ajoute dans la liste un buffer de la taille d'une page
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
    public Liste getJunkFile() {
    	return junkFile;
    }
    
    /**
     * Répère le pointeur vers le dernier élément de la JunkFile
     * 
     * @return le pointeur vers le dernier élément de la JunkFile
     */
    public Liste getLast() {
    	return last;
    }
}