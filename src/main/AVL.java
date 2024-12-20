import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Classe représentant un noeud dans un arbre AVL.
 * Chaque noeud contient un identifiant de page (PageId), un pointeur vers un buffer, 
 * un indicateur de modification (dirtyFlag) et un compteur de référence (pin_count).
 */
class AVLNode {
    PageId id;  // Identifiant de la page associée au noeud
    AVLNode left;   // Pointeur vers le fils gauche
    AVLNode right;  // Pointeur vers le fils droit
    int height; // Hauteur du noeud dans l'arbre
    ByteBuffer buffer; // Pointeur vers un ByteBuffer pour la page
    boolean dirtyFlag = false; // Indicateur pour savoir si la page a été modifiée
    int pin_count; // Nombre d'utilisations en cours de la page
    MyLinkedList.Cellule<PageId> pointeurListe; // Pointeur vers l'emplacement dans la JunkFile (MyLinkedList)

    /**
     * Constructeur pour un noeud AVL.
     * 
     * @param id L'identifiant de la page associée au noeud
     * @param buffer Le buffer associé à cette page
     */
    AVLNode(PageId id, ByteBuffer buffer){
        this.id = id; // Assigner l'identifiant de la page
        this.buffer = buffer; // Assigner le ByteBuffer
        height = 0; // Initialiser la hauteur du noeud à 0
        pin_count = 1; // Nombre d'utilisations en cours de la page
    }

    /**
     * Constructeur pour un noeud AVL.
     * 
     * @param id L'identifiant de la page associée au noeud
     * @param buffer Le buffer associé à cette page
     */
    AVLNode(PageId id, ByteBuffer buffer, boolean dirtyFlag){
        this.id = id; // Assigner l'identifiant de la page
        this.buffer = buffer; // Assigner le ByteBuffer
        this.dirtyFlag = dirtyFlag; // Assigner le dirtyFlag
        height = 0; // Initialiser la hauteur du noeud à 0
    }
}

/**
 * Classe représentant un arbre AVL pour gérer un ensemble de noeuds.
 * Cet arbre assure que les noeuds sont équilibrés pour permettre des recherches rapides.
 */
public class AVL{
    private AVLNode root = null; // Racine de l'arbre AVL, initialement vide

    /**
     * Recherche d'un noeud dans l'arbre AVL en fonction de l'identifiant de la page.
     * 
     * @param id L'identifiant de la page à rechercher
     * @return Le noeud trouvé ou null si non trouvé
     */
    public AVLNode search(PageId id){
        return searchNode(root, id);
    }

    /**
     * Recherche récursive d'un noeud en fonction de l'identifiant de la page.
     * 
     * @param node Le noeud actuel de l'arbre à partir duquel commencer la recherche
     * @param id L'identifiant de la page à rechercher
     * @return Le noeud trouvé ou null si non trouvé
     */
    private AVLNode searchNode(AVLNode node, PageId id){
        // Si le noeud est nul, la page n'a pas été trouvée
        if(node == null) {
            return null;
        }
        
        // Comparaison sur FileIdx, puis sur PageIdx si nécessaire
        if(id.FileIdx < node.id.FileIdx) {
            return searchNode(node.left, id);   // Rechercher à gauche
        }
        else if(id.FileIdx > node.id.FileIdx) {
            return searchNode(node.right, id);  // Rechercher à droite
        }
        else{
            // Si FileIdx est égal, on compare le PageIdx
            if (id.PageIdx == node.id.PageIdx) {
                return node; // La page est trouvée
            }
            else if (id.PageIdx < node.id.PageIdx) {
                return searchNode(node.left, id); // Rechercher à gauche
            }
            else {
                return searchNode(node.right, id); // Rechercher à droite
            }
        }
    }

    /**
     * Insère un nouveau noeud dans l'arbre AVL.
     * 
     * @param node Le noeud à insérer
     */
    public void insert(AVLNode node){
        root = insertNode(root, node);
    }

    /**
     * Insertion récursive d'un noeud dans l'arbre AVL, en maintenant l'équilibre.
     * 
     * @param root Le noeud actuel de l'arbre dans lequel insérer le nouveau noeud
     * @param node Le noeud à insérer
     * @return Le noeud racine de l'arbre après insertion
     */
    private AVLNode insertNode(AVLNode root, AVLNode node){
        // Si l'arbre est vide, insérer le noeud
        if(root == null) {
            return node;
        }

        // Recherche du bon emplacement pour le noeud
        if(node.id.FileIdx < root.id.FileIdx) {
            root.left = insertNode(root.left, node); // Insérer à gauche
        }
        else if(node.id.FileIdx > root.id.FileIdx) {
            root.right = insertNode(root.right, node); // Insérer à droite
        }
        else{
            // Si FileIdx est égal, on compare le PageIdx
            if(node.id.PageIdx < root.id.PageIdx) {
                root.left = insertNode(root.left, node); // Insérer à gauche
            }
            else {
                root.right = insertNode(root.right, node); // Insérer à droite
            }
        }

        // Mise à jour de la hauteur du noeud actuel
        root.height = Math.max(getHeight(root.left), getHeight(root.right)) + 1;

        // Rééquilibrer l'arbre après insertion si nécessaire
        return balanceNode(root);
    }

    /**
     * Supprime un noeud de l'arbre AVL en fonction de l'identifiant de la page.
     * Retourne le noeud supprimé, qui contient également le buffer associé à la page.
     * 
     * @param id L'identifiant de la page à supprimer
     * @return Le noeud supprimé, contenant le ByteBuffer associé
     */
    AVLNode delete(PageId id) {
        // Le noeud supprimé sera retourné par la méthode deleteNode
        AVLNode[] deletedNode = new AVLNode[1];  // Tableau pour encapsuler le noeud supprimé
        deletedNode[0] = null;

        // Met à jour l'arbre AVL et récupère le noeud supprimé
        root = deleteNode(root, id, deletedNode);

        return deletedNode[0];  // Retourne le noeud supprimé
    }

    /**
     * Suppression récursive d'un noeud dans l'arbre AVL et récupération du noeud supprimé.
     * 
     * @param node Le noeud actuel de l'arbre
     * @param id L'identifiant de la page à supprimer
     * @param deletedNode Tableau encapsulant le noeud supprimé
     * @return Le noeud de l'arbre après suppression
     */
    private AVLNode deleteNode(AVLNode node, PageId id, AVLNode[] deletedNode) {
        // Phase de recherche
        if (node == null) {
            return null; // Si le noeud est nul, retourner nul
        }

        // Comparer le FileIdx pour la recherche
        if (id.FileIdx < node.id.FileIdx) {
            node.left = deleteNode(node.left, id, deletedNode);
        }
        else if (id.FileIdx > node.id.FileIdx) {
            node.right = deleteNode(node.right, id, deletedNode);
        }
        else {
            // Si FileIdx est égal, comparer PageIdx
            if (id.PageIdx < node.id.PageIdx) {
                node.left = deleteNode(node.left, id, deletedNode);
            }
            else if (id.PageIdx > node.id.PageIdx) {
                node.right = deleteNode(node.right, id, deletedNode);
            }
            else {
                // Le noeud à supprimer a été trouvé !
                if (deletedNode[0] == null) {
                    deletedNode[0] = new AVLNode(node.id, node.buffer, node.dirtyFlag); // Capture le noeud supprimé (et son buffer associé)
                }
                // Si le noeud a un seul enfant ou aucun enfant
                if (node.left == null) {
                    return node.right;
                }
                else if (node.right == null) {
                    return node.left;
                }

                // Noeud avec deux enfants, obtenir le minimum dans le sous-arbre droit
                AVLNode temp = minValueNode(node.right);
                node.id = temp.id; // Copier les valeurs du successeur
                node.buffer = temp.buffer; // Copier le buffer du successeur
                node.right = deleteNode(node.right, temp.id, deletedNode); // Suppression récursive du successeur
            }
        }

        // Met à jour la hauteur du noeud actuel
        node.height = Math.max(getHeight(node.left), getHeight(node.right)) + 1;

        // Retourne le noeud équilibré
        return balanceNode(node);
    }

    /**
     * Sauvegarde tous les buffers modifiés (dirtyFlag = true) dans le gestionnaire de disque,
     * puis vide les buffers en les ajoutant à la liste des buffers vides.
     * 
     * @param dskm Le gestionnaire de disque pour écrire les pages modifiées
     * @param emptyBuffer Liste des buffers vides pour les réutiliser
     * @throws Exception Si une erreur survient lors de l'écriture des pages
     */
    public void dump(DiskManager dskm, ArrayList<ByteBuffer> emptyBuffer) throws Exception{
        dump(dskm, root, emptyBuffer);   // Vider l'arbre
        root = null;    // Réinitialiser la racine de l'arbre
    }

    /**
     * Parcours récursif de l'arbre AVL pour vider les buffers modifiés.
     * 
     * @param dskm Le gestionnaire de disque pour écrire les pages modifiées
     * @param node Le noeud actuel de l'arbre
     * @param emptyBuffer Liste des buffers vides pour les réutiliser
     * @throws Exception Si une erreur survient lors de l'écriture des pages
     */
    private void dump(DiskManager dskm, AVLNode node, ArrayList<ByteBuffer> emptyBuffer) throws Exception {
        if(node != null){
            // Si le buffer est marqué comme modifié, l'écrire sur le disque
            if (node.dirtyFlag) {
                dskm.WritePage(node.id, node.buffer); // Écrire le buffer modifié
            }

            // Vider le buffer et l'ajouter à la liste des buffers vides
            node.buffer.clear();
            emptyBuffer.add(node.buffer);

            // Parcourir récursivement les sous-arbres gauche et droit
            dump(dskm, node.left, emptyBuffer);
            dump(dskm, node.right, emptyBuffer);
        }
    }

    /* Méthodes utilitaires pour maintenir l'équilibre de l'arbre */

    /**
     * Vérifie si un noeud est déséquilibré et effectue la rotation nécessaire pour rééquilibrer l'arbre.
     * 
     * @param node Le noeud à rééquilibrer
     * @return Le noeud rééquilibré
     */
    private AVLNode balanceNode(AVLNode node){
        if(node == null) {
            return node; // Aucun besoin de rééquilibrer un noeud nul
        }

        int balanceFactor = getBalance(node); // Calcul du facteur d'équilibre

        // Si l'arbre est déséquilibré à gauche
        if(balanceFactor > 1){
            // Cas gauche-gauche
            if(getBalance(node.left) >= 0) {
                return rightRotate(node); // Rotation droite
            }
            // Cas gauche-droit
            else{
                node.left = leftRotate(node.left); // Rotation gauche sur le sous-arbre gauche
                return rightRotate(node); // Rotation droite
            }
        }
        // Si l'arbre est déséquilibré à droite
        else if(balanceFactor < -1){
            // Cas droit-droit
            if(getBalance(node.right) <= 0) {
                return leftRotate(node); // Rotation gauche
            }
            // Cas droit-gauche
            else{
                node.right = rightRotate(node.right); // Rotation droite sur le sous-arbre droit
                return leftRotate(node); // Rotation gauche
            }
        }
        return node; // Si l'arbre est équilibré, retourner tel quel
    }

    /**
     * Retourne la hauteur d'un noeud.
     * 
     * @param node Le noeud dont on veut la hauteur
     * @return La hauteur du noeud
     */
    public int getHeight(AVLNode node){
        return node == null ? 0 : node.height; // Retourner 0 si le noeud est nul
    }

    /**
     * Retourne le facteur d'équilibre d'un noeud.
     * 
     * @param node Le noeud dont on veut le facteur d'équilibre
     * @return Le facteur d'équilibre
     */
    private int getBalance(AVLNode node){
        return node == null ? 0 : getHeight(node.left) - getHeight(node.right); // Différence de hauteur
    }

    /**
     * Effectue une rotation droite sur un noeud.
     * 
     * @param y Le noeud à faire tourner
     * @return Le nouveau noeud racine après la rotation
     */
    private AVLNode rightRotate(AVLNode y){
        AVLNode x = y.left;
        AVLNode T2 = x.right;

        // Effectuer la rotation droite
        x.right = y;
        y.left = T2;

        // Mettre à jour les hauteurs
        y.height = Math.max(getHeight(y.left), getHeight(y.right)) + 1;
        x.height = Math.max(getHeight(x.left), getHeight(x.right)) + 1;

        return x; // Retourner le nouveau noeud racine
    }

    /**
     * Effectue une rotation gauche sur un noeud.
     * 
     * @param x Le noeud à faire tourner
     * @return Le nouveau noeud racine après la rotation
     */
    private AVLNode leftRotate(AVLNode x){
        AVLNode y = x.right;
        AVLNode T2 = y.left;

        // Effectuer la rotation gauche
        y.left = x;
        x.right = T2;

        // Mettre à jour les hauteurs
        x.height = Math.max(getHeight(x.left), getHeight(x.right)) + 1;
        y.height = Math.max(getHeight(y.left), getHeight(y.right)) + 1;

        return y; // Retourner le nouveau noeud racine
    }

    /**
     * Trouve le noeud avec la valeur minimale dans l'arbre.
     * 
     * @return Le noeud avec la valeur minimale
     */
    public AVLNode minValueNode(){
        return minValueNode(root); // Chercher le minimum à partir de la racine
    }
    
    /**
     * Trouve le noeud avec la valeur minimale dans un sous-arbre.
     * 
     * @param node Le noeud à partir duquel commencer la recherche
     * @return Le noeud avec la valeur minimale
     */
    private AVLNode minValueNode(AVLNode node){
        AVLNode current = node;
        // Aller le plus à gauche possible pour trouver la valeur minimale
        while(current.left != null)
            current = current.left;

        return current; // Retourner le noeud minimum
    }
}