import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Classe représentant un nœud dans un arbre AVL.
 * Chaque nœud contient un identifiant de page (PageId), un pointeur vers un buffer, 
 * un indicateur de modification (dirtyFlag) et un compteur de référence (pin_count).
 */
class AVLNode{
    PageId id;  // Identifiant de la page associée au nœud
    AVLNode left;   // Pointeur vers le fils gauche
    AVLNode right;  // Pointeur vers le fils droit
    int height; // Hauteur du nœud dans l'arbre
    ByteBuffer buffer; // Pointeur vers un ByteBuffer pour la page
    boolean dirtyFlag = false; // Indicateur pour savoir si la page a été modifiée
    int pin_count = 1; // Nombre d'utilisations en cours de la page
    Liste pointeurListe;    // Pointeur vers l'adresse dans la JunkFile

    /**
     * Constructeur pour un nœud AVL.
     * 
     * @param id L'identifiant de la page associée au nœud
     * @param buffer Le buffer associé à cette page
     */
    AVLNode(PageId id, ByteBuffer buffer){
        this.id = id; // Assigner l'identifiant de la page
        this.buffer = buffer; // Assigner le ByteBuffer
        height = 0; // Initialiser la hauteur du nœud à 0
    }
}

/**
 * Classe représentant un arbre AVL pour gérer un ensemble de nœuds.
 * Cet arbre assure que les nœuds sont équilibrés pour permettre des recherches rapides.
 */
public class AVL{
    private AVLNode root = null; // Racine de l'arbre AVL, initialement vide

    /**
     * Recherche d'un nœud dans l'arbre AVL en fonction de l'identifiant de la page.
     * 
     * @param id L'identifiant de la page à rechercher
     * @return Le nœud trouvé ou null si non trouvé
     */
    public AVLNode search(PageId id){
        return searchNode(root, id);
    }

    /**
     * Recherche récursive d'un nœud en fonction de l'identifiant de la page.
     * 
     * @param node Le nœud actuel de l'arbre à partir duquel commencer la recherche
     * @param id L'identifiant de la page à rechercher
     * @return Le nœud trouvé ou null si non trouvé
     */
    private AVLNode searchNode(AVLNode node, PageId id){
        // Si le nœud est nul, la page n'a pas été trouvée
        if(node == null)
            return null;
        
        // Comparaison sur FileIdx, puis sur PageIdx si nécessaire
        if(id.FileIdx < node.id.FileIdx)
            return searchNode(node.left, id);   // Rechercher à gauche
        else if(id.FileIdx > node.id.FileIdx)
            return searchNode(node.right, id);  // Rechercher à droite
        else{
            // Si FileIdx est égal, on compare le PageIdx
            if (id.PageIdx == node.id.PageIdx)
                return node; // La page est trouvée
            else if (id.PageIdx < node.id.PageIdx)
                return searchNode(node.left, id); // Rechercher à gauche
            else
                return searchNode(node.right, id); // Rechercher à droite
        }
    }

    /**
     * Insère un nouveau nœud dans l'arbre AVL.
     * 
     * @param node Le nœud à insérer
     */
    public void insert(AVLNode node){
        root = insertNode(root, node);
    }

    /**
     * Insertion récursive d'un nœud dans l'arbre AVL, en maintenant l'équilibre.
     * 
     * @param root Le nœud actuel de l'arbre dans lequel insérer le nouveau nœud
     * @param node Le nœud à insérer
     * @return Le nœud racine de l'arbre après insertion
     */
    private AVLNode insertNode(AVLNode root, AVLNode node){
        // Si l'arbre est vide, insérer le nœud
        if(root == null)
            return node;

        // Recherche du bon emplacement pour le nœud
        if(node.id.FileIdx < root.id.FileIdx)
            root.left = insertNode(root.left, node); // Insérer à gauche
        else if(node.id.FileIdx > root.id.FileIdx)
            root.right = insertNode(root.right, node); // Insérer à droite
        else{
            // Si FileIdx est égal, on compare le PageIdx
            if(node.id.PageIdx < root.id.PageIdx)
                root.left = insertNode(root.left, node); // Insérer à gauche
            else
                root.right = insertNode(root.right, node); // Insérer à droite
        }

        // Mise à jour de la hauteur du nœud actuel
        root.height = Math.max(getHeight(root.left), getHeight(root.right)) + 1;

        // Rééquilibrer l'arbre après insertion si nécessaire
        return balanceNode(root);
    }

    /**
     * Supprime un nœud de l'arbre AVL en fonction de l'identifiant de la page.
     * Retourne le buffer associé au nœud supprimé.
     * 
     * @param id L'identifiant de la page à supprimer
     * @return Le ByteBuffer associé à la page supprimée
     */
    ByteBuffer delete(PageId id){
        ByteBuffer[] buffer = new ByteBuffer[1]; // Encapsule le buffer du noeud a supprimer
        buffer[0] = null;

        root = deleteNode(root, id, buffer);    // Met a jour l'arbre

        return buffer[0];   // Retourne le buffer pour le reutiliser
    }

    /**
     * Suppression récursive d'un nœud dans l'arbre AVL et récupération de son buffer.
     * 
     * @param node Le nœud actuel de l'arbre
     * @param id L'identifiant de la page à supprimer
     * @param buffer Tableau encapsulant le buffer à retourner
     * @return Le nœud de l'arbre après suppression
     */
    private AVLNode deleteNode(AVLNode node, PageId id, ByteBuffer[] buffer){
        // Phase de recherche
        if(node == null)
            return null; // Si le noeud est nul, retourner nul

        // Comparer le FileIdx pour la recherche
        if (id.FileIdx < node.id.FileIdx)
            node.left = deleteNode(node.left, id, buffer);
        else if (id.FileIdx > node.id.FileIdx)
            node.right = deleteNode(node.right, id, buffer);
        else {
            // Si FileIdx est egal, comparer PageIdx
            if (id.PageIdx < node.id.PageIdx)
                node.left = deleteNode(node.left, id, buffer);
            else if (id.PageIdx > node.id.PageIdx)
                node.right = deleteNode(node.right, id, buffer);
            else {
                //CHECK ME
                // Le noeud d'origine a supprimer est trouve
                if(buffer[0] == null)
                    buffer[0] = node.buffer; // Capturer le buffer initial a supprimer

                // Si le noeud a un seul enfant ou pas d'enfant
                if (node.left == null)
                    return node.right;
                else if (node.right == null)
                    return node.left;

                // Noeud avec deux enfants, obtenir le minimum dans le sous-arbre droit
                AVLNode temp = minValueNode(node.right);
                node.id = temp.id; // Copier les valeurs de l'enfant
                node.buffer = temp.buffer; // Copier le buffer du successeur
                node.right = deleteNode(node.right, temp.id, buffer);
            }
        }

        // Mettre a jour la hauteur de l'ancetre
        node.height = Math.max(getHeight(node.left), getHeight(node.right)) + 1;

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
     * @param node Le nœud actuel de l'arbre
     * @param emptyBuffer Liste des buffers vides pour les réutiliser
     * @throws Exception Si une erreur survient lors de l'écriture des pages
     */
    private void dump(DiskManager dskm, AVLNode node, ArrayList<ByteBuffer> emptyBuffer) throws Exception {
        if(node != null){
            // Si le buffer est marqué comme modifié, l'écrire sur le disque
            if (node.dirtyFlag)
                dskm.WritePage(node.id, node.buffer); // Écrire le buffer modifié

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
     * Vérifie si un nœud est déséquilibré et effectue la rotation nécessaire pour rééquilibrer l'arbre.
     * 
     * @param node Le nœud à rééquilibrer
     * @return Le nœud rééquilibré
     */
    private AVLNode balanceNode(AVLNode node){
        if(node == null)
            return node; // Aucun besoin de rééquilibrer un nœud nul

        int balanceFactor = getBalance(node); // Calcul du facteur d'équilibre

        // Si l'arbre est déséquilibré à gauche
        if(balanceFactor > 1){
            // Cas gauche-gauche
            if(getBalance(node.left) >= 0)
                return rightRotate(node); // Rotation droite
            // Cas gauche-droit
            else{
                node.left = leftRotate(node.left); // Rotation gauche sur le sous-arbre gauche
                return rightRotate(node); // Rotation droite
            }
        }
        // Si l'arbre est déséquilibré à droite
        else if(balanceFactor < -1){
            // Cas droit-droit
            if(getBalance(node.right) <= 0)
                return leftRotate(node); // Rotation gauche
            // Cas droit-gauche
            else{
                node.right = rightRotate(node.right); // Rotation droite sur le sous-arbre droit
                return leftRotate(node); // Rotation gauche
            }
        }
        return node; // Si l'arbre est équilibré, retourner tel quel
    }

    /**
     * Retourne la hauteur d'un nœud.
     * 
     * @param node Le nœud dont on veut la hauteur
     * @return La hauteur du nœud
     */
    public int getHeight(AVLNode node){
        return node == null ? 0 : node.height; // Retourner 0 si le nœud est nul
    }

    /**
     * Retourne le facteur d'équilibre d'un nœud.
     * 
     * @param node Le nœud dont on veut le facteur d'équilibre
     * @return Le facteur d'équilibre
     */
    private int getBalance(AVLNode node){
        return node == null ? 0 : getHeight(node.left) - getHeight(node.right); // Différence de hauteur
    }

    /**
     * Effectue une rotation droite sur un nœud.
     * 
     * @param y Le nœud à faire tourner
     * @return Le nouveau nœud racine après la rotation
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

        return x; // Retourner le nouveau nœud racine
    }

    /**
     * Effectue une rotation gauche sur un nœud.
     * 
     * @param x Le nœud à faire tourner
     * @return Le nouveau nœud racine après la rotation
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

        return y; // Retourner le nouveau nœud racine
    }

    /**
     * Trouve le nœud avec la valeur minimale dans l'arbre.
     * 
     * @return Le nœud avec la valeur minimale
     */
    public AVLNode minValueNode(){
        return minValueNode(root); // Chercher le minimum à partir de la racine
    }
    
    /**
     * Trouve le nœud avec la valeur minimale dans un sous-arbre.
     * 
     * @param node Le nœud à partir duquel commencer la recherche
     * @return Le nœud avec la valeur minimale
     */
    private AVLNode minValueNode(AVLNode node){
        AVLNode current = node;
        // Aller le plus à gauche possible pour trouver la valeur minimale
        while(current.left != null)
            current = current.left;

        return current; // Retourner le nœud minimum
    }
}