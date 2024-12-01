import java.util.Objects;

/**
 * La classe PageId représente l'identification d'une page dans un fichier de base de données.
 * Chaque identifiant de page se compose de deux attributs : le numéro du fichier (FileIdx)
 * et l'index de la page dans ce fichier (PageIdx).
 *
 * Cette classe est utilisée pour identifier de manière unique une page dans un fichier,
 * permettant une gestion efficace des pages dans des systèmes de gestion de base de données.
 *
 */
public class PageId {
    // Numéro du fichier où la page est stockée
    public int FileIdx;
    
    // Index de la page dans le fichier spécifié par FileIdx
    public int PageIdx;

    /**
     * Constructeur par défaut de la classe PageId.
     * Initialise les deux attributs FileIdx et PageIdx à leur valeur par défaut (0).
     */
    PageId() {
        // Initialisation par défaut des attributs
    }

    /**
     * Constructeur de la classe PageId avec des valeurs spécifiques pour FileIdx et PageIdx.
     *
     * Permet de créer un objet PageId en spécifiant le numéro de fichier et l'index de la page
     * dans ce fichier.
     *
     * @param fidx Le numéro du fichier où la page est stockée.
     * @param pidx L'index de la page dans ce fichier.
     */
    PageId(int fidx, int pidx) {
        FileIdx = fidx;
        PageIdx = pidx;
    }

    /**
     * Redéfinition de la méthode equals pour comparer deux objets PageId.
     *
     * Cette méthode permet de comparer deux instances de PageId en fonction de leurs
     * attributs FileIdx et PageIdx. Elle retourne true si les deux objets ont les mêmes
     * valeurs pour ces attributs.
     *
     * @param o L'objet à comparer avec l'instance courante.
     * @return true si l'objet donné est égal à l'instance courante, false sinon.
     */
    @Override
    public boolean equals(Object o) {
        // Vérifie si c'est la même instance (optimisation)
        if (this == o)
            return true;
        
        // Vérifie si l'objet est une instance de la même classe
        if (o == null || getClass() != o.getClass())
            return false;
        
        // Compare les valeurs des champs internes
        PageId pid = (PageId) o;
        return this.FileIdx == pid.FileIdx && this.PageIdx == pid.PageIdx;
    }

    /**
     * Redéfinition de la méthode hashCode pour générer un code de hachage basé sur les attributs.
     *
     * La méthode hashCode génère un code de hachage basé sur les valeurs des attributs
     * FileIdx et PageIdx. Cela est important pour garantir une bonne performance dans les
     * collections basées sur le hachage (par exemple, HashMap, HashSet).
     *
     * @return Un code de hachage pour l'objet PageId.
     */
    @Override
    public int hashCode() {
        // Utilisation de Objects.hash pour générer un code de hachage basé sur les attributs
        return Objects.hash(FileIdx, PageIdx);
    }

    /**
     * Redéfinition de la méthode toString pour fournir une représentation lisible de l'objet.
     *
     * Cette méthode retourne une représentation sous forme de chaîne de caractères de l'objet,
     * en affichant les valeurs des attributs FileIdx et PageIdx.
     *
     * @return Une chaîne de caractères représentant l'objet PageId.
     */
    @Override
    public String toString() {
        // Retourne une chaîne lisible des attributs de l'objet
        return FileIdx + "\t" + PageIdx;
    }
}