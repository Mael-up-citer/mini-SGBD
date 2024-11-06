/**
 * La classe PageId représente l'identification d'une page dans un fichier de base de données.
 * Chaque identifiant de page se compose de deux attributs : le numéro du fichier (FileIdx)
 * et l'index de la page dans ce fichier (PageIdx).
 */
public class PageId{
    // Numéro du fichier où la page est stockée
    public int FileIdx;
    
    // Index de la page dans le fichier spécifié par FileIdx
    public int PageIdx;

    /**
     * Constructeur par défaut de la classe PageId.
     * Initialise les deux attributs FileIdx et PageIdx à leur valeur par défaut (0).
     */
    PageId(){
    }

    /**
     * Constructeur de la classe PageId avec des valeurs spécifiques pour FileIdx et PageIdx.
     * 
     * @param fidx Le numéro du fichier où la page est stockée.
     * @param pidx L'index de la page dans ce fichier.
     */
    PageId(int fidx, int pidx){
        FileIdx = fidx;
        PageIdx = pidx;
    }
}