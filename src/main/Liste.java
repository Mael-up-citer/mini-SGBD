public class Liste{
    Liste precedent;
    Liste suivant;
    PageId id;

    Liste(){
        suivant = null;
        precedent = null;
    }

    Liste(PageId id){
        this.id = id;
    }

    // Ajoute en tête le parametre devant l'appellent et retourne un pointeur vers cette élément
    public Liste add(Liste l){
        // L'élément l devient la nouvelle tête de la liste
        l.suivant = this; // L'élément l pointe vers l'élément actuel
        this.precedent = l; // L'élément actuel pointe vers l'élément l

        return l; // Retourne le nouvel élément (nouvelle tête de la liste)
    }

    public Liste remove(){
        // Si l'élément est le premier de la liste
        if(this.precedent == null){
            if(this.suivant != null){
                // L'élément suivant devient le nouveau début
                this.suivant.precedent = null;  // Le prédécesseur du suivant devient null
                return this.suivant;  // Retourner le nouvel élément de tête
            }
            else
                // La liste est vide après la suppression
                return null;  // Il n'y a plus d'élément
        }
        // Si l'élément est le dernier de la liste
        if(this.suivant == null){
            this.precedent.suivant = null;  // Le prédécesseur du dernier élément devient null
            return this.precedent;  // Retourner le nouvel élément de fin
        }
    
        // Si l'élément est au milieu de la liste, on lève une exception
        throw new IllegalArgumentException("Impossible de supprimer un élément qui n'est ni en tête ni en fin de liste.");
    }    
}