/**
 * Classe représentant une liste doublement chaînée générique circulaire avec un noeud sentinelle.
 * Cette liste permet d'ajouter, de supprimer et de vérifier les éléments de manière efficace.
 * Elle utilise un noeud sentinelle pour simplifier les opérations de gestion des cas particuliers
 * (ajout, suppression à la tête et à la queue). De plus, elle optimise les opérations d'ajout et de suppression
 * à la fin de la liste en utilisant un pointeur vers le dernier élément (`tail`).
 * 
 * @param <T> Le type des éléments de la liste. La liste est générique et peut contenir n'importe quel type d'élément.
 */
public class MyLinkedList<T> {

    /**
     * Représente un élément de la liste chaînée.
     * Chaque élément a une valeur de type `T`, ainsi que des pointeurs vers son précédent et son suivant.
     * 
     * @param <T> Le type de la valeur stockée dans ce noeud.
     */
    public static class Cellule<T> {
        private T value;  // La valeur de type T associée à ce noeud
        private Cellule<T> suivant;  // Référence au noeud suivant
        private Cellule<T> precedent;  // Référence au noeud précédent

        /**
         * Constructeur d'un noeud avec une valeur.
         * 
         * @param value La valeur à associer à ce noeud.
         */
        public Cellule(T value) {
            this.value = value;
            this.suivant = null;
            this.precedent = null;
        }

        /**
         * Récupère la valeur de ce noeud.
         * 
         * @return La valeur associée à ce noeud.
         */
        public T getValue() {
            return value;
        }

        /**
         * Définit la valeur de ce noeud.
         * 
         * @param value La valeur à définir.
         */
        public void setValue(T value) {
            this.value = value;
        }

        /**
         * Récupère le noeud suivant dans la liste.
         * 
         * @return Le noeud suivant dans la liste.
         */
        public Cellule<T> getSuivant() {
            return suivant;
        }

        /**
         * Définit le noeud suivant dans la liste.
         * 
         * @param suivant Le noeud à définir comme suivant.
         */
        public void setSuivant(Cellule<T> suivant) {
            this.suivant = suivant;
        }

        /**
         * Récupère le noeud précédent dans la liste.
         * 
         * @return Le noeud précédent dans la liste.
         */
        public Cellule<T> getPrecedent() {
            return precedent;
        }

        /**
         * Définit le noeud précédent dans la liste.
         * 
         * @param precedent Le noeud à définir comme précédent.
         */
        public void setPrecedent(Cellule<T> precedent) {
            this.precedent = precedent;
        }
    }

    private Cellule<T> sentinelle;  // Le noeud sentinelle de la liste
    private Cellule<T> tail;  // Pointeur vers le dernier élément de la liste
    private int size;   // Taille de la liste

    /**
     * Constructeur pour initialiser une liste chaînée avec un noeud sentinelle.
     * La sentinelle sert d'interface pour le début et la fin de la liste, simplifiant la gestion des cas particuliers.
     * Le pointeur `tail` est initialisé pour pointer sur la sentinelle, indiquant que la liste est vide au départ.
     */
    public MyLinkedList() {
        sentinelle = new Cellule<>(null);  // Création du noeud sentinelle avec une valeur nulle
        sentinelle.setSuivant(sentinelle);
        sentinelle.setPrecedent(sentinelle);
        tail = sentinelle;
        size = 0;
    }

    /**
     * Ajoute un nouvel élément à la fin de la liste.
     * Utilise le pointeur `tail` pour insérer le nouvel élément directement après le dernier élément, évitant ainsi de parcourir toute la liste.
     * 
     * @param value La valeur à ajouter à la liste.
     * @return Le noeud nouvellement ajouté.
     */
    public Cellule<T> add(T value) {
        size++;
        // Création d'un nouveau noeud avec la valeur spécifiée
        Cellule<T> nouveauNoeud = new Cellule<>(value);

        // Relier le dernier élément (tail) au nouveau noeud
        tail.setSuivant(nouveauNoeud);  // Le dernier élément de la liste pointe vers le nouveau noeud
        nouveauNoeud.setPrecedent(tail); // Le nouveau noeud pointe vers l'ancien dernier élément

        // Le nouveau noeud doit aussi pointer vers la sentinelle
        nouveauNoeud.setSuivant(sentinelle);  // Le suivant du nouveau noeud est la sentinelle
        sentinelle.setPrecedent(nouveauNoeud); // La sentinelle pointe maintenant sur le nouveau noeud

        // Mettre à jour le pointeur `tail` pour qu'il pointe vers ce nouveau noeud
        tail = nouveauNoeud;

        // Retourner le nouveau noeud ajouté
        return nouveauNoeud;
    }

    /**
     * Recherche et retourne le premier noeud contenant la valeur spécifiée.
     * Si la valeur n'est pas trouvée, retourne `null`.
     *
     * @param t La valeur à rechercher.
     * @return Le noeud contenant la valeur, ou `null` si la valeur n'est pas trouvée.
     */
    public Cellule<T> get(T t) {
        if (t == null) return null; // Ne rien faire si la valeur à rechercher est nulle

        // Parcourir la liste à partir du premier élément (après la sentinelle)
        Cellule<T> current = sentinelle.getSuivant();
        while (current != sentinelle) { // Parcourir jusqu'à revenir à la sentinelle
            if (t.equals(current.getValue()))
                return current; // Retourner le noeud trouvé

            current = current.getSuivant();
        }
        return null; // Si la valeur n'a pas été trouvée, retourner null
    }

    /**
     * Retire le premier élément de la liste.
     * Si la liste est vide, retourne `null`.
     * 
     * @return Le noeud supprimé, ou `null` si la liste est vide.
     */
    public Cellule<T> remove() {
        if (sentinelle.getSuivant() == sentinelle)
            return null;  // La liste est vide (la sentinelle pointe sur elle-même)

        size--;
        // Récupérer le premier élément (le noeud après la sentinelle)
        Cellule<T> premier = sentinelle.getSuivant();

        // Cas où le premier élément est aussi le dernier (le tail)
        if (premier.getSuivant() == sentinelle) {
            // Si la liste ne contient qu'un seul élément, après la suppression, la sentinelle doit pointer sur elle-même
            sentinelle.setSuivant(sentinelle);  // La sentinelle pointe maintenant sur elle-même
            tail = sentinelle;  // Le tail est réinitialisé à la sentinelle (liste vide)
        }
        else {
            // Sinon, la liste contient plusieurs éléments, mettre à jour les pointeurs
            sentinelle.setSuivant(premier.getSuivant());
            premier.getSuivant().setPrecedent(sentinelle);
        }

        // Déconnecter le premier élément de la liste
        premier.setSuivant(null);
        premier.setPrecedent(null);

        // Retourner le noeud supprimé
        return premier;
    }

    /**
     * Retire un noeud spécifique contenant la valeur spécifiée de la liste.
     * Si le noeud est celui à la fin (le `tail`), le pointeur `tail` est mis à jour pour pointer sur l'élément précédent.
     * 
     * @param t La valeur du noeud à supprimer.
     */
    public void remove(T t) {
        // Chercher le noeud correspondant à la valeur
        Cellule<T> node = get(t);

        if (node != null) {
            size--; // Décrémenter la taille de la liste

            // Si la liste ne contient qu'un seul élément
            if (node.getSuivant() == node) {
                sentinelle.setSuivant(sentinelle);
                sentinelle.setPrecedent(sentinelle);
                tail = sentinelle;  // La liste devient vide, donc la sentinelle est aussi le tail
            }
            else {
                // Si le noeud à supprimer est le dernier, mettre à jour `tail`
                if (node == tail)
                    tail = node.getPrecedent();  // Mise à jour de `tail` si le noeud est le dernier

                // Relier le noeud précédent au suivant du noeud à supprimer
                node.getPrecedent().setSuivant(node.getSuivant());
                node.getSuivant().setPrecedent(node.getPrecedent());
            }

            // Déconnecter le noeud de la liste
            node.setSuivant(null);
            node.setPrecedent(null);
        }
    }

    /**
     * Retire le dernier élément de la liste.
     * Utilise le pointeur `tail` pour supprimer l'élément à la fin de la liste en temps constant.
     * 
     * @return Le noeud supprimé, ou `null` si la liste est vide.
     */
    public Cellule<T> removeFromTail() {
        if (tail == sentinelle)
            return null;  // La liste est vide, donc rien à supprimer
    
        size--;
        // Récupérer le dernier élément (le noeud pointé par `tail`)
        Cellule<T> dernier = tail;
    
        // Mettre à jour le pointeur `tail` pour qu'il pointe sur l'élément précédent
        Cellule<T> avantDernier = tail.getPrecedent();
        tail = avantDernier;
    
        if (tail != sentinelle) {
            tail.setSuivant(sentinelle);  // Le dernier élément pointe maintenant vers la sentinelle
            sentinelle.setPrecedent(tail); // La sentinelle pointe maintenant vers l'avant-dernier élément
        }
        else
            sentinelle.setSuivant(sentinelle); // Si la liste est vide après suppression

        // Déconnecter le dernier élément de la liste
        dernier.setSuivant(null);
        dernier.setPrecedent(null);
    
        // Retourner le noeud supprimé
        return dernier;
    }

    /**
     * Retourne une représentation sous forme de chaîne de caractères des éléments de la liste,
     * du noeud sentinelle jusqu'à tail.
     * 
     * @return Une chaîne de caractères représentant la liste.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Cellule<T> current = sentinelle.getSuivant();  // Commencer juste après la sentinelle

        // Parcourir la liste jusqu'à la fin
        while (current != null && current != sentinelle) {
            sb.append(current.getValue().toString());  // Ajouter la valeur de l'élément à la chaîne
            if (current.getSuivant() != null && current.getSuivant() != sentinelle)
                sb.append(" <-> ");  // Ajouter une flèche pour indiquer la double liaison

            current = current.getSuivant();  // Passer au noeud suivant
        }
        return sb.toString();  // Retourner la représentation sous forme de chaîne
    }

    /**
     * Vérifie si la liste est vide.
     * La liste est considérée vide si la sentinelle pointe vers elle-même (aucun élément).
     * 
     * @return `true` si la liste est vide, sinon `false`.
     */
    public boolean isEmpty() {
        return sentinelle.getSuivant() == sentinelle;  // Si la sentinelle pointe sur elle-même, la liste est vide
    }

    /**
     * @return Le nombre de noeud géré par cette liste.
     */
    public int size() {
        return size;
    }

    /**
     * Récupère le noeud sentinelle de la liste.
     * La sentinelle est utilisée comme point d'entrée et de fin pour simplifier les ajouts et suppressions.
     * 
     * @return Le noeud sentinelle.
     */
    public Cellule<T> getRoot() {
        return sentinelle;
    }

    /**
     * Récupère le dernier noeud de la liste.
     * 
     * @return Le dernier noeud de la liste, ou la sentinelle si la liste est vide.
     */
    public Cellule<T> getTail() {
        return tail;
    }

    /**
     * Vide la liste et réinitialise sa taille.
     */
    public void clear() {
        sentinelle.setSuivant(sentinelle);
        sentinelle.setPrecedent(sentinelle);
        tail = sentinelle;
        size = 0;
    }
}