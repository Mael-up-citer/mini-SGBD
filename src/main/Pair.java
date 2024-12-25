import java.util.Objects;

/**
 * Classe générique pour stocker un couple de valeurs de types différents.
 * Cette classe permet de stocker deux valeurs de types différents et fournit des méthodes
 * pour accéder et modifier ces valeurs.
 *
 * @param <T1> Le type de la première valeur du couple.
 * @param <T2> Le type de la deuxième valeur du couple.
 * 
 * @author Mael Lecene
 */
public class Pair<T1, T2> {

    // Première valeur du couple
    private T1 first;  

    // Deuxième valeur du couple
    private T2 second; 

    /**
     * Constructeur de la classe Pair qui initialise les deux valeurs du couple.
     * 
     * @param first La première valeur à stocker dans le couple.
     * @param second La deuxième valeur à stocker dans le couple.
     */
    public Pair(T1 first, T2 second){
        this.first = first;
        this.second = second;
    }

    /**
     * Constructeur vide de la classe Pair.
     * 
     */
    public Pair(){
    }

    /**
     * Méthode equals pour comparer deux objets Pair.
     * Deux paires sont considérées égales si leurs valeurs (first et second) sont égales.
     *
     * @param obj L'objet à comparer avec la paire actuelle.
     * @return true si les paires sont égales, false sinon.
     */
    @Override
    public boolean equals(Object obj) {
        // Vérifie si c'est la même instance
        if (this == obj)
            return true;

        // Vérifie si l'objet est de la bonne classe
        if (obj == null || getClass() != obj.getClass())
            return false;

        // Cast l'objet en Pair
        Pair<?, ?> other = (Pair<?, ?>) obj;

        // Compare les deux éléments de la paire
        return Objects.equals(first, other.first) && Objects.equals(second, other.second);
    }

    /**
     * Méthode hashCode pour générer un code de hachage pour la paire.
     * Le code de hachage est basé sur les valeurs first et second.
     *
     * @return le code de hachage pour la paire.
     */
    @Override
    public int hashCode() {
        return Objects.hash(first, second); // Utilise les objets pour calculer le code de hachage
    }

    /**
     * Méthode toString qui retourne une représentation sous forme de chaîne de caractères du couple.
     *
     * @return La représentation en chaîne de caractères du couple sous la forme "first=value, second=value".
     */
    @Override
    public String toString() {
        return first + "\t" + second;
    }

    /**
     * Retourne la première valeur du couple.
     * 
     * @return La première valeur du couple.
     */
    public T1 getFirst(){
        return first;
    }

    /**
     * Retourne la deuxième valeur du couple.
     * 
     * @return La deuxième valeur du couple.
     */
    public T2 getSecond(){
        return second;
    }

    /**
     * Modifie la première valeur du couple.
     * 
     * @param t La nouvelle valeur pour la première valeur du couple.
     */
    public void setFirst(T1 t){
        first = t;
    }

    /**
     * Modifie la deuxième valeur du couple.
     * 
     * @param t La nouvelle valeur pour la deuxième valeur du couple.
     */
    public void setSecond(T2 t){
        second = t;
    }
}