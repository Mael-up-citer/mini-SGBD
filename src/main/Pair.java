/**
 * Classe générique pour stocker un couple de valeurs de types différents.
 * Cette classe permet de stocker deux valeurs de types différents et fournit des méthodes
 * pour accéder et modifier ces valeurs.
 *
 * @param <T1> Le type de la première valeur du couple.
 * @param <T2> Le type de la deuxième valeur du couple.
 */
public class Pair<T1, T2>{
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
    Pair(T1 first, T2 second){
        this.first = first;
        this.second = second;
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