import java.util.ArrayList;

/**
 * La classe MyRecord représente un enregistrement contenant un tuple de valeurs et leurs types respectifs.
 * Chaque enregistrement est constitué d'une liste de paires, où chaque paire contient une valeur et son type.
 */
public class MyRecord{
    // Liste qui contient les paires de valeurs et de types
    private ArrayList<Pair<Object, DataType>> tuple;

    /**
     * Constructeur de la classe MyRecord.
     * Initialise une liste vide de paires (valeur, type).
     */
    public MyRecord(){
        tuple = new ArrayList<>();
    }

    /**
     * Ajoute une valeur et son type dans le tuple.
     * 
     * @param value La valeur à ajouter dans le tuple.
     * @param type Le type de la valeur.
     */
    public void addValue(Object value, DataType type){
        tuple.add(new Pair<>(value, type));
    }

    /**
     * Retourne la valeur à l'index donné dans le tuple.
     * 
     * @param index L'index de la valeur à récupérer.
     * @return La valeur à l'index donné.
     */
    public Object getValue(int index){
        return tuple.get(index).getFirst();
    }

    /**
     * Retourne le type de la valeur à l'index donné dans le tuple.
     * 
     * @param index L'index du type à récupérer.
     * @return Le type de la valeur à l'index donné.
     */
    public DataType getType(int index){
        return tuple.get(index).getSecond();
    }

    /**
     * Retourne la taille du tuple (le nombre d'éléments dans le tuple).
     * 
     * @return Le nombre d'éléments dans le tuple.
     */
    public int getSize(){
        return tuple.size();
    }

    /**
     * Affiche toutes les valeurs et leurs types dans le tuple.
     */
    public void display(){
        for(int i = 0; i < tuple.size(); i++)
            System.out.println("Value = "+getValue(i)+"     Type = "+getType(i));
    }
}