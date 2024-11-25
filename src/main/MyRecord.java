// Importation de la classe ArrayList, une structure de données de type liste dynamique, 
// qui est utilisée comme base pour la classe MyRecord.
import java.util.ArrayList;

/**
 * Classe MyRecord qui représente un enregistrement de paires (valeur, type).
 * Elle hérite d'ArrayList et stocke des objets de type Pair<Object, DataType>.
 */
public class MyRecord extends ArrayList<Pair<Object, DataType>> {

    /**
     * Constructeur par défaut qui initialise une liste vide.
     * Appelle le constructeur de la classe parent ArrayList.
     */
    public MyRecord() {
        super(); // Initialise une liste vide dans la classe parent.
    }

    /**
     * Surcharge de la méthode add pour ajouter une paire (valeur, type)
     * après vérification de la cohérence entre la valeur et son type attendu.
     *
     * @param value La paire (valeur, type) à ajouter.
     * @throws IllegalArgumentException si la valeur ne correspond pas au type attendu.
     * @return true si l'ajout est réussi, false sinon (hérité de ArrayList).
     */
    public boolean add(Object value, DataType type) throws IllegalArgumentException {
        // Vérifie la cohérence entre la valeur et le type en appelant isTypeConsistent.
        if (!isTypeConsistent(value, type)) {
            // Lève une exception en cas de non-correspondance entre le type attendu et la valeur fournie.
            throw new IllegalArgumentException("Type mismatch: expected " + type 
                                               + " but was " + value.getClass().getSimpleName());
        }
        // Si la vérification est réussie, ajoute la paire à la liste en utilisant la méthode de la classe parent.
        return super.add(new Pair<>(value, type));
    }

    /**
     * Méthode privée qui vérifie si la valeur est cohérente avec le type attendu.
     *
     * @param value La valeur à vérifier.
     * @param type  Le type attendu (DataType).
     * @return true si la valeur correspond au type, false sinon.
     */
    private boolean isTypeConsistent(Object value, DataType type) {
        // Vérifie le type de la valeur en fonction du type attendu.
        switch (type) {
            case INT:
                // Vérifie si la valeur est de type Integer.
                return value instanceof Integer;
            case REAL:
                // Vérifie si la valeur est de type Float ou Double (compatibilité pour les nombres réels).
                return value instanceof Float;
            case CHAR:
            case VARCHAR:
                // Vérifie si la valeur est une chaîne de caractères (String).
                return value instanceof String;
            case DATE:
                // Vérifie si la valeur est de type Date.
                return value instanceof Date;
            default:
                // Retourne false pour les types non reconnus.
                return false;
        }
    }

    /**
     * Retourne la valeur à l'index donné dans le tuple.
     *
     * @param index L'index de la valeur à récupérer.
     * @return La valeur à l'index donné.
     */
    public Object getValue(int index){
        return this.get(index).getFirst();
    }

    /**
     * Retourne le type de la valeur à l'index donné dans le tuple.
     * 
     * @param index L'index du type à récupérer.
     * @return Le type de la valeur à l'index donné.
     */
    public DataType getType(int index){
        return this.get(index).getSecond();
    }

    /**
     * 
     * @return La taille du tuple en octet.
     */
    public int getSizeOctet(Relation relation){
        int sum = 0;    // Initialise la variable sum à 0

        // Parcour chaque entrée du tuple
        for(int i = 0; i < this.size(); i++){
            // Si c'est un varchar on prend la taille dans le record
            if(this.getType(i) == DataType.VARCHAR){
                String tmp = (String)this.getValue(i);
                sum += tmp.length()*2;  // Taille fois 2 car 1char = 2octets
            }
            // Sinno on regarde dans la relation
            else{
                // Si c'est un char on regarde dans la relation
                if(this.getType(i) == DataType.CHAR)
                    sum += relation.getLength(i)*2; // Taille fois 2 car 1char = 2octets
                else
                    sum += relation.getLength(i);   // Sinon on a juste a prendre la taille
            }
        }
        return sum;
    }

    /**
     * @return le tuple sous forme de chaine
     */
    @Override
    public String toString(){
        StringBuilder res = new StringBuilder();
        // Parcour le record
        for(int i = 0; i < this.size(); i++)
            res.append("Value = "+getValue(i)+"     Type = "+getType(i)+"\n");

        return res.toString();
    }
}