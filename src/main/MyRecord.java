import java.util.ArrayList;

/**
 * La classe MyRecord représente un enregistrement contenant un tuple de valeurs et leurs types respectifs.
 * Chaque enregistrement est constitué d'une liste de paires, où chaque paire contient une valeur et son type.
 */
public class MyRecord{
    // Liste qui contient les paires de valeurs et de types
    private ArrayList<Pair<Object, DataType>> tuple;
    private Relation relation;  // Référence à la Relation pour connaître les contraintes


    /**
     * Constructeur de la classe MyRecord.
     * Initialise une liste vide de paires (valeur, type).
     */
    public MyRecord(Relation relation){
        tuple = new ArrayList<>();
        this.relation = relation;
    }

    /**
     * Ajoute une valeur et son type dans le tuple.
     * 
     * @param value La valeur à ajouter dans le tuple.
     * @param type Le type de la valeur.
     */
    public void addValue(Object value, DataType type){
        int index = tuple.size();  // Index basé sur la taille actuelle du tuple

        // Vérification de la taille et du type
        value = validateTupleAttribute(value, type, index);
        tuple.add(new Pair<>(value, type));
    }

    /**
     * Valide une valeur avant de l'ajouter au tuple.
     * Effectue les vérifications suivantes :
     * - Si l'index est valide en fonction du nombre d'attributs de la relation
     * - Si le type spécifié correspond au type attendu pour cet attribut
     * - Si l'objet de la valeur correspond au type de donnée attendu (ex : chaîne pour CHAR)
     * - Si la taille des chaînes respecte la limite spécifiée pour CHAR et VARCHAR
     *
     * @param value La valeur à valider.
     * @param type Le type de la valeur spécifié dans le tuple.
     * @param index L'index de l'attribut dans la relation, pour déterminer le type attendu.
     * @return La valeur validée, éventuellement modifiée (ex : chaîne complétée pour CHAR).
     * @throws IllegalArgumentException Si la validation échoue.
     */
    private Object validateTupleAttribute(Object value, DataType type, int index) {
        // Vérifie que l'index ne dépasse pas le nombre d'attributs définis dans la relation.
        if (index >= relation.getAttribut().size())
            throw new IllegalArgumentException("Le nombre de valeurs dépasse le nombre d'attributs dans la relation.");

        // Récupère le type attendu pour l'attribut à l'index donné.
        DataType expectedType = relation.getType(index);

        // Vérifie si le type de donnée spécifié correspond au type attendu.
        if (expectedType != type)
            throw new IllegalArgumentException("Le type de la valeur ne correspond pas à celui de l'attribut.");

        // Récupère la longueur maximale autorisée pour les chaînes si applicable.
        int maxLength = relation.getLength(index);

        // Effectue une vérification basée sur le type de donnée pour s'assurer que
        // l'objet valeur est du type attendu et respecte les contraintes spécifiques.
        switch (type) {
            case CHAR:
            case VARCHAR:
                // Vérifie que la valeur est une instance de String.
                if (!(value instanceof String))
                    throw new IllegalArgumentException("La valeur doit être une chaîne pour le type CHAR ou VARCHAR.");

                // Convertit la valeur en chaîne pour vérifier sa longueur.
                String strValue = (String) value;

                // Vérifie que la longueur de la chaîne ne dépasse pas la longueur maximale.
                if (strValue.length() > maxLength)
                    throw new IllegalArgumentException("La chaîne dépasse la taille maximale autorisée pour l'attribut.");

                break;

            case INT:
                // Vérifie que la valeur est une instance d'Integer.
                if (!(value instanceof Integer))
                    throw new IllegalArgumentException("La valeur doit être un entier pour le type INT.");

                break;

            case REAL:
                // Vérifie que la valeur est une instance de Float ou Double.
                if (!(value instanceof Float) && !(value instanceof Double))
                    throw new IllegalArgumentException("La valeur doit être un nombre réel pour le type REAL.");

                break;

            case DATE:
                // Vérifie que la valeur est une instance de Date.
                if (!(value instanceof Date))
                    throw new IllegalArgumentException("La valeur doit être une date pour le type DATE.");

                break;

            default:
                // Lève une exception si le type de donnée n'est pas supporté.
                throw new IllegalArgumentException("Type de donnée non supporté.");
        }

        // Retourne la valeur validée ou modifiée si nécessaire (ex : chaîne complétée pour CHAR).
        return value;
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
     * 
     * @return Le nombre d'éléments dans le tuple.
     */
    public int getSize(){
        return tuple.size();
    }

    /**
     * 
     * @return La taille du tuple en octet.
     */
    public int getSizeOctet(){
        int sum = 0;

        for(int i = 0; i < tuple.size(); i++)
            sum += relation.getLength(i);

        return sum;
    }

    /**
     * Affiche toutes les valeurs et leurs types dans le tuple.
     */
    public void display(){
        for(int i = 0; i < tuple.size(); i++)
            System.out.println("Value = "+getValue(i)+"     Type = "+getType(i));
    }
}