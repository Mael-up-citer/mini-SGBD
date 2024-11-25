/**
 * La classe Condition représente une condition d'une requête SQL dans une clause WHERE.
 * Elle permet de comparer deux termes (qui peuvent être des colonnes ou des valeurs constantes)
 * en utilisant un opérateur de comparaison (par exemple '=', '<', '>', '>=', '<=').
 */
public class Condition {

    private final String TERME1;   // Le premier terme de la condition, peut être une colonne ou une valeur constante.
    private final String TERME2;   // Le second terme de la condition, peut être une colonne ou une valeur constante.
    private final String OPERATEUR; // L'opérateur de comparaison, comme '=', '<', '>', '<=', '>=', '<>'.

    /**
     * Constructeur de la classe Condition.
     * 
     * @param terme1 Le premier terme de la condition (par exemple, un nom de colonne ou une valeur constante).
     * @param operateur L'opérateur de comparaison (par exemple, '=', '<', '>', etc.).
     * @param terme2 Le second terme de la condition (par exemple, un nom de colonne ou une valeur constante).
     */
    public Condition(String terme1, String operateur, String terme2) {
        this.TERME1 = terme1.toUpperCase();
        this.OPERATEUR = operateur;
        this.TERME2 = terme2.toUpperCase();
    }

    /**
     * Cette méthode évalue si la condition est satisfaite par un tuple donné.
     * Elle compare les valeurs des termes de la condition en fonction de l'opérateur spécifié.
     * 
     * @param relation La relation contenant les attributs (colonnes) et leurs types.
     * @return true si la condition est satisfaite, false sinon.
     */
    public boolean evaluate(Relation relation, MyRecord record) throws Exception{
        // Obtenir les valeurs des termes en fonction de la colonne ou de la constante.
        Pair<Object, DataType> value1 = getValue(TERME1, relation, record);
        Pair<Object, DataType> value2 = getValue(TERME2, relation, record);

        // Comparer les deux valeurs selon l'opérateur.
        return compare(value1.getFirst(), value1.getSecond(), value2.getFirst(), value2.getSecond());
    }

    /**
     * Cette méthode récupère la valeur d'un terme, soit à partir du tableau record (si c'est une colonne),
     * soit comme valeur constante (si c'est une valeur directe).
     * 
     * @param terme Le terme à analyser (peut être une colonne ou une valeur constante).
     * @param relation La relation contenant les attributs (colonnes).
     * @return La valeur du terme.
     */
    private Pair<Object, DataType> getValue(String terme, Relation relation, MyRecord record)throws Exception {
        // Vérifier si le terme est une colonne.
        Pair<Object, DataType> res = isAttrb(terme, relation, record);

        return (res != null)?res:parseConstant(terme);
    }    

    /**
     * Vérifie si un terme est une colonne (par rapport aux noms de colonnes).
     * 
     * @param terme Le terme à vérifier.
     * @param relation La relation contenant les attributs.
     * @return true si le terme est une colonne, false sinon.
     */

    private Pair<Object, DataType> isAttrb(String terme, Relation relation, MyRecord record) {
    
        // Parcourir les attributs pour voir si le terme correspond à un nom de colonne
        for (int i = 0; i < relation.getAttribut().size(); i++) {
            String attrbName = relation.getNameAttribut(i);
    
            // Si le terme est égal à un attribut, retourner la valeur de cet attribut
            if (terme.equals(attrbName))
                return new Pair<>(record.getValue(i), record.getType(i));
        }
        // Sinon, retourne null
        return null;
    }

    /**
     * Cette méthode permet de convertir une valeur constante en son type correct.
     * Si c'est un nombre, le convertir en Integer ou Float. Si c'est une chaîne, la retourner telle quelle.
     * 
     * @param terme La valeur sous forme de chaîne à convertir.
     * @return L'objet correspondant à la valeur constante (Integer, Float ou String).
     */
    private Pair<Object, DataType> parseConstant(String terme) throws Exception{
        // Si la valeur est entre guillemets, il s'agit d'une chaîne de caractères.
        if (terme.startsWith("'") && terme.endsWith("'"))
            return new Pair<>(terme.substring(1, terme.length() - 1), DataType.CHAR);  // Enlever les guillemets autour de la chaîne.

        // Si la valeur est un nombre (entier ou flottant), la convertir en Integer ou Float
        try {
            // Si c'est un int
            if (terme.matches("-?\\d+"))
                return new Pair<>(Integer.parseInt(terme), DataType.INT);
            // Si c'est un float
            else if (terme.matches("-?\\d*\\.\\d+"))
                return new Pair<>(Float.parseFloat(terme), DataType.REAL);

        } catch (NumberFormatException e) {
        }

        throw new Exception("Type de "+terme+" est inconnu");
    }

    /**
     * Compare deux valeurs en fonction de leur type et de l'opérateur de comparaison.
     * La méthode gère les types de données INTEGER, REAL, CHAR/VARCHAR et DATE.
     * Si les types sont différents, la première valeur est convertie en un type compatible avec la seconde.
     * 
     * @param value1 La première valeur à comparer.
     * @param type1 Le type de la première valeur (DataType).
     * @param value2 La deuxième valeur à comparer.
     * @param type2 Le type de la deuxième valeur (DataType).
     * @return true si les deux valeurs sont égales selon leur type, false sinon.
     * @throws Exception Si les types ne sont pas compatibles ou si une exception de conversion survient.
     */
    private boolean compare(Object value1, DataType type1, Object value2, DataType type2) throws Exception {
        // Si les types des deux valeurs sont identiques, on peut les comparer directement
        if (type1 == type2) {
            // Appeler la méthode de comparaison des valeurs en utilisant le même type
            return compareValues(value1, value2, type1);
        }

        // Si les types sont différents, on tente de convertir la première valeur en un type compatible avec le type de la deuxième valeur
        Object convertedValue1 = DataType.convertToCompatibleType(value1, type2);
        
        // Comparer la valeur convertie avec la deuxième valeur (qui est déjà du bon type)
        return compareValues(convertedValue1, value2, type2);
    }

    /**
     * Compare deux valeurs de même type selon le type spécifié.
     * Cette méthode est utilisée pour comparer des entiers, des réels (floats), des chaînes de caractères (CHAR/VARCHAR),
     * ou des dates.
     * 
     * @param value1 La première valeur à comparer.
     * @param value2 La deuxième valeur à comparer.
     * @param type Le type des deux valeurs (DataType).
     * @return true si les valeurs sont égales selon leur type, false sinon.
     * @throws Exception Si le type n'est pas pris en charge pour la comparaison.
     */
    private boolean compareValues(Object value1, Object value2, DataType type) throws Exception {
        // Comparer les valeurs en fonction du type
        switch (type) {
            case INT:
                // Comparaison des entiers (Integer)
                return Integer.compare((Integer) value1, (Integer) value2) == 0;
            case REAL:
                // Comparaison des réels (Float)
                return Float.compare((Float) value1, (Float) value2) == 0;
            case CHAR:
            case VARCHAR:
                // Comparaison des chaînes de caractères (String)
                return value1.equals(value2);
            case DATE:
                // Comparaison des dates (Date)
                return ((Date) value1).compareTo((Date) value2) == 0;
            default:
                // Si le type n'est pas pris en charge, lever une exception
                throw new UnsupportedOperationException("Type non pris en charge pour la comparaison");
        }
    }  

    /**
     * Retourne le premier terme de la condition.
     * 
     * @return Le premier terme de la condition.
     */
    public String getTerme1() {
        return TERME1;
    }

    /**
     * Retourne le second terme de la condition.
     * 
     * @return Le second terme de la condition.
     */
    public String getTerme2() {
        return TERME2;
    }

    /**
     * Retourne l'opérateur de comparaison de la condition.
     * 
     * @return L'opérateur de comparaison.
     */
    public String getOperateur() {
        return OPERATEUR;
    }
}