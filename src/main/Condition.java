/**
 * Classe représentant une condition dans une requête SQL, par exemple : 
 * "age > 30" ou "nom = 'John'".
 */
public class Condition {

    private final String terme1; // Premier terme de la condition (ex: 'age')
    private final String operateur; // L'opérateur de comparaison (ex: '=', '>', '<', etc.)
    private final String terme2; // Deuxième terme de la condition (ex: '30' ou 'John')

    /**
     * Constructeur de la classe Condition.
     * 
     * @param terme1 Le premier terme de la condition (peut être un attribut ou une constante).
     * @param operateur L'opérateur de comparaison (ex: '=', '<', etc.).
     * @param terme2 Le deuxième terme de la condition (peut être une constante ou un attribut).
     */
    public Condition(String terme1, String operateur, String terme2) {
        this.terme1 = terme1.toUpperCase();  // Convertir le terme1 en majuscules pour normaliser la casse
        this.operateur = operateur;  // Affecter l'opérateur
        this.terme2 = terme2.toUpperCase();  // Convertir le terme2 en majuscules pour normaliser la casse
    }

    /**
     * Évalue la condition en utilisant une relation et un enregistrement.
     * 
     * @param relation La relation dans laquelle la condition sera évaluée.
     * @param record L'enregistrement à vérifier.
     * @return true si la condition est satisfaite, sinon false.
     * @throws Exception Si une erreur survient lors de l'évaluation de la condition.
     */
    public boolean evaluate(Relation relation, MyRecord record) throws Exception {
        // Récupérer les valeurs des termes 1 et 2 de la condition
        Pair<Object, DataType> value1 = getValue(terme1, relation, record);
        Pair<Object, DataType> value2 = getValue(terme2, relation, record);

        // Comparer les deux valeurs avec le type de données du premier terme
        return compare(value1.getFirst(), value2.getFirst(), value1.getSecond());
    }

    /**
     * Récupère la valeur d'un terme, qu'il s'agisse d'un attribut dans la relation
     * ou d'une constante.
     * 
     * @param terme Le terme à analyser (peut être une colonne ou une constante).
     * @param relation La relation dans laquelle chercher l'attribut.
     * @param record L'enregistrement contenant les valeurs de la relation.
     * @return La valeur du terme et son type associé.
     * @throws Exception Si le terme n'est pas trouvé ou ne peut pas être évalué.
     */
    private Pair<Object, DataType> getValue(String terme, Relation relation, MyRecord record) throws Exception {
        // Vérifier si c'est un attribut dans la relation
        Pair<Object, DataType> value = isAttrb(terme, relation, record);

        if (value != null)
            return value;  // Si c'est un attribut, retourner la valeur correspondante

        // Si ce n'est pas un attribut, tenter de le traiter comme une constante
        value = parseConstant(terme);

        if (value != null)
            return value;  // Si c'est une constante, retourner sa valeur

        // Si ce n'est ni un attribut ni une constante, lever une exception
        throw new Exception("Valeur ou attribut non trouvé : " + terme);
    }

    /**
     * Vérifie si le terme donné est un attribut dans la relation.
     * 
     * @param terme Le terme à vérifier (nom de l'attribut).
     * @param relation La relation dans laquelle chercher l'attribut.
     * @param record L'enregistrement contenant les valeurs des attributs.
     * @return La paire (valeur, type) de l'attribut si trouvé, sinon null.
     */
    private Pair<Object, DataType> isAttrb(String terme, Relation relation, MyRecord record) {
        // Parcourir tous les attributs de la relation pour vérifier si le terme correspond à un nom d'attribut
        for (int i = 0; i < relation.getAttribut().size(); i++)
            // Test si le terme vaut l'attribut courrant dans la relation
            if (terme.equalsIgnoreCase(relation.getNameAttribut(i)))
                // Retourner la valeur de l'attribut et son type associé
                return new Pair<>(record.getValue(i), record.getType(i));

        return null;  // Retourner null si ce n'est pas un attribut
    }

    /**
     * Analyse un terme pour le convertir en une constante (chaîne, entier, réel).
     * 
     * @param terme Le terme à analyser (une constante).
     * @return La paire (valeur, type) correspondant à la constante.
     * @throws Exception Si le terme ne peut pas être parsé comme une constante valide.
     */
    private Pair<Object, DataType> parseConstant(String terme) throws Exception {
        // Si le terme commence et finit par des guillemets, c'est une chaîne de caractères
        if (terme.startsWith("'") && terme.endsWith("'"))
            return new Pair<>(terme.substring(1, terme.length() - 1), DataType.VARCHAR);  // Chaîne de caractères

        try {
            // Si le terme est un nombre entier
            if (terme.matches("-?\\d+")) {
                return new Pair<>(Integer.parseInt(terme), DataType.INT);  // Entier
            }
            // Si le terme est un nombre réel
            else if (terme.matches("-?\\d*\\.\\d+")) {
                return new Pair<>(Float.parseFloat(terme), DataType.REAL);  // Réel
            }
        } catch (NumberFormatException e) {
            throw new Exception("Impossible de parser la constante : " + terme);
        }
        return null;  // Retourner null si ce n'est pas une chaîne, un entier, ou un réel
    }

    /**
     * Méthode générique qui compare deux valeurs selon l'opérateur spécifié.
     * 
     * @param value1 La première valeur à comparer.
     * @param value2 La deuxième valeur à comparer.
     * @param type Le type de données des valeurs (utilisé pour vérifier la compatibilité des types).
     * @return true si les valeurs satisfont la condition, sinon false.
     * @throws Exception Si les valeurs ne sont pas comparables ou si un opérateur invalide est donné.
     */
    @SuppressWarnings("unchecked")  // Ignore les avertissements de type
    private boolean compare(Object value1, Object value2, DataType type) throws Exception {
        // Vérifier que les deux valeurs sont non nulles
        if (value1 == null || value2 == null)
            throw new IllegalArgumentException("Les valeurs à comparer ne peuvent pas être nulles.");

        // Vérifier que les valeurs sont du même type
        if (value1.getClass() != value2.getClass())
            throw new IllegalArgumentException("Les valeurs à comparer doivent être du même type.");

        // Comparer les valeurs en utilisant la méthode Comparable (les objets doivent être comparables)
        Comparable<Object> comp1 = (Comparable<Object>) value1;
        Comparable<Object> comp2 = (Comparable<Object>) value2;

        System.out.println(comp1+" "+operateur+" "+comp2);
        System.out.println(comp1.compareTo(comp2)+"\n");

        // Exécution de la comparaison en fonction de l'opérateur spécifié
        switch (operateur) {
            case "=": return comp1.compareTo(comp2) == 0;  // égalité
            case "<": return comp1.compareTo(comp2) < 0;   // inférieur
            case ">": return comp1.compareTo(comp2) > 0;   // supérieur
            case "<=": return comp1.compareTo(comp2) <= 0;  // inférieur ou égal
            case ">=": return comp1.compareTo(comp2) >= 0;  // supérieur ou égal
            case "<>": return comp1.compareTo(comp2) != 0;  // différent
            default: throw new UnsupportedOperationException("Opérateur non supporté: " + operateur);
        }
    }
}