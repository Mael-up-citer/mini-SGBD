/**
 * Enumération représentant les différents types de données pris en charge.
 * Chaque type de données correspond à une représentation spécifique d'un élément 
 * dans la base de données ou dans une structure de données en mémoire.
 */
public enum DataType{
    /**
     * Correspond à un entier (int).
     */
    INT,

    /**
     * Correspond à un nombre à virgule flottante (float).
     */
    REAL,

    /**
     * Correspond à une chaîne de caractères de taille fixe (CHAR).
     */
    CHAR,

    /**
     * Correspond à une chaîne de caractères de taille variable (VARCHAR).
     */
    VARCHAR,

    /**
     * Correspond à un objet de type Date.
     */
    DATE;

    /**
     * Convertit un objet en un type compatible avec le type cible spécifié.
     * Cette méthode tente de convertir l'objet de manière appropriée selon son type actuel 
     * vers le type de données cible (par exemple, convertir un entier en float ou une chaîne en date).
     * 
     * @param value L'objet à convertir. Cela peut être une valeur de type `Integer`, `Float`, `String`, ou `Date`.
     * @param targetType Le type de données cible dans lequel l'objet doit être converti. Ce type est défini par l'énumération `DataType`.
     * @return L'objet converti vers le type cible. 
     * @throws Exception Si la conversion échoue (par exemple, une chaîne de caractères ne peut pas être convertie en nombre).
     */
    public static Object convertToCompatibleType(Object value, DataType targetType) throws Exception {
        // Si la valeur est nulle, on retourne directement null sans effectuer de conversion
        if (value == null)
            return null;

        // Traitement en fonction du type cible
        switch (targetType) {
            case INT:
                // Si la valeur est de type Float, on la convertit en Integer
                if (value instanceof Float)
                    return ((Number) value).intValue();

                // Si la valeur est une chaîne de caractères, on tente de la convertir en Integer
                if (value instanceof String)
                    return Integer.parseInt((String) value);

                // Si la valeur est de type Date, on peut retourner le nombre de millisecondes (int).
                if (value instanceof Date)
                    return (int) ((Date) value).timestamp();  // Convertir la Date en timestamp Unix (millisecondes depuis 1970)

                // Si la valeur est déjà de type Integer, on la retourne telle quelle
                return value;

            case REAL:
                // Si la valeur est un Integer, on le convertit en Float
                if (value instanceof Integer)
                    return ((Integer) value).floatValue();

                // Si la valeur est une chaîne de caractères, on la convertit en Float
                if (value instanceof String)
                    return Float.parseFloat((String) value);

                // Si la valeur est déjà de type Float, on la retourne telle quelle
                return value;

            case CHAR:
            case VARCHAR:
                // Si la valeur est de tout autre type, on la convertit en String
                return value.toString();

            case DATE:
                // Si la valeur est une chaîne de caractères, on tente de la convertir en Date avec le format 'yyyy-MM-dd'.
                if (value instanceof String)
                    return Date.toDate((String)value);

                // Si la valeur est déjà de type Date, on la retourne telle quelle.
                return value;

            // Si le type cible n'est pas pris en charge, une exception est levée.
            default:
                throw new UnsupportedOperationException("Conversion non supportée pour le type : " + targetType);
        }
    }
}