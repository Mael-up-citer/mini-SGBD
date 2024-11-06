/**
 * Enumération représentant les différents types de données pris en charge.
 * Chaque type de données correspond à une représentation spécifique d'un élément 
 * dans la base de données ou dans une structure de données en mémoire.
 */
public enum DataType{
    /**
     * Correspond à un entier (int).
     */
    INT,       // Correspond à un entier

    /**
     * Correspond à un nombre à virgule flottante (float).
     */
    REAL,      // Correspond à un float

    /**
     * Correspond à une chaîne de caractères de taille fixe (CHAR).
     */
    CHAR,      // Correspond à une chaîne de caractères de taille fixe

    /**
     * Correspond à une chaîne de caractères de taille variable (VARCHAR).
     */
    VARCHAR,   // Correspond à une chaîne de caractères de taille variable

    /**
     * Correspond à un objet de type Date.
     */
    DATE;      // Correspond à un objet Date
}