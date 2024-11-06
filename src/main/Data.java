/**
 * Classe représentant un type de données avec un type spécifique et une longueur associée.
 * Cette classe est utilisée pour stocker les informations sur des types de données comme INT, REAL, DATE, CHAR et VARCHAR,
 * en tenant compte de la taille associée à chaque type.
 */
public class Data{
    private DataType type;    // Contient le type de données (par exemple, INT, REAL, DATE, CHAR, VARCHAR)
    private int length; // Taille en octets du type de données

    /**
     * Constructeur pour les types de données de taille constante (comme INT, REAL, DATE).
     * 
     * @param t Le type de données (DataType) à initialiser.
     * @throws IllegalArgumentException Si le type de données n'est pas parmi ceux supportés (INT, REAL, DATE).
     */
    public Data(DataType t){
        type = t;

        switch(t){
            case DATE:
                length = 4*3; // Date est sous forme de 3 int, donc la taille est de 12 octets
                break;
            case INT:
            case REAL:
                length = 4; // INT et REAL ont une taille de 4 octets
                break;
            default:
                throw new IllegalArgumentException("Cette méthode est applicable qu'au type INT, REAL et Date");
        }
    }

    /**
     * Constructeur pour les types de données de taille variable (comme CHAR et VARCHAR).
     * 
     * @param t Le type de données (DataType) à initialiser.
     * @param lg La longueur en octets du type de données.
     * @throws IllegalArgumentException Si le type de données n'est pas CHAR ou VARCHAR.
     */
    public Data(DataType t, int lg){
        type = t;

        if((t == DataType.CHAR) || (t == DataType.VARCHAR))
            length = lg;
        else
            throw new IllegalArgumentException("Cette méthode est applicable qu'au type CHAR et VARCHAR");
    }

    /**
     * Retourne le type de données de cet objet.
     * 
     * @return Le type de données (DataType).
     */
    public DataType getType(){
        return type;
    }

    /**
     * Retourne la longueur associée au type de données de cet objet.
     * 
     * @return La longueur du type de données en octets.
     */
    public int getLength(){
        return length;
    }

    /**
     * Définit la longueur du type de données. Cette méthode n'est applicable qu'aux types VARCHAR.
     * 
     * @param n La nouvelle longueur à assigner.
     * @throws IllegalArgumentException Si la longueur est modifiée pour un type qui n'est pas VARCHAR.
     */
    public void setLenght(int n){
        if(type == DataType.VARCHAR)
            length = n;
        else
            throw new IllegalArgumentException("Seul la taille des VARCHAR peut etre modifié !");
    }
}