// Importation de la classe ArrayList, une structure de données de type liste dynamique, 
// qui est utilisée comme base pour la classe MyRecord.
import java.util.ArrayList;
import java.util.Objects;

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
     * Méthode qui compare l'objet actuel avec un autre objet pour vérifier s'ils sont égaux.
     * Deux objets MyRecord sont considérés comme égaux s'ils contiennent les mêmes paires 
     * (valeur, type) dans le même ordre.
     *
     * @param obj L'objet à comparer avec l'objet actuel.
     * @return true si l'objet comparé est identique (même contenu) à l'objet actuel, sinon false.
     */
    @Override
    public boolean equals(Object obj) {
        // Vérifie si l'objet actuel est identique à l'objet comparé
        if (this == obj)
            return true;

        // Vérifie si l'objet est null ou si les classes sont différentes
        if (obj == null || getClass() != obj.getClass())
            return false;

        // Cast l'objet en MyRecord
        MyRecord other = (MyRecord) obj;

        // Compare les tailles des deux listes et vérifie si elles contiennent les mêmes éléments
        return this.size() == other.size() && this.containsAll(other);
    }

    /**
     * Méthode qui génère un code de hachage basé sur le contenu de l'objet actuel.
     * Le code de hachage est calculé en fonction des paires (valeur, type) présentes 
     * dans le MyRecord, garantissant que deux objets identiques génèrent le même code de hachage.
     *
     * @return Un entier représentant le code de hachage de l'objet actuel.
     */
    @Override
    public int hashCode() {
        // Utilise la méthode hash de la classe Objects pour générer un code de hachage basé sur les paires
        return Objects.hash(this); // Calcule un code de hachage basé sur le contenu de la liste
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
        if (value instanceof String)
            value = ((String)value).toUpperCase();

        // Si la vérification est réussie, ajoute la paire à la liste en utilisant la méthode de la classe parent.
        return super.add(new Pair<>(value, type));
    }
    
    /**
     * Redéfinition de la méthode {@code add} pour interdire l'ajout d'éléments dans {@code MyRecord}.
     * 
     * Cette méthode surcharge la méthode {@code add} héritée de {@code ArrayList} et lève une exception
     * {@code UnsupportedOperationException} chaque fois qu'une tentative d'ajout est effectuée.
     * 
     * Cela empêche toute modification de la liste en interdisant l'ajout de nouveaux éléments dans l'instance
     * de la classe {@code MyRecord}. Elle est utilisée pour garantir que la structure de données reste immuable
     * après sa création.
     * 
     * @param arg L'élément à ajouter à la collection (de type {@link Pair} avec un objet et un type de données).
     * @return Cette méthode ne retourne jamais une valeur, car elle lève systématiquement une exception.
     * 
     * @throws UnsupportedOperationException Si cette méthode est appelée pour tenter d'ajouter un élément.
     *         L'exception indique que l'ajout d'éléments n'est pas autorisé dans la classe {@code MyRecord}.
     */
    @Override
    public boolean add(Pair<Object, DataType> arg) throws UnsupportedOperationException {
        // Lever une exception pour interdire l'ajout
        throw new UnsupportedOperationException("L'ajout d'éléments par Pair est interdit dans MyRecord. Utiliser add(Object value, DataType type)");
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
                return value instanceof Float || value instanceof Double;
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
                if(this.getType(i) == DataType.CHAR) {
                    sum += relation.getLength(i)*2; // Taille fois 2 car 1char = 2octets
                }
                else {
                    sum += relation.getLength(i);   // Sinon on a juste a prendre la taille
                }
            }
        }
        return sum;
    }

    /**
     * Retourne une représentation sous forme de chaîne de caractères de l'objet MyRecord.
     * Cette méthode construit une chaîne contenant les valeurs et les types de toutes les paires
     * présentes dans le MyRecord.
     *
     * @return Une chaîne représentant l'objet MyRecord avec les valeurs et types des paires.
     */
    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();

        // Parcours le record et construit la chaîne
        for (int i = 0; i < this.size(); i++)
            res.append("Value = " + getValue(i) + "     Type = " + getType(i) + "\n");

        return res.toString();
    }

    /**
     * Retourne une représentation sous forme de chaîne de caractères des valeurs d'un record.
     * Cette méthode construit une chaîne contenant les valeurs toutes les paires
     *
     * @return Une chaîne représentant les valeurs des paires.
     */
    public String printValue() {
        StringBuilder res = new StringBuilder();
    
        // Parcours du record et construction de la chaîne avec les valeurs séparées par " ; "
        for (int i = 0; i < this.size(); i++) {
            res.append(getValue(i));
    
            // Ajouter un séparateur " ; " après chaque valeur sauf la dernière
            if (i < this.size() - 1)
                res.append(" ; ");
        }
        // Ajouter un point à la fin
        res.append(".");
    
        return res.toString();
    }    
}