import java.util.ArrayList;

/**
 * La classe RecordPrinter permet d'imprimer tous les enregistrements (tuples) 
 * d'un itérateur d'enregistrements donné en affichant les attributs spécifiés.
 */
public class RecordPrinter {
    
    // L'itérateur d'enregistrements qui permet de parcourir les enregistrements.
    IRecordIterator recordIterator;

    // La liste des noms des attributs à afficher.
    ArrayList<String> nameAttrb;

    /**
     * Constructeur de la classe RecordPrinter.
     * 
     * @param recordIterator L'itérateur d'enregistrements à parcourir.
     * @param nameAttrb La liste des noms des attributs à afficher.
     */
    RecordPrinter(IRecordIterator recordIterator, ArrayList<String> nameAttrb) {
        this.recordIterator = recordIterator; // Initialisation de l'itérateur d'enregistrements.
        this.nameAttrb = nameAttrb; // Initialisation de la liste des noms d'attributs.
    }

    /**
     * Cette méthode permet d'afficher tous les enregistrements en parcourant l'itérateur 
     * et en affichant les attributs spécifiés dans nameAttrb.
     * Elle affiche les attributs en tête de colonne, puis parcourt les enregistrements 
     * et les affiche un par un.
     */
    public void printAllRecord() {
        MyRecord record; // Déclaration de la variable pour contenir le record courant à afficher.
        int cpt = 0; // Compteur pour le nombre d'enregistrements affichés.

        // Affiche les noms des attributs spécifiés dans nameAttrb
        // Cela imprime la première ligne, les en-têtes des colonnes.
        for (String name : nameAttrb) System.out.print(name + "\t");

        // Saute à la ligne après avoir affiché les en-têtes
        System.out.println();

        // Parcourt les enregistrements à l'aide de l'itérateur
        // et affiche chaque record jusqu'à ce qu'il n'y ait plus de records
        while ((record = recordIterator.GetNextRecord()) != null) {
            // Affiche le premier attribut du record courant.
            // record.getFirst() renvoie le premier attribut, et record.getFirst().getFirst() 
            // renvoie sa valeur réelle (la première valeur associée au nom de l'attribut).
            System.out.println(record.printValue());
            // Incrémente le compteur du nombre d'enregistrements affichés.
            cpt++;
        }
        // Affiche le nombre total d'enregistrements imprimés.
        System.out.println("Total records=" + cpt);

        // Ferme l'itérateur pour libérer les ressources utilisées.
        recordIterator.Close();
    }
}