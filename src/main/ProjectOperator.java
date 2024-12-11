import java.util.ArrayList;

/**
 * Cette classe est un opérateur de projection qui permet de filtrer les attributs d'un record 
 * pour ne conserver que ceux que l'on souhaite afficher.
 */
public class ProjectOperator implements IRecordIterator {

    // L'opérateur fils, qui est normalement un opérateur SELECT
    IRecordIterator scanner;
    // Liste des indices des attributs que l'on souhaite conserver dans le record.
    ArrayList<Integer> attrbWeWant;

    /**
     * Constructeur de la classe ProjectOperator.
     *
     * @param rs             L'opérateur fils (généralement un SELECT) qui fournit les records à filtrer.
     * @param attrbWeWant    La liste des indices des attributs à afficher dans le record projeté.
     */
    ProjectOperator(IRecordIterator rs, ArrayList<Integer> attrbWeWant) {
        this.scanner = rs;               // Initialise l'opérateur fils (le scanner qui fournit les records)
        this.attrbWeWant = attrbWeWant;   // Initialise la liste des indices des attributs à afficher
    }

    /**
     * Récupère le prochain record projeté en appliquant la sélection des attributs.
     *
     * @return Le prochain record projeté ou null si aucun record n'est disponible.
     */
    @Override
    public MyRecord GetNextRecord() {
        // Obtient le prochain record de l'opérateur fils (SELECT)
        MyRecord record = scanner.GetNextRecord();

        // Si le record est null, cela signifie qu'il n'y a plus de records à traiter.
        if (record == null)
            return null;    // Retourne null si aucun record n'est disponible.

        // Crée un nouveau record qui contiendra uniquement les attributs sélectionnés.
        MyRecord res = new MyRecord();

        // Parcourt les indices des attributs que l'on souhaite conserver dans le record.
        for (int index : attrbWeWant) {
            // Ajoute l'attribut de l'index sélectionné dans le nouveau record.
            // get(index).getFirst() récupère la valeur de l'attribut et get(index).getSecond() récupère son type.
            res.add(record.get(index).getFirst(), record.get(index).getSecond());
        }
        // Retourne le record projeté (avec seulement les attributs souhaités).
        return res;
    }

    /**
     * Réinitialise l'opérateur de scan.
     * Appelle la méthode Reset de l'opérateur fils pour le réinitialiser.
     */
    @Override
    public void Reset() {
        scanner.Reset();  // Réinitialise le scanner (l'opérateur fils).
    }

    /**
     * Ferme l'opérateur de scan.
     * Appelle la méthode Close de l'opérateur fils pour le fermer proprement.
     */
    @Override
    public void Close() {
        scanner.Close();  // Ferme le scanner (l'opérateur fils).
    }
}