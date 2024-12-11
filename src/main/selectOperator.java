import java.util.ArrayList;

/**
 * Opérateur de sélection implémentant les interfaces {@link Operateur} et {@link IRecordIterator}.
 * Cette classe permet d'itérer sur des enregistrements d'une relation en filtrant ceux qui satisfont
 * certaines conditions spécifiées.
 */
public class selectOperator implements IRecordIterator {

    // Itérateur de pages, permettant de parcourir les pages de données.
    private PageDirectoryIterator pageIterator;
    // Itérateur pour parcourir les tuples d'une page.
    private IRecordIterator tupleIterator;

    // Relation associée aux enregistrements.
    private Relation relation;
    // Gestionnaire de buffer.
    private BufferManager bm;

    // Liste des conditions à appliquer pour filtrer les enregistrements.
    private ArrayList<Condition> conditions;

    /**
     * Constructeur pour l'opérateur de sélection.
     *
     * @param pageIterator Itérateur pour parcourir les pages de la relation.
     * @param relation     Relation contenant les enregistrements.
     * @param bm           Gestionnaire de mémoire tampon.
     */
    public selectOperator(PageDirectoryIterator pageIterator, Relation relation, ArrayList<Condition> conditions, BufferManager bm) {
        this.pageIterator = pageIterator;
        this.relation = relation;
        this.bm = bm;
        this.conditions = conditions;

        // Réinitialise les itérateurs pour démarrer la lecture.
        Reset();
    }

    /**
     * Récupère le prochain enregistrement qui satisfait les conditions.
     * 
     * @return Le prochain enregistrement valide ou null s'il n'y en a plus.
     */
    @Override
    public MyRecord GetNextRecord() {
        MyRecord record;

        do {
            try {
                // Tente de récupérer le prochain enregistrement.
                record = tupleIterator.GetNextRecord();

                // Si aucun enregistrement n'est disponible dans l'itérateur actuel.
                if (record == null) {
                    // Passe à la page suivante.
                    PageId id = pageIterator.GetNextDataPageId();

                    // Si aucune page suivante n'est disponible, retourne null.
                    if (id == null)
                        return null;

                    // Ferme l'itérateur actuel et crée un nouvel itérateur pour la nouvelle page.
                    tupleIterator.Close();
                    tupleIterator = new DataPageHoldRecordIterator(relation, bm.getPage(id), bm, id);

                    // Tente de récupérer le premier enregistrement de la nouvelle page.
                    record = tupleIterator.GetNextRecord();
                }
            } catch (Exception e) {
                // Relève une exception en cas d'erreur.
                throw new RuntimeException(e.getMessage());
            }
        } while (!satifyConditions(record)); // Continue tant que les conditions ne sont pas satisfaites.

        return record;  // Retourne le record resultat
    }

    /**
     * Vérifie si un enregistrement satisfait toutes les conditions spécifiées.
     * 
     * @param record   Enregistrement à vérifier.
     * @param relation Relation associée à l'enregistrement.
     * @return true si toutes les conditions sont satisfaites, sinon false.
     */
    private boolean satifyConditions(MyRecord record) {
        // Parcourt toutes les conditions.
        for (Condition cond : conditions) {
            try {
                // Si une condition n'est pas satisfaite, retourne false.
                if (!cond.evaluate(record))
                    return false;
            } catch (Exception e) {
                // Retourne false en cas d'erreur d'évaluation.
                return false;
            }
        }
        // Retourne true si toutes les conditions sont satisfaites.
        return true;
    }

    /**
     * Réinitialise les itérateurs pour recommencer à parcourir les enregistrements depuis le début.
     */
    @Override
    public void Reset() {
        // Réinitialise l'itérateur de pages.
        pageIterator.Reset();
        // Charge la première page et initialise un itérateur pour ses enregistrements.
        PageId id = pageIterator.GetNextDataPageId();
        try {
            tupleIterator = new DataPageHoldRecordIterator(relation, bm.getPage(id), bm, id);
        } catch (Exception e) {
            // Relève une exception si une erreur se produit pendant l'initialisation.
            throw new RuntimeException("Erreur lors de la création de l'itérateur de tuple");
        }
    }

    /**
     * Ferme tous les itérateurs et libère les ressources associées.
     */
    @Override
    public void Close() {
        // Ferme l'itérateur de tuples.
        tupleIterator.Close();
        // Ferme l'itérateur de pages.
        pageIterator.Close();
    }
}