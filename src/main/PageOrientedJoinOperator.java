import java.util.ArrayList;

/**
 * Classe représentant un opérateur de jointure orientée page, utilisé pour effectuer
 * une jointure entre deux relations (une externe et une interne), en parcourant les pages
 * et les tuples à l'aide d'itérateurs de pages et de tuples.
 * Cet opérateur est conçu pour supporter des conditions de jointure sur les tuples.
 */
public class PageOrientedJoinOperator implements IRecordIterator {
    BufferManager bm;

    // Iterateur de data page de la relation extérieur
    private PageDirectoryIterator outerPageIt;
    // Iterateur de data page de la relation interne
    private PageDirectoryIterator innerPageIt;

    // Iterateur de tuple de la relation extérieur
    private DataPageHoldRecordIterator outerTupleIt;
    // Iterateur de tuple de la relation interne
    private DataPageHoldRecordIterator innerTupleIt;

    private ArrayList<Condition> joinConditions;
    private MyRecord outerRecord; // Le tuple courant de la relation extérieur

    /**
     * Constructeur de l'opérateur de jointure orientée page pour produit cartésien.
     */
    public PageOrientedJoinOperator(PageDirectoryIterator outerPageIt, PageDirectoryIterator innerPageIt, ArrayList<Condition> conditions) throws Exception {
        this.outerPageIt = outerPageIt; // Initialisation des relations à joindre
        this.innerPageIt = innerPageIt; // Initialisation des relations à joindre
        joinConditions = conditions;

        // Récupère le bm
        this.bm = innerPageIt.getBm();

        Reset();
    }

    /**
     * Récupère le prochain enregistrement satisfaisant les conditions de jointure.
     * 
     * Cette méthode parcourt les tuples externes et internes pour trouver la première
     * correspondance valide selon les conditions de jointure.
     * 
     * @return Le prochain enregistrement valide, ou null si aucun enregistrement
     *         valide n'est trouvé.
     */
    @Override
    public MyRecord GetNextRecord() {
        MyRecord res = new MyRecord();

        try {
            // Boucle principale pour trouver le prochain enregistrement qui satisfait les conditions
            do {
                MyRecord innRecord = innerTupleIt.GetNextRecord();

                // Si on est au bout d'une data Page de la relation interne
                if (innRecord == null) {
                    PageId id2 = innerPageIt.GetNextDataPageId();

                    // Si on est au bout de la relation interne
                    if (id2 == null) {
                        outerRecord = outerTupleIt.GetNextRecord();

                        // Si on est au bout de la data Page externe
                        if (outerRecord == null) {
                            PageId id1 = outerPageIt.GetNextDataPageId();

                            // Si on est au bout de la relation externe
                            if (id1 == null)
                                return null;

                            outerTupleIt.Close();
                            outerTupleIt = new DataPageHoldRecordIterator(outerPageIt.getRelation(), bm.getPage(id1), bm, id1);
                            outerRecord = outerTupleIt.GetNextRecord();
                        }
                        innerPageIt.Reset();
                        id2 = innerPageIt.GetNextDataPageId();
                    }
                    innerTupleIt.Close();
                    innerTupleIt = new DataPageHoldRecordIterator(innerPageIt.getRelation(), bm.getPage(id2), bm, id2);
                    innRecord = innerTupleIt.GetNextRecord();
                }
                res.addAll(outerRecord);
                res.addAll(innRecord);

            } while(!satifyConditions(res));
        } catch (Exception e) {
            // Gestion centralisée des erreurs
            throw new RuntimeException("Erreur dans GetNextRecord : " + e.getMessage());
        }
        return res;
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
        for (Condition cond : joinConditions) {
            try {
                // Si une condition n'est pas satisfaite, retourne false.
                if (!cond.evaluate(record))
                    return false;
            } catch (Exception e) {
                System.out.println("erreur dans l'évaluation des conditions "+e.getMessage());
                return false;
            }
        }
        // Retourne true si toutes les conditions sont satisfaites.
        return true;
    }

/**
     * Réinitialise l'opérateur de jointure en réinitialisant tous les itérateurs.
     */
    @Override
    public void Reset() {
        try {
            // Reset les opérateurs de data Page
            outerPageIt.Reset();
            innerPageIt.Reset();

            // Charge les 1er data Page Id
            PageId outerId = outerPageIt.GetNextDataPageId();
            PageId innerId = innerPageIt.GetNextDataPageId();

            // Si on a une null c'est une erreur
            if (outerId == null || innerId == null)
                throw new IllegalStateException("l'une des 2 relations est vide");

            // Initialise les itérateurs de tuple
            outerTupleIt = new DataPageHoldRecordIterator(outerPageIt.getRelation(), bm.getPage(outerId), bm, outerId);
            innerTupleIt = new DataPageHoldRecordIterator(innerPageIt.getRelation(), bm.getPage(innerId), bm, innerId);

            // Initialise le 1er tuple externe
            outerRecord = outerTupleIt.GetNextRecord();

        } catch (Exception e) {
            throw new RuntimeException("Erreur lors de la réinitialisation : " + e.getMessage());
        }
    }

     /**
     * Ferme l'opérateur de jointure et libère les ressources utilisées.
     * Tous les attributs sont également mis à null.
     */
    @Override
    public void Close() {
        try {
            outerPageIt.Close();
            innerPageIt.Close();
        
            outerTupleIt.Close();
            innerTupleIt.Close();
        
            joinConditions = null;
            outerRecord = null;
            bm = null;
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors de la fermeture : " + e.getMessage());
        }
    }
}