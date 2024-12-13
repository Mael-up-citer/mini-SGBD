import java.nio.Buffer;
import java.util.ArrayList;

import javax.management.RuntimeErrorException;

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
    private MyRecord outerRecord; // Le tuple courant de la relation interieur

    /**
     * Constructeur de l'opérateur de jointure orientée page pour produit cartésien.
     */
    public PageOrientedJoinOperator(PageDirectoryIterator outerPageIt, PageDirectoryIterator innerPageIt, ArrayList<Condition> conditions) throws Exception {
        this.outerPageIt = outerPageIt; // Initialisation des relations à joindre
        this.innerPageIt = innerPageIt; // Initialisation des relations à joindre
        joinConditions = conditions;

        // Récupère le bm
        this.bm = innerPageIt.getBm();

        PageId outerId = outerPageIt.GetNextDataPageId();
        PageId innerId = innerPageIt.GetNextDataPageId();

        if (outerId == null || innerId == null)
            throw new IllegalStateException("l'une des 2 relations est vide");

        // Initialise les itérateurs de tuple externe
        outerTupleIt = new DataPageHoldRecordIterator(outerPageIt.getRelation(), bm.getPage(outerId), bm, outerId);
        innerTupleIt = new DataPageHoldRecordIterator(innerPageIt.getRelation(), bm.getPage(innerId), bm, innerId);
    }

    /*
     * Récupère le prochain enregistrement qui satisfait les conditions.
     * 
     * @return Le prochain enregistrement valide ou null s'il n'y en a plus.
     */
    @Override
    public MyRecord GetNextRecord() {
        MyRecord record = null;
/*
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
                // Lève une exception en cas d'erreur.
                throw new RuntimeException(e.getMessage());
            }
        } while (!satifyConditions(record)); // Continue tant que les conditions ne sont pas satisfaites.
*/
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
        for (Condition cond : joinConditions) {
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
     * Réinitialise l'opérateur de jointure en réinitialisant tous les itérateurs.
     */
    @Override
    public void Reset() {
/*
        try {
            operateurs.getFirst().Reset();
            operateurs.getSecond().Reset();
            outerRecord = operateurs.getFirst().GetNextRecord();
        } catch (Exception e) {
            System.out.println("Erreur lors de la réinitialisation : " + e.getMessage());
        }
*/
    }

    /**
     * Ferme l'opérateur de jointure et libère les ressources utilisées.
     * Tous les attributs sont également mis à null.
     */
    @Override
    public void Close() {
/*
        operateurs.setFirst(null);; // Les deux relations à joindre représenté par lurs select
        operateurs.setSecond(null);; // Les deux relations à joindre représenté par lurs select
        joinConditions = null;
        outerRecord = null;
*/
    }
}