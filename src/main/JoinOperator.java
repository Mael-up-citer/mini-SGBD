import java.util.ArrayList;

public class JoinOperator implements IRecordIterator {
    private Pair<IRecordIterator, IRecordIterator> operateurs; // Les deux relations à joindre représenté par lurs select
    private ArrayList<Condition> joinConditions;
    private MyRecord outerRecord; // Le tuple courant de la relation interieur

    /**
     * Constructeur de l'opérateur de jointure orientée page pour produit cartésien.
     */
    public JoinOperator(Pair<IRecordIterator, IRecordIterator> operateurs, ArrayList<Condition> conditions) throws Exception {
        this.operateurs = operateurs; // Initialisation des relations à joindre
        joinConditions = conditions;

        // Initialise le tuple externe 
        outerRecord = this.operateurs.getFirst().GetNextRecord();

        if (outerRecord == null)
            throw new IllegalStateException("L'une des relations est vide, on ne peut pas faire de produit cartésien");
    }

    /**
     * Récupère le prochain tuple résultant du produit cartésien des relations.
     */
    @Override
    public MyRecord GetNextRecord() {
        MyRecord res;   // Record resultant de la fusion

        do {
            try {
                res = new MyRecord();

                // Récupère le prochain record de la relation interne
                MyRecord innerRecord = operateurs.getSecond().GetNextRecord();

                // Si il est null on est au bout de la relation interne
                if (innerRecord == null) {
                    // Récupère le record suivant dans la relation externe
                    outerRecord = operateurs.getFirst().GetNextRecord();

                    // Si il est null on est au bout de la relation externe
                    if (outerRecord == null) return null;    // Fin de l'iterateur

                    // Si la relation externe n'est pas null reset la relation interne
                    operateurs.getSecond().Reset();
                    // Récupère le prochain record de la relation interne
                    innerRecord = operateurs.getSecond().GetNextRecord();
                }
                res.addAll(outerRecord);
                res.addAll(innerRecord);

            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        } while (!satifyConditions(res)); // Continue tant que les conditions ne sont pas satisfaites.

        return res;  // Retourne le record resultat
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
        try {
            operateurs.getFirst().Reset();
            operateurs.getSecond().Reset();
            outerRecord = operateurs.getFirst().GetNextRecord();
        } catch (Exception e) {
            System.out.println("Erreur lors de la réinitialisation : " + e.getMessage());
        }
    }

    /**
     * Ferme l'opérateur de jointure et libère les ressources utilisées.
     * Tous les attributs sont également mis à null.
     */
    @Override
    public void Close() {
        operateurs.setFirst(null);; // Les deux relations à joindre représenté par lurs select
        operateurs.setSecond(null);; // Les deux relations à joindre représenté par lurs select
        joinConditions = null;
        outerRecord = null;
    }
}