import java.util.List;
import java.util.ArrayList;

/**
 * Cette classe implémente un opérateur de jointure multi-table pour un produit cartésien
 * dynamique de relations, où le nombre de relations n'est pas connu à l'avance.
 * Elle combine les tuples de toutes les relations en générant un produit cartésien sans jointure conditionnelle.
 */
public class PageOrientedJoinOperator implements IRecordIterator {
    private List<Relation> relations;         // Liste des relations à joindre
    private ArrayList<IRecordIterator> iterators;  // Liste des itérateurs pour chaque relation
    private BufferManager bm;                 // Gestionnaire de buffer
    private ArrayList<MyRecord> records; // List qui contient 1 record issues de chaque relation


    /**
     * Constructeur de l'opérateur de jointure multi-table orientée page pour produit cartésien dynamique.
     * 
     * @param relations Liste des relations à joindre.
     * @param bm Le gestionnaire de buffer pour accéder aux pages de données.
     * @throws Exception Si une erreur survient lors de l'initialisation des itérateurs.
     */
    public PageOrientedJoinOperator(List<Relation> relations, BufferManager bm) throws Exception {
        this.relations = relations;
        this.bm = bm;
        this.iterators = new ArrayList<>();

        // Initialisation des itérateurs pour chaque relation
        for (Relation relation : relations) {
            PageDirectoryIterator pdi = new PageDirectoryIterator(relation, bm);
            //PageId pid = pdi.GetNextDataPageId()
            //iterators.add(new DataPageHoldRecordIterator(relation, bm, relation.getHeaderPageId()));
        }

        for (int i = 0; i < iterators.size(); i++) {
            MyRecord record = iterators.get(i).GetNextRecord();
            // Si on n'arrive à extraire 0 record d'une relation
            if (record == null)
                throw new Exception("impossible de faire la jointure car un des relation est vide");

            records.add(i, record);
        }
    }

    /**
     * Récupère le prochain record résultant du produit cartésien des relations.
     * Cette méthode utilise un algorithme de boucle imbriquée dynamique pour générer un produit cartésien
     * entre toutes les relations, en incrémentant les indices des itérateurs des relations.
     * 
     * @return Le tuple fusionné, ou null s'il n'y a plus de tuples à traiter.
     */
    @Override
    public MyRecord GetNextRecord() {
        // Parcours dynamique des relations pour générer le produit cartésien
        while (true) {
            MyRecord record = advance(relations.size());  // Retourne le record fusionné
        }
    }

    private MyRecord advance(int indexIterator) {
        // Demande le prochain record de la dernière page
        MyRecord record = iterators.get(indexIterator).GetNextRecord();
        records.remove(indexIterator);  // On enlève l'ancien record qui était à cette position
        records.add(indexIterator, record); // Ajoute celui qui vient d'etre donnée

        // Si on est au bout de l'itérateur
        if (record == null) {
            // Si on est au bout du dernier itérateur
            if (indexIterator == 0) {
                Close();    // Ferme les itérateurs
                return null;    // Retourne null pour marquer la fin
            }
            // Sinon reset l'iterateur
            iterators.get(indexIterator).Reset();
            advance(indexIterator-1);   // Et on avance dans l'itérateur au dessus
        }
        // Ici on sait que le record est non null si il vient du dernier itérateur
        else if (indexIterator == relations.size()) {
            return combineRecords();
        }
        return advance(indexIterator+1);
    }

    /**
     * Combine les records extraits de chaque relation pour créer un nouveau record fusionné,
     * représentant le produit cartésien de tous les tuples des relations jointes.
     * 
     * @param records Liste des records extraits de chaque relation.
     * @return Un record combiné représentant un produit cartésien.
     */
    private MyRecord combineRecords() {
        MyRecord combinedRecord = new MyRecord();

        // Ajouter les attributs de chaque record à celui combiné
        for (MyRecord record : records)
            combinedRecord.addAll(record);

        return combinedRecord;
    }

    /**
     * Réinitialise l'opérateur de jointure en réinitialisant tous les itérateurs.
     * Cette méthode est appelée lorsque les itérateurs sont épuisés ou après une nouvelle tentative de génération
     * du produit cartésien.
     */
    @Override
    public void Reset() {
        // Réinitialiser tous les itérateurs pour recommencer le produit cartésien
        for (IRecordIterator iterator : iterators)
            iterator.Reset();
    }

    /**
     * Ferme l'opérateur de jointure en libérant les ressources.
     * Cette méthode est appelée pour libérer la mémoire et les ressources utilisées par les itérateurs.
     */
    @Override
    public void Close() {
        // Fermer tous les itérateurs et libérer les ressources associées
        for (IRecordIterator iterator : iterators)
            iterator.Close();
    }
}