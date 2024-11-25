/**
 * L'interface IRecordIterator fournit des méthodes pour parcourir un ensemble de tuples (enregistrements).
 * Elle permet d'accéder aux enregistrements un par un, de réinitialiser l'itérateur et de le fermer.
 */
public interface IRecordIterator {

    /**
     * Retourne le record courant et avance le curseur de l'itérateur d'un cran.
     * La méthode retourne null lorsqu'il ne reste plus de record à parcourir.
     *
     * @return le record courant ou null s'il n'y a plus de record.
     */
    MyRecord GetNextRecord();

    /**
     * Ferme l'itérateur, signalant qu'il n'est plus utilisé.
     * Cette méthode libère les ressources associées à l'itérateur, le cas échéant.
     */
    void Close();

    /**
     * Réinitialise le curseur au début de l'ensemble des records à parcourir.
     * Après appel, l'itérateur est prêt à recommencer la lecture depuis le premier record.
     */
    void Reset();
}