import java.nio.ByteBuffer;

/**
 * Cette classe implémente un itérateur permettant de parcourir les records dans une page de données.
 * Elle fait partie d'un système de gestion de base de données et permet d'extraire les records d'une page spécifique
 * de données en utilisant un gestionnaire de buffers (BufferManager) et en accédant directement aux données via un ByteBuffer.
 * 
 * Un DataPageHoldRecordIterator est initialisé avec une page de données spécifique et est conçu pour itérer sur 
 * les records présents dans cette page en utilisant l'offset pour accéder aux données.
 * 
 * @see IRecordIterator
 */
public class DataPageHoldRecordIterator implements IRecordIterator {
    // Variable de décalage pour accéder à l'offset des records dans la page de données
    private int offsetRecord = 0; // Initialisation pour commencer avant la fin de la page de données
    private int nbRecord; // Nombre total de records présents sur la page

    private Relation relation; // Relation associée à cet itérateur
    private BufferManager bm; // Gestionnaire de buffer pour accéder aux pages de données
    private ByteBuffer buffer; // Buffer contenant la page de données actuelle
    private PageId dataPageId; // Identifiant de la page de données courante

    /**
     * Constructeur de la classe DataPageHoldRecordIterator.
     * 
     * Ce constructeur initialise l'itérateur avec la relation (table) associée, le gestionnaire de buffer et
     * l'identifiant de la page de données. Il charge ensuite la page de données dans le buffer et extrait
     * le nombre total de records présents sur cette page à partir du header de la page (à l'offset 8).
     * 
     * @param relation La relation (table) associée à l'itérateur.
     * @param bm Le gestionnaire de buffer permettant d'accéder aux pages de données.
     * @param dataPageId L'identifiant de la page de données à parcourir.
     * @throws Exception Si une erreur survient lors du chargement de la page ou de l'accès à la mémoire.
     */
    DataPageHoldRecordIterator(Relation relation, ByteBuffer buffer, BufferManager bm, PageId dataPageId) throws Exception {
        this.relation = relation;  // Initialisation de la relation
        this.bm = bm;  // Initialisation du gestionnaire de buffer

        this.dataPageId = dataPageId;  // Initialisation de l'identifiant de la page de données

        this.buffer = buffer;

        // Extrait le nombre de records dans la page
        nbRecord = buffer.getInt(DBConfig.pagesize-8);

    }

    /**
     * Cette méthode permet d'obtenir le prochain record de la page de données.
     * Elle vérifie d'abord si plus de records sont disponibles et, dans le cas contraire, retourne null.
     * Si des records sont disponibles, elle lit la position du prochain record dans la page de données
     * et extrait ce dernier (la logique d'extraction n'est pas encore implémentée ici).
     * 
     * @return Le prochain record extrait de la page de données sous forme de MyRecord, ou null s'il n'y a plus de records.
     */
    public MyRecord GetNextRecord() {
        // Si tous les records de la page ont été parcourus
        if (nbRecord == 0)
            return null;

        MyRecord res = new MyRecord();  // Variable pour stocker le record extrait

        offsetRecord += relation.readRecordFromBuffer(res, buffer, offsetRecord);
        nbRecord--;

        return res; // La méthode retourne actuellement null; ici, vous devez extraire les données et retourner le record
    }

    /**
     * Réinitialise l'itérateur pour recommencer à partir du début de la page de données.
     * Cette méthode remet l'offset des records à zéro pour recommencer l'itération sur la page de données.
     */
    public void Reset() {
        nbRecord = buffer.getInt(DBConfig.pagesize-8);
        offsetRecord = 0;  // Réinitialisation de l'offset à 0 pour recommencer l'itération
    }

    /**
     * Cette méthode ferme l'itérateur en libérant les ressources allouées.
     * Elle libère la référence au buffer et renvoie la page de données au gestionnaire de buffer.
     * Cela permet de libérer la mémoire et de gérer les ressources de manière efficace.
     */
    public void Close() {
        nbRecord = 0;
        buffer = null;  // Libère la référence au buffer, permettant au garbage collector de gérer la mémoire
        bm.freePage(dataPageId, false);  // Libère la page de données via le gestionnaire de buffer
    }
}