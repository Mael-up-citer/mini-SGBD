import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Classe RelationScanner implémente IRecordIterator.
 * Permet de parcourir les pages de données d'une relation et de récupérer les enregistrements (records)
 * qui respectent les conditions spécifiées.
 */
public class RelationScanner implements IRecordIterator {
    private Relation relation; // Référence vers l'objet Relation.
    private List<PageId> dataPageId; // Liste des identifiants des pages de données associées à la relation.
    private BufferManager bm; // Référence au gestionnaire de mémoire tampon (BufferManager).
    private ByteBuffer buffer; // Contient la page de données actuellement en cours de traitement.
    private int index = 0; // Indice de la page de données actuelle dans dataPageId.
    private int offset = 0; // Position de lecture dans le buffer (page de données actuelle).

    private ArrayList<Condition> conditions; // Liste des conditions à vérifier pour les enregistrements.

    /**
     * Constructeur de RelationScanner.
     * 
     * @param relation   Relation à scanner.
     * @param bm         Gestionnaire de mémoire tampon.
     * @param conditions Liste des conditions à appliquer lors du scan.
     * @throws Exception Si une erreur survient lors de l'initialisation.
     */
    RelationScanner(Relation relation, BufferManager bm, ArrayList<Condition> conditions) throws Exception {
        dataPageId = relation.getDataPages(); // Récupère toutes les pages de données associées à la relation.
        this.relation = relation;
        this.bm = bm;
        this.conditions = conditions; // Initialise les conditions.
    }

    /**
     * Lit les enregistrements (records) un par un et retourne le premier
     * qui satisfait les conditions spécifiées.
     * 
     * @return MyRecord Enregistrement satisfaisant les conditions ou null si aucun.
     */
    public MyRecord GetNextRecord() {
        MyRecord record = new MyRecord(); // Crée une nouvelle instance de MyRecord.
        int cpt = 0; // Compteur pour suivre le nombre d'enregistrements lus dans la page.
        int nbRecord = 1; // Nombre d'enregistrements dans une page (initialisé à 1 pour démarrer la lecture).

        do {
            try {
                // Si toutes les pages ont été lues, retourne null.
                if (index >= dataPageId.size())
                    return null; // Plus de pages à lire.

                // Si tous les enregistrements d'une page ont été lus.
                if (cpt > nbRecord) {
                    System.out.println("end page");
                    bm.freePage(dataPageId.get(index - 1), false); // Libère la page précédente.
                    offset = 0; // Réinitialise l'offset.
                    index++; // Passe à la page suivante.
                }

                // Si offset est à 0, une nouvelle page est chargée.
                if (offset == 0) {
                    System.out.println("start page");
                    buffer = bm.getPage(dataPageId.get(index)); // Charge la nouvelle page dans le buffer.
                    nbRecord = buffer.getInt(DBConfig.pagesize - 8); // Lit le nombre total d'enregistrements dans la page.
                }

                // Lit un enregistrement dans la page courante.
                offset += relation.readRecordFromBuffer(record, buffer, offset); // Lit un record à partir du buffer.
                cpt++; // Incrémente le compteur d'enregistrements lus dans la page.
            } catch (Exception e) {
                return null; // Retourne null en cas d'erreur.
            }
        } while (!satifyConditions(record, relation)); // Continue jusqu'à trouver un record valide ou la fin.

        return record; // Retourne le record valide.
    }

    /**
     * Vérifie si un enregistrement satisfait toutes les conditions spécifiées.
     * 
     * @param record   Enregistrement à vérifier.
     * @param relation Relation associée à l'enregistrement.
     * @return true si toutes les conditions sont satisfaites, sinon false.
     */
    private boolean satifyConditions(MyRecord record, Relation relation) {
        boolean flag = true; // Indicateur pour détecter les violations de conditions.

        // Parcourt toutes les conditions.
        for (Condition cond : conditions) {
            try {
                // Si une condition n'est pas satisfaite, marque comme invalide.
                if (!cond.evaluate(relation, record))
                    flag = false;
            } catch (Exception e) {
                return false; // Retourne false en cas d'exception pendant l'évaluation.
            }
        }
        return flag; // Retourne true si toutes les conditions sont valides.
    }

    /**
     * Libère les ressources utilisées par RelationScanner.
     */
    public void Close() {
        dataPageId = null; // Libère la liste des identifiants de pages.
        bm = null; // Libère la référence au BufferManager.
    }

    /**
     * Réinitialise le scanner pour relire les enregistrements depuis le début.
     */
    public void Reset() {
        index = 0; // Réinitialise l'indice des pages.
        offset = 0; // Réinitialise l'offset de lecture.
    }
}