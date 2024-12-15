import java.nio.ByteBuffer;

/**
 * Cette classe permet d'itérer sur les pages de données d'une relation dans une base de données
 * en parcourant les pages d'entêtes et en extrayant les identifiants des pages de données.
 */
public class PageDirectoryIterator {
    private Relation relation;              // Instance de la relation pour laquelle on parcourt les pages
    private BufferManager bm;               // Instance du gestionnaire de buffer pour accéder aux pages en mémoire

    private PageId currentPageId = new PageId();          // PageId de la header page courante
    private int offsetDataPage = 4;        // Décalage pour accéder aux pages de données dans les entêtes
    private int nbDataPage;                 // Nombre total de pages de données
    private int cptDataPage;                // compteur pages de données déjà traité


    /**
     * Constructeur de la classe PageDirectoryIterator.
     * Initialisation de l'itérateur en fonction de la relation et du gestionnaire de buffer.
     * @param rela La relation associée
     * @param bm Le gestionnaire de buffer
     * @throws Exception Si un problème survient lors de la récupération des pages
     */
    PageDirectoryIterator(Relation rela, BufferManager bm) throws Exception {
        relation = rela;             // Assignation de la relation
        this.bm = bm;                // Assignation du gestionnaire de buffer

        PageId tmp = relation.getHeaderPageId();    // Récupère l'@ de la première hearder Page
        currentPageId.FileIdx = tmp.FileIdx;
        currentPageId.PageIdx = tmp.PageIdx;

        nbDataPage = bm.getPage(relation.getHeaderPageId()).getInt(0);  // Récupération du nombre de pages de données
        bm.freePage(relation.getHeaderPageId(), false);

        cptDataPage = nbDataPage;
    }

    /**
     * Cette méthode permet de récupérer l'identifiant de la page de données suivante.
     * @return L'identifiant de la page de données suivante ou null si la fin des pages est atteinte.
     */
    PageId GetNextDataPageId() {
        // Si on a parcouru toutes les pages de données
        if (cptDataPage == 0)
            return null;

        try {
            // Charge la header Page courrante
            ByteBuffer buffer = bm.getPage(currentPageId);

            // Récupère l'@ de la prochaine data Page
            PageId res = new PageId(
                buffer.getInt(offsetDataPage),
                buffer.getInt(offsetDataPage+4)
            );

            cptDataPage --; // Décrémente le compteur de page de données à parcourir
            offsetDataPage += 12;  // Augmente le décalage pour passer à la page de données suivante

            // Libère la page d'entête courante
            bm.freePage(currentPageId, false);

            // Si le décalage dépasse la taille restante de la page d'entête, on passe à la page suivante
            if (offsetDataPage > (DBConfig.pagesize - (8 + 12))) {
                offsetDataPage = 0;      // Réinitialisation du décalage
                currentPageId.FileIdx = buffer.getInt(DBConfig.pagesize-8); // Passage à la page d'entête suivante
                currentPageId.PageIdx = buffer.getInt(DBConfig.pagesize-4);  // Passage à la page d'entête suivante
            }
            return res;  // Retourne l'identifiant de la page de données trouvée

        } catch (Exception e) {
            e.printStackTrace();
            // En cas d'exception, on libère la page d'entête courante
            bm.freePage(currentPageId, false);
        }
        return null;  // Retourne null en cas d'erreur
    }

    /**
     * Réinitialise l'itérateur pour recommencer à partir de la première page d'entête.
     */
    public void Reset() {
        PageId tmp = relation.getHeaderPageId();
        currentPageId.FileIdx = tmp.FileIdx;    // Réinitialisation du compteur de pages d'entêtes
        currentPageId.PageIdx = tmp.PageIdx;    // Réinitialisation du compteur de pages d'entêtes

        offsetDataPage = 4;  // Réinitialisation du décalage pour les pages de données
        cptDataPage = nbDataPage;
    }

    /**
     * Ferme l'itérateur en libérant les ressources et en réinitialisant son état.
     */
    public void Close() {
        // Libération explicite des références pour permettre le nettoyage mémoire
        relation = null;
        bm = null;

        // Réinitialisation des variables internes
        currentPageId = null;
        offsetDataPage = 4;
        nbDataPage = 0; // Indiquer qu'il n'y a plus de pages à parcourir
        cptDataPage = 0;
    }

    public BufferManager getBm() {
        return bm;
    }

    public Relation getRelation() {
        return relation;
    }
}