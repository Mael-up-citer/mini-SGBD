import java.nio.ByteBuffer;

/**
 * Cette classe permet d'itérer sur les pages de données d'une relation dans une base de données
 * en parcourant les pages d'entêtes et en extrayant les identifiants des pages de données.
 */
public class PageDirectoryIterator {
    int numHeaderPage = 1;          // Compteur pour le nombre de pages d'entêtes parcourues (initialisé à 1)
    int offsetDataPage = 4;        // Décalage pour accéder aux pages de données dans les entêtes
    int nbDataPage;                 // Nombre total de pages de données
    int cptDataPage;                // compteur pages de données déjà traité
    Relation relation;              // Instance de la relation pour laquelle on parcourt les pages
    BufferManager bm;               // Instance du gestionnaire de buffer pour accéder aux pages en mémoire

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

        // Récupère l'@ de la 1er header Page
        PageId current = new PageId(
            relation.getHeaderPageId().FileIdx,
            relation.getHeaderPageId().PageIdx
        );

        try {
            // Charge la première header Page
            ByteBuffer buffer = bm.getPage(current);

            // Si nous sommes sur une autre header Page plus loin dans le chaînage
            if (numHeaderPage > 1) {
                int cpt = numHeaderPage;  // Compteur pour gérer le chaînage des pages d'entêtes

                // Avance jusqu'à la page d'entête correcte
                while (cpt > 1) {
                    // Extraction des identifiants de page à partir de la page d'entête actuelle
                    PageId tmp = new PageId(
                        buffer.getInt(DBConfig.pagesize - 8),       // Récupère le fileIdx de la header Page suivante
                        buffer.getInt(DBConfig.pagesize - 4)   // Récupère le pageIdx de la header Page suivante
                    );
                    // Si un chaînage existe
                    if (tmp.PageIdx != -1) {
                        // Libère la page courante
                        bm.freePage(current, false);
                        // Redefini current
                        current.FileIdx = tmp.FileIdx;
                        current.PageIdx = tmp.PageIdx;
                        // Charge la page suivante dans le buffer
                        buffer = bm.getPage(current);
                    }
                    cpt--;
                }
            }
            // Extrait l'identifiant de la page de données à partir de la page d'entête
            PageId res = new PageId(
                buffer.getInt(offsetDataPage),
                buffer.getInt(offsetDataPage + 4)
            );

            //System.out.println("nb hp "+numHeaderPage);
            //System.out.println("offset "+offsetDataPage+"/"+DBConfig.pagesize);
            //System.out.println("id "+res);

            offsetDataPage += 12;  // Augmente le décalage pour passer à la page de données suivante

            // Si le décalage dépasse la taille restante de la page d'entête, on passe à la page suivante
            if (offsetDataPage > (DBConfig.pagesize - (8 + 12))) {
                //System.out.println("here");
                offsetDataPage = 0;      // Réinitialisation du décalage
                numHeaderPage++;          // Passage à la page d'entête suivante
            }
            cptDataPage --; // Décrémente le compteur de page de données a parcourir
            // Libère la page d'entête courante
            bm.freePage(current, false);
            return res;  // Retourne l'identifiant de la page de données trouvée
        } catch (Exception e) {
            e.printStackTrace();
            // En cas d'exception, on libère la page d'entête courante
            bm.freePage(current, false);
        }
        return null;  // Retourne null en cas d'erreur

    }

    /**
     * Réinitialise l'itérateur pour recommencer à partir de la première page d'entête.
     */
    public void Reset() {
        numHeaderPage = 1;    // Réinitialisation du compteur de pages d'entêtes
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
        numHeaderPage = 1;
        offsetDataPage = 4;
        nbDataPage = 0; // Indiquer qu'il n'y a plus de pages à parcourir
        cptDataPage = 0;
    }
}