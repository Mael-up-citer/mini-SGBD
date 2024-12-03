import java.util.ArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;

/**
 * Cette classe gère l'allocation, la lecture, l'écriture et la désallocation des pages dans un système de gestion de base de données.
 * Elle garantit que les pages sont allouées et libérées correctement en fonction de l'espace disponible sur le disque.
 */
public class DiskManager {

    // Instance unique de la classe
    private static DiskManager instance;
    // Indicateur pour garantir l'unicité de l'instance
    private static boolean instanceable = true;

    public ArrayList<PageId> freePage = new ArrayList<PageId>(); // Liste des pages libres
    private long GlobalStorage = 0; // Espace global utilisé par les pages

    /**
     * Constructeur privé pour empêcher l'instanciation externe.
     * Initialise l'instance unique du DiskManager.
     * 
     * @throws Exception Si une instance existe déjà.
     */
    private DiskManager() throws Exception {
        if (!instanceable) {
            throw new Exception("Une instance existe déjà");
        }
        instanceable = false; // Empêche l'instanciation future
    }

    /**
     * Méthode pour obtenir l'unique instance du DiskManager.
     * 
     * @return L'instance unique de DiskManager.
     * @throws Exception Si l'instance ne peut pas être créée.
     */
    public static DiskManager getInstance() throws Exception {
        if (instance == null) {
            instance = new DiskManager();
        }
        return instance;
    }

    public void RAZ() {
        GlobalStorage = 0;
    }

    /**
     * Alloue une nouvelle page dans le système.
     * Si des pages sont libres, elles sont réutilisées. Sinon, une nouvelle page est ajoutée à un fichier.
     * 
     * @return L'ID de la page allouée.
     * @throws Exception Si une erreur survient lors de l'allocation.
     */
    public PageId AllocPage() throws Exception {
        PageId id = new PageId();

        // Si des pages sont disponibles dans freePage, on les récupère
        if (freePage.size() != 0) {
            id = freePage.remove(freePage.size() - 1); // Retirer la dernière page libre
        }
        else {
            // Calculer l'index du fichier en fonction de l'espace global alloué
            id.FileIdx = (int) (GlobalStorage / DBConfig.dm_maxfilesize);
            ByteBuffer buffer = ByteBuffer.allocate(DBConfig.pagesize); // Créer un buffer pour la nouvelle page

            // Calculer l'index de la page en fonction de l'espace global
            id.PageIdx = (int) (GlobalStorage / DBConfig.pagesize) - (id.FileIdx * (DBConfig.dm_maxfilesize / DBConfig.pagesize));

            // Écrire la page dans le fichier
            WritePage(id, buffer); // L'exception sera propagée à l'appelant
        }
        GlobalStorage += DBConfig.pagesize; // Mettre à jour l'espace global
        return id;
    }

    /**
     * Lit le contenu d'une page à partir du disque et le place dans le buffer spécifié.
     * 
     * @param id L'ID de la page à lire.
     * @param buffer Le buffer dans lequel les données de la page seront lues.
     * @throws Exception Si une erreur survient lors de la lecture de la page.
     */
    public void ReadPage(PageId id, ByteBuffer buffer) throws Exception {
        String cheminFichier = DBConfig.dbpath + "BinData/F" + id.FileIdx + ".rsdb"; // Chemin du fichier contenant la page

        try (RandomAccessFile raf = new RandomAccessFile(cheminFichier, "r");
             FileChannel channel = raf.getChannel()) {

            // Positionner le curseur à la position de la page dans le fichier
            channel.position(id.PageIdx * DBConfig.pagesize);

            buffer.clear(); // Nettoyer le buffer avant de lire
            int bytesRead = channel.read(buffer); // Lire dans le buffer

            if (bytesRead == -1) {
                throw new Exception("Aucune donnée lue. Vérifiez si la page existe.");
            }
            else if (bytesRead < DBConfig.pagesize) {
                throw new Exception("Lecture partielle : " + bytesRead + " octets lus.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Écrit le contenu du buffer dans la page spécifiée.
     * 
     * @param id L'ID de la page à écrire.
     * @param buffer Le buffer contenant les données à écrire.
     * @throws Exception Si une erreur survient lors de l'écriture de la page.
     */
    public void WritePage(PageId id, ByteBuffer buffer) throws Exception {
        String cheminFichier = DBConfig.dbpath + "BinData/F" + id.FileIdx + ".rsdb"; // Chemin du fichier contenant la page

        try (RandomAccessFile raf = new RandomAccessFile(cheminFichier, "rw");
             FileChannel channel = raf.getChannel()) {

            // Positionner le curseur à la position de la page dans le fichier
            channel.position(id.PageIdx * DBConfig.pagesize);
            buffer.clear(); // Nettoyer le buffer avant d'écrire
            int bytesWritten = channel.write(buffer); // Écrire le buffer dans le fichier

            // Vérifier si l'écriture a été complète
            if (bytesWritten != DBConfig.pagesize)
                throw new Exception("Erreur d'écriture : " + bytesWritten + " octets écrits au lieu de " + DBConfig.pagesize);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Désalloue une page en la réintégrant à la liste des pages libres.
     * 
     * @param id L'ID de la page à désallouer.
     */
    public void DeallocPage(PageId id) {
        freePage.add(id); // Ajouter l'ID de la page à la liste des pages libres
        GlobalStorage -= DBConfig.pagesize; // Mettre à jour l'espace global
    }

    /**
     * Retourne le nombre actuel de pages allouées dans le système.
     * 
     * @return Le nombre de pages allouées.
     */
    public int getCurrentCountAllocPages() {
        return (int) (GlobalStorage / DBConfig.pagesize);
    }

    /**
     * Sauvegarde l'état actuel du DiskManager dans un fichier.
     * Cela inclut la taille totale de l'espace alloué et la liste des pages libres.
     */
    public void SaveState() {
        String cheminFichier = DBConfig.dbpath + "dm.save"; // Chemin du fichier de sauvegarde

        try (RandomAccessFile raf = new RandomAccessFile(cheminFichier, "rw");
             FileChannel channel = raf.getChannel()) {

            raf.writeLong(GlobalStorage); // Sauvegarder l'espace global alloué

            // Sauvegarder la liste des pages libres
            for (PageId pageId : freePage) {
                raf.writeInt(pageId.FileIdx); // Sauvegarder FileIdx
                raf.writeInt(pageId.PageIdx); // Sauvegarder PageIdx
            }
        } catch (IOException e) {
            System.err.println("Erreur lors de la sauvegarde du statut : " + cheminFichier);
            e.printStackTrace();
        }
    }

    /**
     * Charge l'état précédemment sauvegardé du DiskManager à partir d'un fichier.
     * 
     * @throws Exception Si une erreur survient lors du chargement de l'état.
     */
    public void loadState() throws Exception {
        String cheminFichier = DBConfig.dbpath + "dm.save"; // Chemin du fichier de sauvegarde

        try (RandomAccessFile raf = new RandomAccessFile(cheminFichier, "rw")) {
            if (raf.length() == 0) {
                throw new Exception("Il n'y a pas de sauvegarde de ce Disque Manager");
            }

            GlobalStorage = raf.readLong(); // Charger l'espace global alloué

            // Charger la liste des pages libres
            while (raf.getFilePointer() < raf.length()) {
                int fileIdx = raf.readInt(); // Charger FileIdx
                int pageIdx = raf.readInt(); // Charger PageIdx
                freePage.add(new PageId(fileIdx, pageIdx)); // Ajouter la page à la liste
            }
        } catch (IOException e) {
            System.err.println("Erreur lors de la gestion du fichier : " + cheminFichier);
            e.printStackTrace();
        }
    }
}