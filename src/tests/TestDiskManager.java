import org.junit.jupiter.api.AfterEach; // Importation de l'annotation pour exécuter du code après chaque test
import org.junit.jupiter.api.BeforeEach; // Importation de l'annotation pour exécuter du code avant chaque test
import org.junit.jupiter.api.Test; // Importation de l'annotation pour marquer une méthode comme un test
import static org.junit.jupiter.api.Assertions.*; // Importation des assertions statiques pour effectuer des tests

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class TestDiskManager{

    private DiskManager dskM; // Instance de DiskManager à tester
    private DBConfig dbc; // Instance de DBConfig pour configurer les tests

    @BeforeEach
    private void setup() throws Exception{
        // Initialiser DiskManager et charger la configuration avant chaque test
        dskM = DiskManager.getInstance();
        dbc = DBConfig.loadConfig("src/tests/config.txt");
        DBConfig.dbpath = "src/tests/db/";
    }

    @AfterEach
    private void tearDown() throws IOException{
        // Nettoyer après les tests en supprimant les fichiers générés
        Files.deleteIfExists(Paths.get(DBConfig.dbpath + "dm.save"));

        for (int i = 0; i < 10; i++) // Nettoyer tous les fichiers de test
            Files.deleteIfExists(Paths.get(DBConfig.dbpath + "BinData/F" + i + ".rsdb"));
    }

    @Test // Test le constructeur de DiskManager
    void testConstructeur() throws Exception {
        DiskManager dskM2 = DiskManager.getInstance(); // Obtenir une nouvelle instance de DiskManager

        // Vérifier que les deux instances sont identiques
        assertEquals(dskM, dskM2);
    }

    @Test // Test l'allocation de page
    void testAllocPage() throws Exception {
        int cpt = 0; // Compteur pour le nombre de pages allouées
        int nbFile = 5; // Nombre de fichiers à allouer
        int f = -1, p = 0; // Index de fichier et d'index de page
        PageId id = new PageId(); // Instance de PageId pour stocker l'identifiant de la page

        // Allouer autant de pages pour remplir 5 fichiers
        while (cpt < (nbFile * (DBConfig.dm_maxfilesize / DBConfig.pagesize))) {
            id = dskM.AllocPage(); // Allouer une page

            // Mettre à jour f et p si on change de fichier
            if ((cpt % (DBConfig.dm_maxfilesize / DBConfig.pagesize)) == 0) {
                p = 0; // Réinitialiser l'index de la page
                f++; // Passer au fichier suivant
            }

            // Vérifie que les attributs évoluent correctement
            assertEquals(f, id.FileIdx); // Vérifier que l'index de fichier est correct
            assertEquals(p, id.PageIdx); // Vérifier que l'index de page est correct

            p++; // Incrémenter l'index de page
            cpt++; // Incrémenter le compteur
        }

        // Vérifie que les nbFile fichiers sont pleins
        for (int i = 0; i < nbFile; i++) {
            String cheminFichier = DBConfig.dbpath + "BinData/F" + i + ".rsdb"; // Chemin du fichier
            try (RandomAccessFile raf = new RandomAccessFile(cheminFichier, "r");
                 FileChannel channel = raf.getChannel()) {
                // Si le fichier n'est pas plein, erreur
                assertEquals(DBConfig.dm_maxfilesize, channel.size()); // Vérifier que la taille du fichier est correcte
            } catch (IOException e) {
                e.printStackTrace(); // Afficher l'exception si elle se produit
            }
        }
    }

    @Test // Teste ReadPage et WritePage
    void testWriteAndRead() throws Exception {
        // 1. Phase d'écriture
    
        PageId id = new PageId(); // Créer un nouvel identifiant de page
        id.FileIdx = 0;
        id.PageIdx = 0;
    
        ByteBuffer buffer = ByteBuffer.allocate(DBConfig.pagesize); // Créer un buffer pour écrire des données
    
        // Remplir le buffer avec des données d'exemple
        for (int i = 0; i < buffer.capacity(); i++) {
            buffer.put((byte) ('A' + (i % 26))); // Remplir le buffer avec des lettres A-Z
        }
    
        dskM.WritePage(id, buffer); // Écrire la page dans le disque
    
        // 2. Vérifier le contenu du buffer après écriture
        buffer.flip(); // Préparer le buffer pour la lecture
    
        // Créer un buffer pour la lecture des données
        ByteBuffer buffer2 = ByteBuffer.allocate(DBConfig.pagesize); // Créer un buffer pour lire les données
        dskM.ReadPage(id, buffer2); // Lire la page depuis le disque
    
        System.out.println("Capacité de buffer2 : " + buffer2.capacity()); // Afficher la capacité de buffer2
        System.out.println("Capacité de buffer : " + buffer.capacity()); // Afficher la capacité de buffer
    
        // Préparer le buffer de lecture
        buffer2.flip(); // Préparer le buffer de lecture
    
        // Lire le contenu des buffers avec une boucle while
        while(buffer.hasRemaining() && buffer2.hasRemaining()) {
            byte a = buffer.get(); // Lire un octet du buffer d'écriture
            byte b = buffer2.get(); // Lire un octet du buffer de lecture
            // Vérifier que les octets sont identiques
            assertEquals(a, b, "Les octets lus ne correspondent pas."); // Vérifier que les octets lus sont identiques
        }
    }

    @Test // Test si la page est bien désallouée et si la réallocation marche
    void testDeallocPage() throws Exception{
        PageId id = dskM.AllocPage(); // Allouer une page
        dskM.DeallocPage(id); // Désallouer la page
        dskM.DeallocPage(id); // Tenter de désallouer à nouveau, ce qui devrait échouer
        PageId id2 = dskM.AllocPage(); // Réallouer une page
        assertEquals(id, id2); // Vérifier que la page réallouée est la même que celle désallouée
    }

    @Test // Test de GetCurrentCountAllocPages
    void testGetCurrentCountAllocPages() throws Exception {
        // Récupérer le nombre de pages allouées
        int courant = dskM.getCurrentCountAllocPages();
        int cpt = 0; // Compteur pour suivre le nombre de pages allouées
        // Tableau des pages allouées
        PageId[] id = new PageId[8];

        // On alloue x pages qu'on stocke
        for (int i = 0; i < 8; i++) {
            id[i] = dskM.AllocPage(); // Allouer une page
            cpt++; // Incrementer le compteur
        }

        // Pour les désallouer apres
        for(int i = 0; i < 3; i++){
            dskM.DeallocPage(id[i]); // Desallouer des pages
            cpt--; // Décrémenter le compteur
        }

        // Si le nombre de pages allouees vaut sa valeur de départ, c'est bon
        assertEquals(courant + cpt, dskM.getCurrentCountAllocPages()); // Vérifier le nombre actuel de pages allouées
    }

    @Test // Test de la sauvegarde et du chargement de l'état
    void testSaveStateEtLoadState() {
        // Copie des valeurs de dskM
        long GlobalStorage2 = dskM.getCurrentCountAllocPages(); // Nombre de pages allouées
        ArrayList<PageId> freePage2 = new ArrayList<PageId>(); // Liste pour stocker les pages libres

        // Fait une copie de la liste des pages libres
        for (PageId id : dskM.freePage) {
            freePage2.add(id); // Ajouter l'identifiant de la page libre à la nouvelle liste
        }

        // Sauvegarder l'état dans un fichier
        dskM.SaveState(); // Sauvegarder l'état
        // Charger l'état depuis le fichier
        try{
            dskM.loadState(); // Charger l'état
        } catch(Exception e){
            System.out.println(e); // Afficher l'exception si elle se produit
        }

        // Comparer les valeurs
        for (int i = 0; i < freePage2.size(); i++) {
            // Si différent, retourne faux
            assertEquals(freePage2.get(i), dskM.freePage.get(i)); // Vérifier que les pages libres sont identiques
        }
        // Vérifier si le nombre de pages allouées est resté le même après le chargement
        assertEquals(GlobalStorage2, dskM.getCurrentCountAllocPages()); // Comparer le nombre de pages allouées
    }
}