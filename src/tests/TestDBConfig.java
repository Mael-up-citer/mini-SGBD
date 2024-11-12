/*
 * @Before // Annotation indiquant que cette methode sera executee avant chaque test
 * @After // Annotation indiquant que cette methode sera executee apres chaque test
 * @Test // Annotation pour la methode de test
 */



import org.junit.After;    // Importation de l'annotation pour exécuter une méthode après chaque test
import org.junit.Before;   // Importation de l'annotation pour initialiser des méthodes avant chaque test
import org.junit.Test;     // Importation de l'annotation pour définir une méthode de test

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*; // Importation des méthodes d'assertion pour les tests

public class TestDBConfig{

    private static final String TEST_CONFIG_FILE = "src/tests/config.txt"; // Constante pour le fichier de configuration de test
    private static final String OUTPUT_CONFIG_FILE = "src/tests/outputConfig.txt"; // Constante pour le fichier de configuration de sortie

    // Constantes pour les valeurs de configuration
    private static final String DB_PATH = "src/tests/db/"; // Chemin vers la base de données
    private static final String PAGE_SIZE = "4096"; // Taille de la page en octets
    private static final String DM_MAX_FILE_SIZE = "10485760"; // Taille maximale du fichier en octets
    private static final String BM_BUFFER_COUNT = "10"; // Nombre de buffers gérés par le BufferManager
    private static final String BM_POLICY = "LRU"; // Politique de remplacement du BufferManager

    private DBConfig dbConfig; // Variable d'instance pour stocker l'objet DBConfig

    @Before
    public void setUp(){
        // Initialisation des valeurs de configuration dans un HashMap
        Map<String, String> configValues = new HashMap<>();
        configValues.put("dbpath", DB_PATH); // Chemin vers la base de données
        configValues.put("pagesize", PAGE_SIZE); // Taille de la page en octets
        configValues.put("dm_maxfilesize", DM_MAX_FILE_SIZE); // Taille maximale du fichier en octets
        configValues.put("bm_buffercount", BM_BUFFER_COUNT); // Nombre de buffers gérés par le BufferManager
        configValues.put("bm_policy", BM_POLICY); // Politique de remplacement du BufferManager
        dbConfig = new DBConfig(configValues); // Création d'une instance de DBConfig avec les valeurs de configuration
    }

    @After
    public void tearDown(){
        // Nettoyer les fichiers de test apres chaque méthode de test
        new File(OUTPUT_CONFIG_FILE).delete(); // Suppression du fichier de sortie
    }

    @Test
    public void testLoadConfig(){
        try (FileWriter writer = new FileWriter(TEST_CONFIG_FILE)){ // Création d'un FileWriter pour écrire dans le fichier
            // Écriture des paires clé-valeur dans le fichier
            writer.write("dbpath = " + DB_PATH + "\n");
            writer.write("pagesize = " + PAGE_SIZE + "\n");
            writer.write("dm_maxfilesize = " + DM_MAX_FILE_SIZE + "\n");
            writer.write("bm_buffercount = " + BM_BUFFER_COUNT + "\n");
            writer.write("bm_policy = " + BM_POLICY + "\n");
        } catch (IOException e) { // Gestion des exceptions en cas d'erreur d'écriture
            fail("Échec de l'écriture du fichier de configuration : " + e.getMessage()); // Échoue si une exception se produit
        }

        // Charger la configuration à partir du fichier
        DBConfig loadedConfig = DBConfig.loadConfig(TEST_CONFIG_FILE); // Appel à la méthode loadConfig
        assertNotNull(loadedConfig); // Vérifie que la configuration chargée n'est pas nulle
        // Assertions pour vérifier que les valeurs sont correctement assignées
        assertEquals(DB_PATH, loadedConfig.dbpath); // Vérifie le chemin de la base de données
        assertEquals(Integer.parseInt(PAGE_SIZE), loadedConfig.pagesize); // Vérifie la taille de la page
        assertEquals(Long.parseLong(DM_MAX_FILE_SIZE), loadedConfig.dm_maxfilesize); // Vérifie la taille maximale du fichier
        assertEquals(Integer.parseInt(BM_BUFFER_COUNT), loadedConfig.bm_buffercount); // Vérifie le nombre de buffers
        assertEquals(BM_POLICY, loadedConfig.bm_policy); // Vérifie la politique de remplacement
    }

    @Test
    public void testAssignValue() throws Exception{
        // Vérifie que les valeurs sont correctement assignées
        assertEquals(DB_PATH, dbConfig.dbpath); // Vérifie le chemin de la base de données
        assertEquals(4096, dbConfig.pagesize); // Vérifie la taille de la page
        assertEquals(10485760, dbConfig.dm_maxfilesize); // Vérifie la taille maximale du fichier
        assertEquals(10, dbConfig.bm_buffercount); // Vérifie le nombre de buffers
        assertEquals(BM_POLICY, dbConfig.bm_policy); // Vérifie la politique de remplacement
    }

    @Test
    public void testDisplayConfig(){
        // Teste que la méthode displayConfig ne lance pas d'exception
        try {
            dbConfig.displayConfig(); // Appel à la méthode displayConfig
        } catch (Exception e) { // Gestion des exceptions en cas d'erreur
            fail("displayConfig a lancé une exception : " + e.getMessage()); // Échoue si une exception se produit
        }
    }

    @Test
    public void testPushConfig(){
        dbConfig.pushConfig(OUTPUT_CONFIG_FILE); // Appel à la méthode pour écrire la configuration dans le fichier

        // Vérifie que le fichier de configuration de sortie a été créé
        assertTrue(new File(OUTPUT_CONFIG_FILE).exists()); // Vérifie si le fichier existe

        // Vérifie le contenu du fichier
        try {
            String content = new String(Files.readAllBytes(Paths.get(OUTPUT_CONFIG_FILE))); // Lecture du contenu du fichier
            // Vérification que le contenu du fichier contient les bonnes valeurs
            assertTrue(content.contains("dbpath = \"" + DB_PATH + "\"")); // Vérifie la présence du chemin de la base de données
            assertTrue(content.contains("pagesize = " + PAGE_SIZE)); // Vérifie la présence de la taille de la page
            assertTrue(content.contains("dm_maxfilesize = " + DM_MAX_FILE_SIZE)); // Vérifie la présence de la taille maximale du fichier
            assertTrue(content.contains("bm_buffercount = " + BM_BUFFER_COUNT)); // Vérifie la présence du nombre de buffers
            assertTrue(content.contains("bm_policy = \"" + BM_POLICY + "\"")); // Vérifie la présence de la politique de remplacement
        } catch (IOException e) { // Gestion des exceptions en cas d'erreur de lecture
            fail("Erreur lors de la lecture du fichier : " + e.getMessage()); // Échoue si une exception se produit
        }
    }

    @Test
    public void testDbpath(){
        // Teste le chemin de la base de données
        assertTrue(dbConfig.testDbpath()); // Vérifie que le chemin dbpath est valide

        // Tester avec un chemin invalide
        dbConfig.dbpath = "invalid/path"; // Attribue un chemin invalide
        assertFalse(dbConfig.testDbpath()); // Vérifie que le testDbpath renvoie false pour un chemin invalide
    }
}
