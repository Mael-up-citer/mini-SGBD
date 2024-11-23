import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Classe de test unitaire pour le SGBD.
 * Elle teste les fonctionnalités principales du SGBD, notamment la création,
 * la suppression de bases de données et de tables, ainsi que l'exécution des commandes SQL de base.
 */
public class TestSGBD {

    private SGBD sgbd; // Instance du SGBD à tester

    /**
     * Réinitialise le contenu capturé avant chaque test.
     * Cette méthode est appelée avant l'exécution de chaque test.
     */
    @BeforeEach
    void resetOutput() throws Exception {
        // Initialisation de la configuration de la base de données à partir d'un fichier de configuration
        DBConfig dbc = DBConfig.loadConfig("src/tests/config.txt");
        sgbd = new SGBD(dbc); // Initialisation du SGBD
    }

    /**
     * Test de la création d'une base de données valide.
     */
    @Test
    void testCreateDatabaseValid() {
        String dbName = "testDB";
        sgbd.assocQuery("CREATE DATABASE " + dbName); // Envoi de la commande CREATE DATABASE
    }

    /**
     * Test de la création d'une base de données valide.
     */
    @Test
    void testCreateDatabaseValid() {
        String dbName = "testDB";
        sgbd.assocQuery("CREATE DATABASE " + dbName); // Envoi de la commande CREATE DATABASE
    }
}