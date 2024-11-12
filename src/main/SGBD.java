import java.util.*;
import java.util.regex.*;
import java.lang.reflect.*;

/**
 * La classe SGBD (Système de Gestion de Base de Données) simule l'exécution de requêtes SQL en décomposant
 * les sous-requêtes et en associant chaque commande SQL à une méthode spécifique via la réflexion.
 */
public class SGBD {
    private DBConfig dbc;
    private DiskManager dskM;
    private BufferManager bm;
    private DBManager dbM;

    private static ArrayList<SGBD> bd;

    /**
     * Constructeur qui initialise le SGBD avec la configuration de la base de données.
     *
     * @param dbc La configuration de la base de données.
     * @throws Exception Si une erreur se produit lors de l'initialisation du SGBD.
     */
    SGBD(DBConfig dbc) throws Exception {
        this.dbc = dbc;
        dskM = DiskManager.getInstance();
        dskM.loadState();
        bm = new BufferManager(dbc, dskM);
        dbM = new DBManager();
        dbM.loadState();
    }

    /**
     * Point d'entrée principal du programme. Il initialise la configuration et lance le SGBD.
     *
     * @param args Le chemin vers la base de données est passé en argument.
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Veuillez donner 1 argument, qui sera le chemin vers la Base de Données");
            System.exit(-1);
        }

        DBConfig.dbpath = args[0];
        DBConfig dbc = new DBConfig(null);

        if (!DBConfig.testDbpath()) {
            System.out.println("Erreur: le chemin: " + args[0] + " n'est pas un chemin valide sur votre machine");
            System.exit(-2);
        }

        SGBD sgbd = null;

        try {
            sgbd = new SGBD(dbc);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-3);
        }

        sgbd.run();
    }

    /**
     * Méthode principale qui lance la boucle de lecture des requêtes SQL de l'utilisateur.
     * Elle analyse et exécute les requêtes en appelant les méthodes associées.
     */
    private void run() {
        Scanner sc = new Scanner(System.in);
        String query;

        // Boucle principale pour recevoir les requêtes SQL de l'utilisateur
        while (true) {
            System.out.print("SQL> \n");
            query = sc.nextLine();

            // Vérifier la validité de la requête avant de la traiter
            if (isValidSQL(query))
                assocQuery(query);
        }
    }

    /**
     * Vérifie la validité d'une requête SQL (par exemple, si elle se termine par un point-virgule).
     *
     * @param query La requête SQL à vérifier.
     * @return true si la requête est valide, false sinon.
     */
    private boolean isValidSQL(String query) {
        // Supprimer les espaces avant et après la requête
        query = query.trim();

        // Vérifier si la requête se termine par un point-virgule
        if (!query.endsWith(";")) {
            System.out.println("Erreur : La requête doit se terminer par un point-virgule.");
            return false;
        }

        // Cree une liste qui contient les commandes possibles
        List<String> validCommands = Arrays.asList("CREATE", "SET", "LIST", "DROP");

        // Verifie que query est dans la liste
        if (!validCommands.contains(query)) {
            System.out.println("Erreur : Commande SQL non prise en charge : " + query);
            return false;
        }

        return true;
    }

    /**
     * Cette méthode associe une requête SQL principale à une méthode de traitement correspondante via la réflexion.
     *
     * @param query La requête SQL principale à exécuter.
     */
    private void assocQuery(String query) {
        String command = extractCommand(query);
        if (command == null) {
            System.out.println("Erreur : Commande SQL invalide.");
            return;
        }

        try {
            // Recherche dynamique de la méthode correspondante à la commande (par exemple, processINSERTCommand)
            Method method = this.getClass().getDeclaredMethod("process" + command + "Command", String.class);
            method.setAccessible(true);
            // Appel de la méthode correspondant à la commande
            method.invoke(this, query);  // Exécution de la méthode avec la requête en paramètre
        } catch (NoSuchMethodException e) {
            System.out.println("Erreur : La commande '" + command + "' n'est pas prise en charge.");
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    /**
     * Méthode pour traiter une requête UPDATE. Cette méthode est appelée via la réflexion.
     *
     * @param query La requête UPDATE à exécuter.
     */
    private void processUPDATECommand(String query) {
        System.out.println("Traitement de la requête UPDATE : " + query);
        // Logique pour exécuter une requête UPDATE
    }
}