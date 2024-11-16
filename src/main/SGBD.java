import java.util.*;
import java.util.function.Consumer;

/**
 * La classe SGBD représente un système de gestion de base de données. 
 * Elle permet de créer, supprimer, lister des bases de données et des tables, ainsi que de définir la base de données courante.
 * Elle gère aussi l'interaction avec l'utilisateur via des commandes SQL basiques.
 */
public class SGBD {
    private DBConfig dbc;               // Instance de la configuration de la base de données
    private DiskManager dskM;            // Instance du gestionnaire de disque
    private BufferManager bm;            // Instance du gestionnaire de buffers
    private DBManager dbM;               // Instance du gestionnaire de base de données

    // Table de dispatching pour associer les commandes à leurs méthodes
    private static final Map<String, Consumer<String>> commandMap = new HashMap<>();

    /**
     * Méthode d'initialisation de la table de dispatching. 
     * Elle associe chaque commande à une méthode de traitement correspondante.
    */
    public void initializeCommandMap() {
        commandMap.put("CREATE DATABASE", this::processCREATEDATABASECommand);
        commandMap.put("DROP DATABASE", this::processDROPDATABASECommand);
        commandMap.put("LIST DATABASES", this::processLISTDATABASESCommand);
        commandMap.put("SET DATABASE", this::processSETDATABASECommand);

        commandMap.put("CREATE TABLE", this::processCREATETABLECommand);
        commandMap.put("DRO PTABLE", this::processDROPTABLECommand);
        commandMap.put("LIST TABLES", this::processLISTTABLESCommand);

        commandMap.put("QUIT", this::processQUITCommand);
    }

    /**
     * Constructeur qui initialise le SGBD avec la configuration de la base de données.
     *
     * @param dbc La configuration de la base de données.
     * @throws Exception Si une erreur se produit lors de l'initialisation du SGBD.
     */
    SGBD(DBConfig dbc) throws Exception{
        this.dbc = dbc;
        dskM = DiskManager.getInstance();  // Initialisation du gestionnaire de disque
        try{
            dskM.loadState();   // Chargement de l'état du disque
        } catch(Exception e){
        }
        bm = new BufferManager(dbc, dskM); // Initialisation du gestionnaire de buffers
        dbM = new DBManager();             // Initialisation du gestionnaire de base de données
        dbM.loadState();                   // Chargement de l'état des bases de données

        initializeCommandMap();
    }

    /**
     * Point d'entrée principal du programme. Il initialise la configuration et lance le SGBD.
     *
     * @param args Le chemin vers la base de données est passé en argument.
     */
    public static void main(String[] args) {
        // Vérification que le chemin de la base de données est passé en argument
        if (args.length != 1) {
            System.out.println("Veuillez donner 1 argument, qui sera le chemin vers la Base de Données");
            System.exit(-1);
        }

        DBConfig dbc = DBConfig.loadConfig("config.txt");  // Initialisation de la configuration de la base de données

        DBConfig.dbpath = args[0];          // Affectation du chemin de la base de données

        // Vérification de la validité du chemin de la base de données
        if (!DBConfig.testDbpath()) {
            System.out.println("Erreur: le chemin: " + DBConfig.dbpath + " n'est pas un chemin valide sur votre machine");
            System.exit(-2);
        }

        SGBD sgbd = null;

        // Tentative d'initialisation du SGBD
        try {
            sgbd = new SGBD(dbc);
        } catch (Exception e) {
            e.printStackTrace();   // Si une erreur survient, elle est imprimée
            System.exit(-3);       // Le programme se termine avec un code d'erreur
        }

        sgbd.run();  // Lancement de la boucle principale
    }

    /**
     * Méthode principale qui lance la boucle de lecture des requêtes SQL de l'utilisateur.
     * Elle analyse et exécute les requêtes en appelant les méthodes associées.
     */
    private void run() {
        Scanner sc = new Scanner(System.in);  // Scanner pour interagir avec l'utilisateur
        String query;                         // Commande reçue de l'utilisateur

        try {
            // Boucle principale pour recevoir les requêtes SQL de l'utilisateur
            do {
                System.out.print("SQL> ");    // Affichage de l'invite de commande
                query = sc.nextLine().trim();   // Récupère la commande de l'utilisateur et la nettoie

                // Vérifier la validité de la requête avant de la traiter
                if (isValidSQL(query))
                    assocQuery(query);      // Si valide, associer la commande à sa méthode correspondante

            } while (true);

        } catch (NoSuchElementException | IllegalStateException e) {
            // En cas d'erreur de saisie (ex : entrée vide), afficher un message d'erreur
            System.out.println("Erreur dans la saisie, veuillez réessayer.");
        } finally {
            sc.close();  // Ferme le scanner
        }
    }

    /**
     * Cette méthode associe une requête SQL à une méthode de traitement correspondante via la table de dispatching.
     *
     * @param query La requête SQL à exécuter.
     */
    private void assocQuery(String query) {
        query = query.trim().toUpperCase();    // Passe la chaîne de caractères en majuscules
        String[] string = extractCommand(query);  // Extrait la commande et les arguments
        String command = string[0];             // La commande (ex : "CREATEDATABASE")
        String settings = string[1];            // Les arguments associés à la commande (ex : nom de la base)

        // Recherche dans la table de dispatching pour trouver la méthode correspondante
        Consumer<String> handler = commandMap.get(command);

        // Si la commande est trouvée, l'exécuter ; sinon, afficher une erreur
        if (handler != null)
            handler.accept(settings);  // Appel de la méthode associée à la commande
        else
            System.out.println("Erreur : La commande '" + command + "' n'est pas prise en charge.");
    }

    /**
     * Extrait le type de commande et les arguments d'une requête SQL.
     *
     * @param s La requête SQL complète.
     * @return Un tableau contenant le type de commande et les arguments.
    */
    private String[] extractCommand(String s) {
        // Diviser la chaîne par le premier espace rencontré pour séparer la commande et ses arguments
        String[] parts = s.split("\\s+");

        switch (parts.length) {
            case 3:
                return new String[] { parts[0]+" "+parts[1], parts[2] };
        
            case 2:
                return new String[] { parts[0]+" "+parts[1], "" };  // Retourner la commande et les arguments

            case 1:
                return new String[] { parts[0], "" };  // Retourner la commande et les arguments

            default:
                return null;
        }
    }

    // Méthodes de traitement des commandes

    /**
     * Méthode pour traiter la commande CREATE DATABASE.
    */
    private void processCREATEDATABASECommand(String param) {
        if (!isValidName(param)) {  // Vérifier la validité du nom de la base de données
            System.out.println("Erreur : Le nom de la base de données '" + param + "' est invalide.");
            return;
        }
        try {
            dbM.CreateDatabase(param);  // Appel à la méthode pour créer la base de données
            System.out.println("La base de données '" + param + "' a été créée avec succès.");
        } catch (Exception e) {
            System.out.println("Erreur lors de la création de la base de données '" + param + "'.");
            e.printStackTrace();  // Si une erreur se produit, elle est imprimée
        }
    }

    /**
     * Méthode pour traiter la commande SET DATABASE.
    */
    private void processSETDATABASECommand(String param) {
        try {
            dbM.SetCurrentDatabase(param);  // Définir la base de données actuelle
        } catch (Exception e) {
            System.out.println("Verifier que la base " + param + " existe dans le systeme");
            e.printStackTrace();  // Si une erreur se produit, elle est imprimée
        }
    }

    /**
     * Méthode pour traiter la commande LIST DATABASES.
    */
    private void processLISTDATABASESCommand(String param) {
        dbM.ListDatabases();  // Lister les bases de données
    }

    /**
     * Méthode pour traiter la commande DROP DATABASE.
     */
    private void processDROPDATABASECommand(String param) {
        try {
            dbM.RemoveDatabase(param);  // Supprimer la base de données
        } catch (Exception e) {
            System.out.println("Erreur dans la suppression de la base"+ param);
        }
    }

    /**
     * Méthode pour traiter la commande CREATE TABLE.
    */
    private void processCREATETABLECommand(String param) {
        if (!isValidName(param)) {  // Vérifier la validité du nom de la table
            System.out.println("Erreur : Le nom de la table '" + param + "' est invalide.");
            return;
        }
        dbM.AddTableToCurrentDatabase(param);  // Ajouter la table à la base de données actuelle
    }

    /**
     * Méthode pour traiter la commande DROP TABLE.
    */
    private void processDROPTABLECommand(String param) {
        try {
            dbM.RemoveTableFromCurrentDatabase(param);  // Supprimer la table de la base de données actuelle
        } catch (Exception e) {
            System.out.println("Erreur dans la suppression de la table"+ param);
        }
    }

    /**
     * Méthode pour traiter la commande LIST TABLES.
    */
    private void processLISTTABLESCommand(String param) {
        dbM.ListTablesInCurrentDatabase();  // Lister les tables dans la base de données actuelle
    }

    /**
     * Méthode pour traiter la commande QUIT (quitter le programme).
    */
    private void processQUITCommand(String param){
        try {
            dbM.saveState();    // Sauvegarder l'état de la base de données
            bm.flushBuffers();  // Vider les buffers
            dbc.pushConfig("config.txt");    // Sauvegarder la configuration
            dskM.SaveState();   // Sauvegarder l'état du disque

            System.out.println("Aurevoir :)");
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Vérifie la validité d'une requête SQL
     *
     * @param query La requête SQL à vérifier.
     * @return true si la requête est valide, false sinon.
     */
    private boolean isValidSQL(String query) {
        if (query == null || query.isEmpty()) {
            System.out.println("Erreur : La requête ne doit pas être null.");
            return false;
        }

        return true;
    }

    /**
     * Vérifie si un nom respecte les règles de nommage.
     * @param name Le nom à tester.
     * @return true si le nom est valide, sinon false.
     */
    private boolean isValidName(String name) {
        // Le nom ne doit pas commencer par un chiffre
        if (Character.isDigit(name.charAt(0)))
            return false;

        // Vérifie que le nom ne contient que des lettres, des chiffres et des underscores
        if (!name.matches("^[a-zA-Z0-9_]+$"))
            return false;

        // Vérifie que le nom ne dépasse pas 64 caractères (exemple de limite de MySQL)
        if (name.length() > 64)
            return false;

        // Liste des mots réservés sous forme d'ArrayList
        ArrayList<String> reservedWords = new ArrayList<>(Arrays.asList(
            "SELECT", "INSERT", "DELETE", "DROP", "UPDATE", "CREATE", 
            "ALTER", "DATABASE", "TABLE"
        ));

        return !reservedWords.contains(name);  // Vérifie que le nom n'est pas un mot réservé
    }
}