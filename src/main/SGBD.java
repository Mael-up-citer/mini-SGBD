import java.util.*;
import java.util.function.Consumer;
import java.util.regex.*;

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
    private static final Map<String, Consumer<String>> COMMANDMAP = new HashMap<>();

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
        finally{
            bm = new BufferManager(dbc, dskM); // Initialisation du gestionnaire de buffers
            dbM = new DBManager(dbc, dskM, bm);  // Initialisation du gestionnaire de base de données
            dbM.loadState();    // Chargement de l'état des bases de données
        }
        initializeCOMMANDMAP();
    }

    /**
     * Méthode d'initialisation de la table de dispatching. 
     * Elle associe chaque commande à une méthode de traitement correspondante.
     */
    public void initializeCOMMANDMAP() {
        COMMANDMAP.put("CREATE DATABASE", this::processCREATEDATABASECommand);
        COMMANDMAP.put("DROP DATABASE", this::processDROPDATABASECommand);
        COMMANDMAP.put("DROP DATABASES", unused -> processDROPDATABASESCommand());
        COMMANDMAP.put("LIST DATABASES", unused -> processLISTDATABASESCommand());
        COMMANDMAP.put("SET DATABASE", this::processSETDATABASECommand);

        COMMANDMAP.put("CREATE TABLE", this::processCREATETABLECommand);
        COMMANDMAP.put("DROP TABLE", this::processDROPTABLECommand);
        COMMANDMAP.put("DROP TABLES", unused -> processDROPTABLESCommand());
        COMMANDMAP.put("LIST TABLES", this::processLISTTABLESCommand);

        COMMANDMAP.put("SELECT", this::processSELECTCommand);

        COMMANDMAP.put("QUIT", unused -> processQUITCommand());
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
                query = sc.nextLine();   // Récupère la commande de l'utilisateur et la nettoie

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
    public void assocQuery(String query) {
        query = query.trim().toUpperCase();    // Passe la chaîne de caractères en majuscules et enleve les espaces autour
        boolean succes = false;

        for (int i = 1; i <=3 ; i++) {
            String[] string = extractCommand(query, i);  // Extrait la commande et les arguments

            String command = string[0];             // La commande
            String settings = string[1];            // Les arguments associés à la commande

            // Recherche dans la table de dispatching pour trouver la méthode correspondante
            Consumer<String> handler = COMMANDMAP.get(command);

            // Si la commande est trouvée, l'exécuter
            if (handler != null){
                succes = true;
                handler.accept(settings);  // Appel de la méthode associée à la commande
                break;
            }
        }
        if(!succes)
            System.out.println("la commande "+query+" n'est pas supporté par le system");
    }

    /**
     * Extrait le type de commande et les arguments d'une requête SQL.
     *
     * @param input La requête SQL complète.
     * @return Un tableau contenant le type de commande et les arguments.
    */
    private String[] extractCommand(String input, int nbword) {
        // Diviser la chaîne en mots, en gérant les espaces multiples
        String[] words = input.split("\\s+");

        // Créer les deux parties
        String firstPart = String.join(" ", Arrays.copyOfRange(words, 0, nbword));
        String secondPart = String.join(" ", Arrays.copyOfRange(words, nbword, words.length));

        return new String[] { firstPart, secondPart };
    }

    // Méthodes de traitement des commandes

    /**
     * Méthode pour traiter la commande CREATE DATABASE.
     */
    private void processCREATEDATABASECommand(String param) {
        // Vérifier la validité du nom de la base de données
        if (!isValidName(param)) {
            System.out.println("Erreur : Le nom de la base de données '" + param + "' est invalide.");
            return;
        }
        try {
            dbM.CreateDatabase(param);  // Appel à la méthode pour créer la base de données
            System.out.println("La base de données '" + param + "' a été créée avec succès.");
        } catch (Exception e) {
            System.out.println("Erreur lors de la création de la base de données '" + param + "' "+e.getMessage());
        }
    }

    /**
     * Méthode pour traiter la commande SET DATABASE.
    */
    private void processSETDATABASECommand(String param) {
        try {
            dbM.SetCurrentDatabase(param);  // Définir la base de données actuelle
            System.out.println("Base de données actuelle définie sur : " + param);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Méthode pour traiter la commande LIST DATABASES.
     */
    private void processLISTDATABASESCommand() {
        dbM.ListDatabases();  // Lister les bases de données
    }

    /**
     * Méthode pour traiter la commande DROP DATABASE.
     */
    private void processDROPDATABASECommand(String param) {
        try {
            dbM.RemoveDatabase(param);  // Supprimer la base de données
            // Informe l'utilisateur de la réussite de l'opération
            System.out.println("Suppression de la base de données : "+param);
        } catch (Exception e) {
            System.out.println("Erreur dans la suppression de la base '"+param+"': "+e.getMessage());
        }
    }

    /**
     * Méthode pour traiter la commande CREATE TABLE.
    */
    private void processCREATETABLECommand(String param) {
        String name = param.split("\\(", 2)[0].trim();

        if (!isValidName(name)) {  // Vérifier la validité du nom de la table
            System.out.println("Erreur : Le nom de la table '" + name + "' est invalide.");
            return;
        }

        // Vérifiz si le nom de la table existe pas deja dans la base
        if (dbM.tableExiste(name)) {  // Vérifier la validité du nom de la table
            System.out.println("Erreur : La table '" + name + "' existe déjà.");
            return;
        }

        try{
            // Enlever le premier mot (nom de la table) et la parenthèse extérieure
            // Trouver l'index du premier espace
            int firstSpaceIndex = param.indexOf(' ');

            // Vérifier si un espace a été trouvé (ce qui signifie qu'il y a un nom de table suivi d'un espace)
            if (firstSpaceIndex != -1)
                param = param.substring(firstSpaceIndex + 1).trim();  // Enlever le nom de la table
            
            // Étape 2: Enlever les parenthèses extérieures
            if (param.startsWith("(") && param.endsWith(")"))
                param = param.substring(1, param.length() - 1).trim();  // Supprime les parenthèses extérieures
            
            // Crée une liste d'attribut qu'on initialise avec parseRelation qui a pour but de convertir une chaine de caractère en Pair<attribut, longueur>
            ArrayList<Pair<String, Data>> attribut = parseRelation(param);
            // Instancie la relation avec les variables précédente
            Relation relation = new Relation(name, attribut, dskM.AllocPage(), dskM, bm);

            dbM.AddTableToCurrentDatabase(relation);  // Ajouter la table à la base de données actuelle

        } catch(Exception e){
            System.out.println("erreur dans la création de la table "+param);
            e.printStackTrace();
        }
    }

    /**
     * Assure une conversion d'une chaine de forme: (nom:type,nom:type(size)) en une liste d'attribut d'une relation.
     * @param arg la chaine à convertir.
     * @return la liste des attribut extrait de arg.
     * @throws Exception Si une erreur survient lors de la convertion.
     * 
     */
    private ArrayList<Pair<String, Data>> parseRelation(String arg) throws Exception {        
        // Initialise la liste d'attributs
        ArrayList<Pair<String, Data>> attributs = new ArrayList<>();
        
        // Nettoie les espaces superflus
        arg = arg.trim();
        
        // Sépare la chaîne principale par les virgules
        String[] paires = arg.split("\\s*,\\s*");
    
        // Parcourt chaque sous-chaîne (attribut) et traite les informations
        for (String paire : paires) {
            paire = paire.trim(); // Enlever les espaces superflus autour de chaque attribut
            
            // Séparer en deux parties : nom et type
            String[] parts = paire.split(":");
    
            if (parts.length != 2)
                throw new Exception("Erreur de format dans la chaîne d'attribut : " + paire);
            
            String attName = parts[0].trim(); // Nom de l'attribut
            String typePart = parts[1].trim(); // Type de l'attribut
            
            // Extraire le type (avant la parenthèse si elle existe)
            DataType type = DataType.valueOf(typePart.split("\\(")[0].trim());
    
            // Si le type est CHAR ou VARCHAR, nous devons extraire la taille
            if (type == DataType.CHAR || type == DataType.VARCHAR) {
                // Chercher la taille entre les parenthèses
                int length = Integer.parseInt(typePart.split("\\(")[1].split("\\)")[0].trim());
    
                // Ajouter l'attribut à la liste avec sa taille
                attributs.add(new Pair<>(attName, new Data(type, length)));
            } 
            else {
                // Ajouter l'attribut à la liste sans taille
                attributs.add(new Pair<>(attName, new Data(type)));
            }
        }
        // Retourner la liste d'attributs
        return attributs;
    }

    /**
     * Méthode pour traiter la commande DROP TABLE.
    */
    private void processDROPTABLECommand(String param) {
        try {
            dbM.RemoveTableFromCurrentDatabase(param);  // Supprimer la table de la base de données actuelle
            //Informe l'utilisateur de la réussite de l'opération
            System.out.println("Table " + param + " supprimée de la base de données actuelle.");
        } catch (Exception e) {
            System.out.println("Erreur dans la suppression de la table '"+ param+"': "+e.getMessage());
        }
    }

    /**
     * Méthode pour traiter la commande LIST TABLES.
    */
    private void processLISTTABLESCommand(String param) {
        try {
            dbM.ListTablesInCurrentDatabase();  // Lister les tables dans la base de données actuelle
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Méthode pour traiter la commande QUIT (quitter le programme).
    */
    private void processQUITCommand(){
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

    private void processDROPDATABASESCommand(){
        try {
            dbM.RemoveDatabases();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processDROPTABLESCommand(){
        try {
            dbM.RemoveTablesFromCurrentDatabase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Méthode pour traiter la commande SELECT.
     * @param param Commande SQL sans le mot-clé "SELECT".
     */
    private void processSELECTCommand(String param) {
        // Expression régulière pour valider et extraire les parties de la commande
        String selectReg = String.join("",
            "(\\*|[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+", // '*' ou aliasRel.col1
            "(\\s*,\\s*[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+)*)", // , aliasRel.col2, ...
            "\\s+FROM\\s+", // FROM obligatoire
            "([a-zA-Z0-9_]+)\\s+([a-zA-Z0-9_]+)", // nomRelation aliasRel
            "(\\s+WHERE\\s+", // WHERE optionnel
            "([a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\s*(<|>|<=|>=|=|!=)\\s*[a-zA-Z0-9_'\"]+)", // aliasRel.col OP value
            "(\\s+AND\\s+[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\s*(<|>|<=|>=|=|!=)\\s*[a-zA-Z0-9_'\"]+)*", // AND aliasRel.col OP value
            ")?$" // WHERE est optionnel
        );

        // Compiler la regex
        Pattern pattern = Pattern.compile(selectReg, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(param);

        // Si la commande correspond à la regex, extraire les informations
        if (matcher.matches()) {
            String attributs = matcher.group(1); // Attributs sélectionnés
            String tableName = matcher.group(3);  // Nom de la table
            String alias = matcher.group(4);     // Alias de la table
            String whereConditions = matcher.group(6); // Conditions dans WHERE (optionnel)

            // Charger la relation (table) de la base de données
            Relation relation = extractRelation(tableName);
            if (relation == null) { 
                System.out.println("La table " + tableName + " n'existe pas dans la base de donnée courrante");
                return; // Quitter la méthode si la table n'existe pas
            }

            // Extraire les indices des attributs à afficher
            ArrayList<Integer> attrbToPrint = extractAttribut(attributs, relation);

            // Extraire les conditions
            ArrayList<Condition> conditions = new ArrayList<>();
            if (whereConditions != null)
                conditions = extractCondition(whereConditions);

            // Afficher les résultats extraits
            System.out.println("Attributs : " + attrbToPrint);
            System.out.println("Relation : " + relation.getRelationName());
            System.out.println("Conditions : " + conditions);

            // Exécuter la commande avec les opérateurs relationnels
            try {
                RelationScanner scanner = new RelationScanner(relation, bm, conditions);
                ProjectOperator projOp = new ProjectOperator(scanner, attrbToPrint);
                RecordPrinter rp = new RecordPrinter(projOp);

                rp.printAllRecord();
            } catch(Exception e){
                System.out.println("Erreur lors de l'exécution de la commande");
                e.printStackTrace();
            }
        }
        else
            System.out.println("Erreur de syntaxe dans la requête.");
    }

    /**
     * Méthode pour extraire une relation (table et alias) à partir du paramètre.
     */
    private Relation extractRelation(String tableName) {
        // Accéder à la base de données courante pour récupérer la table
        HashMap<String, Relation> db = dbM.getCurrentDatabase();

        // Si la base de donné courrante est pas défini
        if (db != null)
            return db.get(tableName);
        // Sinon retourne null
        return null;
    }

    /**
     * Méthode pour extraire les attributs à afficher.
     * @param param Partie des attributs (ex. "aliasRel.col1, aliasRel.col2").
     * @param relation Relation contenant les attributs.
     * @return Liste des indices des attributs dans la relation.
     */
    private ArrayList<Integer> extractAttribut(String param, Relation relation) {
        ArrayList<Integer> attributesToPrint = new ArrayList<>();

         // Si à '*' on les prends tous
         if ("*".equals(param.trim())) {
            // Si l'utilisateur veut tous les attributs
            for (int i = 0; i < relation.getNbAttribut(); i++)
                attributesToPrint.add(i);  // Ajouter tous les indices des attributs
        }
        else{
            // Séparer les attributs par des virgules
            String[] attrb = param.split(",");
            HashMap<String, Integer> fromNameToIndex = new HashMap<>();

            // Associer chaque attribut de la relation à son index
            for (int i = 0; i < relation.getNbAttribut(); i++)
                fromNameToIndex.put(relation.getNameAttribut(i), i);

            // Valider et récupérer les indices
            for (String attr : attrb) {
                attr = attr.trim();
                String[] parts = attr.split("\\."); // Séparer l'alias et le nom de colonne

                // Récupérer l'index de la colonne
                if (fromNameToIndex.containsKey(parts[1]))
                    attributesToPrint.add(fromNameToIndex.get(parts[1]));

                else 
                    throw new IllegalArgumentException("Colonne inexistante : " + parts[1]);
            }
        }
        return attributesToPrint;
    }

    /**
     * Méthode pour extraire les conditions WHERE.
     * @param param Partie WHERE (ex. "aliasRel.col1 = 10 AND aliasRel.col2 > 20").
     * @return Liste des conditions extraites.
     */
    private ArrayList<Condition> extractCondition(String param) {
        String[] conditions = param.split("\\s+AND\\s+");
        ArrayList<Condition> res = new ArrayList<>();
        String regex = "([a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+)(<|>|<=|>=|=|!=)([a-zA-Z0-9_'\"]+)";
        Pattern pattern = Pattern.compile(regex);

        for (String cond : conditions) {
            Matcher matcher = pattern.matcher(cond);
            if (matcher.matches()) {
                String terme1 = matcher.group(1).trim();
                String operateur = matcher.group(2).trim();
                String terme2 = matcher.group(3).trim();
                res.add(new Condition(terme1, operateur, terme2));
            } else {
                throw new IllegalArgumentException("Condition invalide : " + cond);
            }
        }

        return res;
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
    public static boolean isValidName(String name) {
        // Vérifie que le nom contienne quelque chose
        if (name == null || name.isEmpty())
            return false;

        // Le nom ne doit pas commencer par un chiffre
        if (Character.isDigit(name.charAt(0)))
            return false;

        // Vérifie que le nom ne contient que des lettres, des chiffres et des underscores
        if (!name.matches("^[a-zA-Z0-9_]+$")) {
            return false;
        }

        // Vérifie que le nom ne dépasse pas 64 caractères (exemple de limite de MySQL)
        if (name.length() > 64) {
            return false;
        }

        // Liste des mots réservés sous forme d'ArrayList
        ArrayList<String> reservedWords = new ArrayList<>(Arrays.asList(
            "SELECT", "INSERT", "DELETE", "DROP", "UPDATE", "CREATE", 
            "ALTER", "DATABASE", "TABLE"
        ));

        return !reservedWords.contains(name);  // Vérifie que le nom n'est pas un mot réservé
    }


    /**
     * Valide une valeur avant de l'ajouter au tuple.
     * Effectue les vérifications suivantes :
     * - Si l'index est valide en fonction du nombre d'attributs de la relation
     * - Si le type spécifié correspond au type attendu pour cet attribut
     * - Si l'objet de la valeur correspond au type de donnée attendu (ex : chaîne pour CHAR)
     * - Si la taille des chaînes respecte la limite spécifiée pour CHAR et VARCHAR
     *
     * @param value La valeur à valider.
     * @param type Le type de la valeur spécifié dans le tuple.
     * @param index L'index de l'attribut dans la relation, pour déterminer le type attendu.
     * @return La valeur validée, éventuellement modifiée (ex : chaîne complétée pour CHAR).
     * @throws IllegalArgumentException Si la validation échoue.
     */ /*
    private Object validateTupleAttribute(Object value, DataType type, int index, Relation relation) {
        // Vérifie que l'index ne dépasse pas le nombre d'attributs définis dans la relation.
        if (index >= relation.getAttribut().size())
            throw new IllegalArgumentException("Le nombre de valeurs dépasse le nombre d'attributs dans la relation.");

        // Récupère le type attendu pour l'attribut à l'index donné.
        DataType expectedType = relation.getType(index);

        // Vérifie si le type de donnée spécifié correspond au type attendu.
        if (expectedType != type)
            throw new IllegalArgumentException("Le type de la valeur ne correspond pas à celui de l'attribut.");

        // Récupère la longueur maximale autorisée pour les chaînes si applicable.
        int maxLength = relation.getLength(index);

        // Effectue une vérification basée sur le type de donnée pour s'assurer que
        // l'objet valeur est du type attendu et respecte les contraintes spécifiques.
        switch (type) {
            case CHAR:
            case VARCHAR:
                // Vérifie que la valeur est une instance de String.
                if (!(value instanceof String))
                    throw new IllegalArgumentException("La valeur doit être une chaîne pour le type CHAR ou VARCHAR.");

                // Convertit la valeur en chaîne pour vérifier sa longueur.
                String strValue = (String) value;

                // Vérifie que la longueur de la chaîne ne dépasse pas la longueur maximale.
                if (strValue.length() > maxLength)
                    throw new IllegalArgumentException("La chaîne dépasse la taille maximale autorisée pour l'attribut.");

                break;

            case INT:
                // Vérifie que la valeur est une instance d'Integer.
                if (!(value instanceof Integer))
                    throw new IllegalArgumentException("La valeur doit être un entier pour le type INT.");

                break;

            case REAL:
                // Vérifie que la valeur est une instance de Float ou Double.
                if (!(value instanceof Float) && !(value instanceof Double))
                    throw new IllegalArgumentException("La valeur doit être un nombre réel pour le type REAL.");

                break;

            case DATE:
                // Vérifie que la valeur est une instance de Date.
                if (!(value instanceof Date))
                    throw new IllegalArgumentException("La valeur doit être une date pour le type DATE.");

                break;

            default:
                // Lève une exception si le type de donnée n'est pas supporté.
                throw new IllegalArgumentException("Type de donnée non supporté.");
        }
        // Retourne la valeur validée ou modifiée si nécessaire (ex : chaîne complétée pour CHAR).
        return value;
    }
    */

    public DBManager getDBManager() {
        return dbM;
    }

}