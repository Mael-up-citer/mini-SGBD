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

    Scanner sc;

    /**
     * Constructeur qui initialise le SGBD avec la configuration de la base de données.
     *
     * @param dbc La configuration de la base de données.
     * @throws Exception Si une erreur se produit lors de l'initialisation du SGBD.
     */
    SGBD(DBConfig dbc) {
        this.dbc = dbc;
        try{
            dskM = DiskManager.getInstance();  // Initialisation du gestionnaire de disque

            dskM.loadState();   // Chargement de l'état du disque
            bm = new BufferManager(dbc, dskM); // Initialisation du gestionnaire de buffers
            dbM = new DBManager(dbc, dskM, bm);  // Initialisation du gestionnaire de base de données
            dbM.loadState();    // Chargement de l'état des bases de données
        } catch(Exception e){
            System.out.println(e.getMessage());
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
        COMMANDMAP.put("INSERT INTO", this::processINSERTCommand);
        COMMANDMAP.put("BULKINSERT INTO", this::processBULKINSERTCommand);
        
        COMMANDMAP.put("CREATEINDEX ON", this::processCREATEINDEXCommand);
        COMMANDMAP.put("SELECTINDEX * FROM", this::processSELECTINDEXCommand);

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

        DBConfig dbc = DBConfig.loadConfig(args[0]+"config.txt");  // Initialisation de la configuration de la base de données

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
    public void run() {
        sc = new Scanner(System.in);  // Scanner pour interagir avec l'utilisateur
        String query;                         // Commande reçue de l'utilisateur

            // Boucle principale pour recevoir les requêtes SQL de l'utilisateur
            do {
                try {
                    System.out.print("SQL> ");    // Affichage de l'invite de commande
                    query = sc.nextLine();   // Récupère la commande de l'utilisateur et la nettoie

                    // Vérifier la validité de la requête avant de la traiter
                    if (!query.equals("") && !query.equals("\n") && isValidSQL(query))
                        assocQuery(query);      // Si valide, associer la commande à sa méthode correspondante

                } catch (Exception e) {
                    // En cas d'erreur de saisie (ex : entrée vide), afficher un message d'erreur
                    System.out.println("Erreur dans la saisie, veuillez réessayer.");
                }
            } while (true);
    }

    /**
     * Cette méthode associe une requête SQL à une méthode de traitement correspondante via la table de dispatching.
     *
     * @param query La requête SQL à exécuter.
     */
    public void assocQuery(String query) throws Exception{
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
    private String[] extractCommand(String input, int nbword) throws Exception{
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
            
            System.out.println(typePart);

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
            sc.close(); // Ferme le scanner
            dbM.saveState();    // Sauvegarder l'état de la base de données
            bm.flushBuffers();  // Vider les buffers
            dbc.pushConfig("config.txt");    // Sauvegarder la configuration
            dskM.SaveState();   // Sauvegarder l'état du disque

            System.out.println("Aurevoir :)");
            System.exit(0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Méthode pour traiter la commande DROPDATABASES
     */
    private void processDROPDATABASESCommand(){
        try {
            dbM.RemoveDatabases();
            System.out.println("les bases de données on toutes disparu");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Méthode pour traiter la commande DROPTABLES
     */
    private void processDROPTABLESCommand(){
        try {
            dbM.RemoveTablesFromCurrentDatabase();
            System.out.println("Suppression terminé");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    /**
     * Méthode pour traiter la commande INSERT
     */
    private void processINSERTCommand(String param) {
    	String[] parts = param.split("VALUES", 2);
    	String nomTable = parts[0].trim();
    	String values = parts[1].trim();

    	if (values.startsWith("(") && values.endsWith(")")) {
            values = values.substring(1, values.length() - 1).trim();
            // Sépare la chaîne principale par les virgules
            String[] listeValeur = values.split("\\s*,\\s*");
        
            // Parcourt chaque sous-chaîne (attribut) et traite les informations
            for (String valeur : listeValeur) {
                valeur = valeur.trim(); // Enlever les espaces superflus autour de chaque attribut
            }
            try {
            	dbM.InsertIntoCurrentDatabase(nomTable, listeValeur);
            } catch(Exception e) {
            	System.out.println(e.getMessage());
            }
        	System.out.println("Les valeurs ont été ajoutés à la table " + nomTable);
    	}
        else
    		System.out.println("Format d'Insert non respecté");
    }

    /**
     * Méthode pour traiter la commande BULKINSERT
     */
    private void processBULKINSERTCommand(String param) {
    	// Sépare la ligne de commande entre le nom de la table et le nom du fichier
    	String[] parts = param.split(" ", 2);
    	// Retire les espaces inutiles
    	String nomTable = parts[0].trim();
    	String nomFichier = parts[1].trim();
    	try {
        	dbM.BulkInsertIntoCurrentDatabase(nomTable, nomFichier);
    	} catch(Exception e) {
            e.printStackTrace();
    	}
    }
    
    /**
     * Méthode pour traiter la commande CREATEINDEX
     */
    private void processCREATEINDEXCommand(String param) {
    	String[] parts = param.split("KEY=", 2);
    	String nomRelation = parts[0].trim();
    	String[] arguments = parts[1].split("ORDER=", 2);
    	String nomColonne = arguments[0].trim();
    	int ordre = Integer.valueOf(arguments[1].trim());
    	try {
        	dbM.CreateIndex(nomRelation, nomColonne, ordre);
    	} catch(Exception e) {
            e.printStackTrace();
    	}
    }
    
    /**
     * Méthode pour traiter la commande SELECTINDEX
     */
    private void  processSELECTINDEXCommand(String param) {
    	String[] parts = param.split("WHERE", 2);
    	String nomRelation = parts[0].trim();
    	String[] arguments = parts[1].split("=", 2);
    	String nomColonne = arguments[0].trim();
    	String cle = arguments[1].trim();
    	try {
        	dbM.SelectIndex(nomRelation, nomColonne, cle);
    	} catch(Exception e) {
            e.printStackTrace();
    	}
    }

    /**
     * Méthode pour traiter la commande SELECT.
     * @param param Commande SQL sans le mot-clé "SELECT".
     */
    private void processSELECTCommand(String param) {
        if (dbM.getCurrentDatabase() == null)
            System.out.println("Erreur aucune database n'est défini");
        else {
            String selectReg = String.join("",
            "^(\\*|", 
            "(?:[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+", // alias.colonne
            "(?:\\s*,\\s*[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+)*))", 
            "\\s+FROM\\s+", 
            "([a-zA-Z0-9_]+\\s+[a-zA-Z0-9_]+", // Table avec alias
            "(?:\\s*,\\s*[a-zA-Z0-9_]+\\s+[a-zA0-9_]+)*)", 
            "(?:\\s+WHERE\\s+", // Optionnel WHERE
            "([a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\s*(?:=|<|>|<=|>=|<>)\\s*" + // Alias1.colonne OP
            "(?:[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+)?" + // Alias2.colonne (optionnel)
            "(?:\\s*(?:'[^']*'|[a-zA-Z0-9_]+))?", // Valeur ou autre colonne (optionnel)

            // Support pour plusieurs conditions avec AND
            "(?:\\s+AND\\s+" + 
            "[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\s*(?:=|<|>|<=|>=|<>)\\s*" + // Alias1.colonne OP
            "(?:[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+)?" + // Alias2.colonne (optionnel)
            "(?:\\s*(?:'[^']*'|[a-zA-Z0-9_]+))?" + // Valeur ou autre colonne (optionnel)
            ")*", // Permet plusieurs répétitions du motif AND-condition
            ")*)?$"
        );       
            // Compiler la regex
            Pattern pattern = Pattern.compile(selectReg, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(param);

            // Si la commande correspond à la regex, extraire les informations
            if (matcher.matches()) {
                String attributs = matcher.group(1); // Attributs sélectionnés (par exemple "*", "r.col1")
                String tables = matcher.group(2);  // Tables et alias dans FROM
                String whereConditions = matcher.group(3); // Conditions dans WHERE (optionnel)

                // Afficher les résultats extractions
                //System.out.println("Attributs : " + attributs);
                //System.out.println("Relation et allias : " + tables);
                //System.out.println("Conditions : " + whereConditions);

                try {
                    // Liste qui contient les relations
                    ArrayList<Relation> relations = new ArrayList<>();
                    // 1er element associe le nom d'une relation à son allias, le 2nd l'inverse
                    HashMap<String, String> assocAlliasToNom = extractRelation(tables, relations);

                    // Liste du nom des attributs à afficher sous la forme alias.nomAtrb
                    ArrayList<String> nomToPrint = new ArrayList<>();
                    // Map pour garder les décalages des relations
                    HashMap<String, Integer> relationOffsets = new HashMap<>();
                    int globalOffset = 0;

                    // Calculer les décalages globaux pour chaque relation en fonction de l'ordre d'apparition de la relation dans le 'FROM'
                    for (String alias : assocAlliasToNom.keySet()) {
                        String relationName = assocAlliasToNom.get(alias);
                        Relation relation = dbM.getCurrentDatabase().get(relationName);
                        relationOffsets.put(alias, globalOffset);

                        globalOffset += relation.getNbAttribut();
                    }
                    // Extrait les noms des attributs à afficher et remplie le 
                    ArrayList<Integer> attrbToPrint = extractAttribut(attributs, assocAlliasToNom, nomToPrint, relationOffsets);

                    // Pair qui contient en 1 les conditions interne des tables et en second les conditions de jointures
                    Pair<HashMap<String, ArrayList<Condition>>, ArrayList<Condition>> conditions = new Pair<>(); 
                    // Liste des conditions interne à une table sous forme nomRelaion -> liste conditions
                    HashMap<String, ArrayList<Condition>> internConditions = new HashMap<>();
                    // Liste des conditions de jointures entre 2 éléments
                    ArrayList<Condition> joinConditions = new ArrayList<>();

                    // Si on a des conditions
                    if (whereConditions != null) {
                        // Extrait les condition
                        conditions = extractCondition(whereConditions, assocAlliasToNom, relationOffsets);  // Extraire les conditions
                        internConditions = conditions.getFirst();   // Affecte les conditions interne
                        joinConditions = conditions.getSecond();    // Affecte les conditions de jointure
                    }

                    //System.out.println("Relations : " + relations);
                    //System.out.println("AssocAlliasToNom : " + assocAlliasToNom);
                    //System.out.println("NomToPrint : " + nomToPrint);
                    //System.out.println("RelationOffsets : " + relationOffsets);
                    //System.out.println("Attributs à afficher (attrbToPrint) : " + attrbToPrint);
                    //System.out.println("Conditions internes : " + internConditions);
                    //System.out.println("Conditions de jointure : " + joinConditions);
                    
                    // Utilise l'algo de jointure orienté page sur 2 relations sans conditions interne
                    if (relations.size() == 2 && internConditions.isEmpty()) {
                        PageOrientedJoinOperator pageJoin = new PageOrientedJoinOperator(new PageDirectoryIterator(relations.get(0), bm), new PageDirectoryIterator(relations.get(1), bm), joinConditions);
                        ProjectOperator projecectionOp = new ProjectOperator(pageJoin, attrbToPrint);
                        RecordPrinter printer = new RecordPrinter(projecectionOp, nomToPrint);

                        printer.printAllRecord();
                    }
                    // Sinon Utilise le tree d'execution
                    else {
                        // Exécuter la commande avec les opérateurs relationnels
                        TreeAlgebra planExec = new TreeAlgebra(relations, joinConditions, internConditions, attrbToPrint, nomToPrint, bm);
                        planExec.execute();   // Exécuter la commande avec les opérateurs relationnels
                    }
                } catch(Exception e){
                    e.printStackTrace();
                    System.out.println("Erreur lors de l'exécution de la commande "+e.getMessage());
                }
            }
            else
                System.out.println("Erreur de syntaxe dans la requête.");
        }
    }

    /**
     * Méthode pour extraire une relation (table et alias) à partir du paramètre.
     */
    private HashMap<String, String> extractRelation(String listeTableName, ArrayList<Relation> relations) {
        HashMap<String, Relation> db = dbM.getCurrentDatabase();
        HashMap<String, String> assocAlliasToNom = new HashMap<>();

        // Chaque case contient une relation et son allias
        String[] name = listeTableName.split("\\s*,\\s*");

        // Parcour le tableau
        for (String tableNameAndAlias : name) {
            // Split la chaine par l'espace [0] = nomRelation [1] = alias
            String[] str = tableNameAndAlias.split("\\s+");
            Relation r = db.get(str[0]);    // Récupere la relation depuis la bd

            // Test si la relation existe dans la bd courrante
            if (r != null) {
                relations.add(r);
                assocAlliasToNom.put(str[1], str[0]);  // Insère dans la map le couple
            }
            else
                throw new IllegalArgumentException("La relation "+str[0]+" n'existe pas dans la base de donnée courante");
        }
        return assocAlliasToNom;
    }


    /**
     * Méthode pour extraire les indices absolus des attributs à afficher dans le record fusionné.
     * Cette méthode prend en compte soit tous les attributs d'une relation (avec `*`), 
     * soit des attributs spécifiques (format "aliasRelation.nomAttribut").
     *
     * L'index absolu d'un attribut est déterminé par l'ordre des relations dans le `FROM` et la 
     * position de l'attribut au sein de sa relation respective. 
     * 
     * @param param Liste des attributs demandés dans la clause SELECT (ex. "aliasRel.col1, aliasRel.col2").
     *              Si `*`, tous les attributs des relations sont sélectionnés.
     * @param assocAlliasToNom Association des alias de relations aux noms réels des relations.
     * 
     * @return Liste des indices absolus des attributs dans le record fusionné. L'ordre des indices
     *         correspond à l'ordre dans lequel les attributs apparaissent dans la clause SELECT.
     * @throws IllegalArgumentException Si un alias ou une colonne est mal formé ou inexistant.
     */
    private ArrayList<Integer> extractAttribut(String param, HashMap<String, String> assocAlliasToNom, ArrayList<String> nomToPrint, HashMap<String, Integer> relationOffsets) {
        // Liste des index des attributs a afficher pour le dernier projet opérateur
        ArrayList<Integer> attributesToPrint = new ArrayList<>();
        int globalOffset = 0;
        // Chaque case contient allais.attrbName
        String[] attrbs = param.split("\\s*,\\s*");        

        // Si on a '*', on prend tous les attributs dans l'ordre des relations
        if ("*".equals(param.trim())) {
            // Parcours des relations dans l'ordre des alias
            for (String alias : assocAlliasToNom.keySet()) {
                String relationName = assocAlliasToNom.get(alias);
                Relation relation = dbM.getCurrentDatabase().get(relationName);

                // Ajouter les indices absolus pour tous les attributs de cette relation
                for (int i = 0; i < relation.getNbAttribut(); i++) {
                    // Ajoute l'index absolu
                    attributesToPrint.add(globalOffset);
                    globalOffset++;  // Incrémente le décalage pour la relation suivante

                    // Reconstruit le nom complet alias.nomAttribut
                    String attributeName = alias + "." + relation.getNameAttribut(i);
                    nomToPrint.add(attributeName);  // Ajoute le nom reconstruit
                }
            }
        }
        else {
            // Si on a des attributs spécifiques dans le SELECT
            // Capture les attributs que l'on veut afficher
            nomToPrint.addAll(Arrays.asList(attrbs));

            // Traitement des attributs spécifiques dans le SELECT
            for (String attrb : attrbs) {
                attrb = attrb.trim();
                // La case 0 contient l'allias et la case 1 son NameAttrbut
                String[] parts = attrb.split("\\.");  // Séparer l'alias et le nom de la colonne

                // Vérification que l'attribut est bien formé sous le format alias.colonne
                if (parts.length != 2)
                    throw new IllegalArgumentException("Attribut mal formé : " + attrb);

                // Récupère les composants
                String alias = parts[0];
                String columnName = parts[1];

                // Vérifier que l'alias correspond à une relation dans le FROM
                if (!assocAlliasToNom.containsKey(alias))
                    throw new IllegalArgumentException("Alias inconnu : " + alias);

                // Récupère le nom de la relation lié à cette allias
                String relationName = assocAlliasToNom.get(alias);
                // Récupère la relation associé
                Relation relation = dbM.getCurrentDatabase().get(relationName);

                // Trouver l'index relatif de la colonne dans la relation
                Integer relativeIndex = relation.getNameToIndex().get(columnName);

                // Si la colonne n'existe pas dans la relation, lancer une exception
                if (relativeIndex == null)
                    throw new IllegalArgumentException("Colonne inconnue : " + columnName);

                // Calculer l'index absolu en ajoutant le décalage global de la relation
                int absoluteIndex = relationOffsets.get(alias) + relativeIndex;
                attributesToPrint.add(absoluteIndex);
            }
        }
        return attributesToPrint;
    }

    /**
     * Méthode pour extraire les conditions WHERE.
     * @param param Partie WHERE (ex. "aliasRel.col1 = 10 AND aliasRel.col2 > 20").
     * @return Liste des conditions extraites.
     */
    private Pair<HashMap<String, ArrayList<Condition>>, ArrayList<Condition>> extractCondition(String param, HashMap<String, String> assocAlliasToNom, HashMap<String, Integer> relationOffsets) {
        // Tableau ou chaque case contient une condition
        String[] conditions = param.split("\\s+AND\\s+");
        // Liste ou on stock les conditons extrait
        Pair<HashMap<String, ArrayList<Condition>>, ArrayList<Condition>> res = new Pair<>();
        res.setFirst(new HashMap<>());
        res.setSecond(new ArrayList<>());

        // Regex pour capturer le patern d'une condition
        String regex = "(?:([a-zA-Z_][a-zA-Z0-9_]*)\\.)?([a-zA-Z_][a-zA-Z0-9_]*)\\s*(<|>|<=|>=|=|<>)\\s*(?:([a-zA-Z_][a-zA-Z0-9_]*)\\.)?([a-zA-Z_][a-zA-Z0-9_]*|'[a-zA-Z0-9_\\sÀ-ÿ]+'|[0-9]+(?:\\.[0-9]+)?)";
        Pattern pattern = Pattern.compile(regex);

        // Parcour le tableau des conditions
        for (String cond : conditions) {
            // Applique le patern à chaque conditions
            Matcher matcher = pattern.matcher(cond);

            // Si on à un match
            if (matcher.matches()) {
                // Récupère les groupes correspondants
                String alias1 = matcher.group(1);   // Alias du premier terme (peut être null)
                String colonne1 = matcher.group(2); // Colonne du premier terme
                String operateur = matcher.group(3); // Opérateur
                String alias2 = matcher.group(4);   // Alias du second terme (peut être null)
                String colonne2 = matcher.group(5); // Colonne ou valeur du second terme

                // Si c'est une condition sur la même table
                if ((alias1 == null) || (alias2 == null) || (alias1.equals(alias2))) {
                    // Contient le nom de la relation
                    String relationName;

                    // Extrait le nom de la relation
                    // L'un des 2 alias est null ou c'est le même
                    if (alias1 != null) relationName = assocAlliasToNom.get(alias1);
                    else if (alias2 != null) relationName = assocAlliasToNom.get(alias2);
                    else throw new IllegalArgumentException("erreur dans la condition: "+cond);

                    Relation relation = dbM.getCurrentDatabase().get(relationName);

                    // Trouver l'index relatif de la colonne dans la relation
                    Integer relativeIndex1 = relation.getNameToIndex().get(colonne1);
                    Integer relativeIndex2 = relation.getNameToIndex().get(colonne2);
                    
                    if (relativeIndex1 == null) relativeIndex1 = -1;

                    if (relativeIndex2 == null) relativeIndex2 = -1;

                    // Crée les pairs de terme
                    Pair<String, Integer> p1 = new Pair<String,Integer>(colonne1, relativeIndex1);
                    Pair<String, Integer> p2 = new Pair<String,Integer>(colonne2, relativeIndex2);
    
                    // Ajoute une nouvelle condition à la liste associée à la relation liée à alias1
                    res.getFirst().computeIfAbsent(assocAlliasToNom.get(alias1), k -> new ArrayList<>()).add(new Condition(p1, operateur, p2));
                }
                // Sinon c'est une condition de jointure
                else {
                    // Extrait le nom des relations
                    String relationName1 = assocAlliasToNom.get(alias1);
                    String relationName2 = assocAlliasToNom.get(alias2);

                    // Extrait les relations
                    Relation relation1 = dbM.getCurrentDatabase().get(relationName1);
                    Relation relation2 = dbM.getCurrentDatabase().get(relationName2);

                    // Trouver l'index relatif de la colonne dans la relation
                    Integer relativeIndex1 = relation1.getNameToIndex().get(colonne1);
                    Integer relativeIndex2 = relation2.getNameToIndex().get(colonne2);

                    // Si l'une des colonnes n'existe pas dans la relation, lancer une exception
                    if (relativeIndex1 == null || relativeIndex2 == null)
                        throw new IllegalArgumentException("Colonne inconnue : "+colonne1+" || "+colonne2);

                    // Calculer l'index absolu en ajoutant le décalage global de la relation
                    int absoluteIndex1 = relationOffsets.get(alias1) + relativeIndex1;
                    int absoluteIndex2 = relationOffsets.get(alias2) + relativeIndex2;

                    Pair<String, Integer> p1 = new Pair<String,Integer>(colonne1, absoluteIndex1);
                    Pair<String, Integer> p2 = new Pair<String,Integer>(colonne2, absoluteIndex2);
    
                    res.getSecond().add(new Condition(p1, operateur, p2));
                }
            }
            // Sinon c'est un problème dans la commande
            else
                throw new IllegalArgumentException("Condition invalide : "+cond);
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

    /**
     * Obtient l'instance de la configuration de la base de données.
     *
     * @return l'instance de {@link DBConfig}.
     */
    public DBConfig getDbc() {
        return dbc;
    }

    /**
     * Obtient l'instance du gestionnaire de disque.
     *
     * @return l'instance de {@link DiskManager}.
     */
    public DiskManager getDskM() {
        return dskM;
    }

    /**
     * Obtient l'instance du gestionnaire de buffers.
     *
     * @return l'instance de {@link BufferManager}.
     */
    public BufferManager getBm() {
        return bm;
    }

    /**
     * Obtient l'instance du gestionnaire de la base de données.
     *
     * @return l'instance de {@link DBManager}.
     */
    public DBManager getDbM() {
        return dbM;
    }
}