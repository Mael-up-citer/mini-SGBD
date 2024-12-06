import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Classe représentant un gestionnaire de Base de données.
 * Le DBManager gère une liste de Bases de Donées, réalise ou redirige les actions demandées par l'utilisateur sur une ou plusieurs base de données.
 * Il stocke une base de Données courante sur laquelle travaille l'utilisateur.
 */
public class DBManager {
	private HashMap<String, HashMap<String, Relation>> listeDatabase;
	private HashMap<String, Relation> current;
	private DBConfig dbc;
	private DiskManager dskm;
	private BufferManager bm;
	
	/**
	 * Constructeur de la classe DBManager
	 * @param dbc une instance de la classe DBConfig
	 */
	public DBManager(DBConfig dbc, DiskManager dskm, BufferManager bm) {
		this.listeDatabase = new HashMap<>();
		this.dbc = dbc;
		this.dskm = dskm;
		this.bm = bm;
	}

    /**
     * Crée une Base de données dans le gestionnaire
     * 
     * @param databaseName nom de la Base de Données à créer
     */
    public void CreateDatabase(String databaseName) throws IllegalArgumentException{
		// Vérifie que le bd n'existe pas deja
		if (listeDatabase.containsKey(databaseName.toUpperCase())) {
			throw new IllegalArgumentException("Duplicate data base "+databaseName);
		}

    	// Crée une nouvelle Database et l'ajoute dans la liste des database crées
        listeDatabase.put(databaseName.toUpperCase(), new HashMap<>());
    }

    /**
     * Stocke la Base de Données demandées en tant que Base de données courante
     * 
     * @param databaseName nom de la Base de Données courante
     */
    public void SetCurrentDatabase(String databaseName) throws IllegalArgumentException {
		// Vérifie que la base de donnée existe
    	if(!listeDatabase.containsKey(databaseName.toUpperCase())) {
    		throw new IllegalArgumentException("La base de donnée " + databaseName + " n'existe pas");
    	}

    	// Charge la database demandée dans current
        this.current = listeDatabase.get(databaseName.toUpperCase());
    }

    /**
     * Ajoute une table dans la Base de Données courante
     * 
     * @param tab Table à ajouter
     */
    public void AddTableToCurrentDatabase(Relation tab) throws IllegalArgumentException{
    	if(current == null) {
    		throw new IllegalArgumentException("La Base de Données de travail n'a pas été définie");
    	}

    	// Ajoute la table à la BDD current
        current.put(tab.getRelationName(), tab);
        // Informe l'utilisateur de la réussite de l'opération
        System.out.println("Table " + tab.getRelationName() + " ajoutée à la base de données actuelle.");
    }
    
    public void InsertIntoCurrentDatabase(String nomTable, String[] valeurs) throws Exception{
    	if(current == null) {
    		throw new IllegalArgumentException("La Base de Données de travail n'a pas été définie");
    	}
    	Relation rel = GetTableFromCurrentDatabase(nomTable);
    	if(valeurs.length != rel.getNbAttribut()) {
    		throw new IllegalArgumentException("Le nombre d'arguments n'est pas égal au nombre requis pour cette table : " + rel.getNbAttribut());
    	}
    	MyRecord rec = new MyRecord();
    	for(int i = 0; i<rel.getNbAttribut(); i++) {
    		rec.add(valeurs[i], rel.getType(i));
    	}
    	rel.InsertRecord(rec);
    }
    
    /**
     * Renvoi la table demandée de la BDD courante
     * 
     * @param nomTable nom de la table à aller chercher
     * @return la table demandée
     */
    public Relation GetTableFromCurrentDatabase(String nomTable) throws IllegalArgumentException {
    	if(current == null) {
    		throw new IllegalArgumentException("La Base de Données de travail n'a pas été définie");
    	}
    	if(!current.containsKey(nomTable.toUpperCase())) {
    		throw new IllegalArgumentException("La table " + nomTable + "n'existe pas");
    	}
    	// Demande la table en question à current
    	return current.get(nomTable.toUpperCase());
    }

    /**
     * Supprime une table de la BDD courante
     * 
     * @param nomTable nom de la table à supprimer
     */
    public void RemoveTableFromCurrentDatabase(String nomTable) throws IllegalArgumentException {
    	if(current == null) {
    		throw new IllegalArgumentException("La Base de Données de travail n'a pas été définie");
    	}

    	if(!current.containsKey(nomTable.toUpperCase())) {
    		throw new IllegalArgumentException("La table " + nomTable + "n'existe pas");
    	}

    	// Supprime la table de current
        current.remove(nomTable.toUpperCase());
    }
    
    /**
     * Supprime une Base de Données
     * 
     * @param databaseName nom de la Base de Données à supprimer
     */
    public void RemoveDatabase(String databaseName) throws IllegalArgumentException {
    	if(!listeDatabase.containsKey(databaseName.toUpperCase())) {
    		throw new IllegalArgumentException("La base de donnée " + databaseName + " n'existe pas");
    	}

        // Supprime la BDD de la liste de BDD existantes
        listeDatabase.remove(databaseName.toUpperCase());
    }
    
    /**
     * Supprime toutes les tables de la Base de Données courante
     */
    public void RemoveTablesFromCurrentDatabase() throws IllegalArgumentException {
    	if(current == null) {
    		throw new IllegalArgumentException("La Base de Données de travail n'a pas été définie");
    	}

    	// Vide la BDD courante
    	current.clear();
    }
    
    /**
     * Supprime toutes les Bases de Données existantes
     */
    public void RemoveDatabases(){
    	// Vide le stockage courant
    	current = null;
    	// Vide la liste des Base de Données
    	listeDatabase.clear();
    }

    /**
     * Affiche la liste des Bases de Données existantes
     */
    public void ListDatabases() {
    	System.out.println("Les Bases de Données enregistrées sont :");
    	// Parcourt la liste des BDD et affiche ses clés
    	// Les clés de la liste sont les noms des BDD
        for(String i : listeDatabase.keySet())
        	System.out.println(i);
    }
    
    /**
     * Affiche la liste des tables de la Base de Données courante
     */
    public void ListTablesInCurrentDatabase() throws IllegalArgumentException {
    	if(current == null) {
    		throw new IllegalArgumentException("La Base de Données de travail n'a pas été définie");
    	}

    	System.out.println("Les tables de cette base de données sont :");
    	StringBuilder database = new StringBuilder();
		// Parcourt la liste de tables et affiche ses clés, ce sont les noms des tables
        for(String tableNom : current.keySet()) {
        	database.append(tableNom).append(" (");
        	Relation rel = current.get(tableNom);
        	for(int i = 0; i < rel.getNbAttribut(); i++) {
        		if(i!=0) {
        			database.append(",");
        		}
        		database.append(rel.getNameAttribut(i));
        		database.append(":");
        		database.append(rel.getType(i));
        		database.append("(");
        		database.append(rel.getLength(i));
        		database.append(")");
        	}
        	database.append(")");
        	System.out.println(database.toString());
        	database.setLength(0);
        }
    }

    /**
     * Sauvegarde l'état du DBManager dans un fichier
     */
    public void saveState() {
    	try (FileOutputStream fos = new FileOutputStream(DBConfig.dbpath + "/databases.save"); FileChannel channel = fos.getChannel()){

    		StringBuilder saveContent = new StringBuilder();  // Créer une chaîne pour contenir la configuration
            ByteBuffer buffer;
               
            for(String bddName : listeDatabase.keySet()) {
            	saveContent.append("BDD = ");
                saveContent.append(bddName);
                saveContent.append("\n");
                HashMap <String, Relation> bdd = listeDatabase.get(bddName);
                
                for(String tableNom : bdd.keySet()) {
                	saveContent.append("Table = ");
                	saveContent.append(tableNom);
                	saveContent.append("; ");
                	Relation rel = bdd.get(tableNom);
                	PageId relHPId = rel.getHeaderPageId();
                	saveContent.append(relHPId.FileIdx);
                	saveContent.append("; ");
                	saveContent.append(relHPId.PageIdx);
                	saveContent.append("\n");

                	for(int i = 0; i < rel.getNbAttribut(); i++) {
                		saveContent.append("Colonne = ");
                		saveContent.append(rel.getNameAttribut(i));
                		saveContent.append("; ");
                		saveContent.append(rel.getType(i));
                		saveContent.append("; ");
                		saveContent.append(rel.getLength(i));
                		saveContent.append("\n");
                	}
               }
            }
    		// Convertir le contenu de la configuration en bytes et l'écrire dans le fichier
            buffer = ByteBuffer.wrap(saveContent.toString().getBytes());
            channel.write(buffer);
               
    	} catch (IOException e){
    		System.err.println("Erreur lors de l'écriture dans le fichier : " + e.getMessage());
        }
    }
    
    /**
     * Charge l'état du DBManager depuis le fichier de sauvegarde
     */
    public void loadState() throws IOException{
    	HashMap<String, Relation> DatabaseEnCours = null;
    	Relation RelationEnCours = null;
        String save = readSave();

        if(save == null) {
        	throw new IOException("La sauvegarde est vide");
        }
        else {
        	String[] lines = save.split("\n");
        	for(String line : lines) {
        		line = line.trim(); // Enlever les espaces en début et fin de ligne
        		if(!line.isEmpty()){ // Vérifie que la ligne n'est pas vide
        			String[] parts = line.split("=", 2); // Diviser la ligne en mot clé et valeur
        			if(parts.length == 2){
        				 String keyword = parts[0].trim(); // Extraire la clé
        				 switch(keyword) {
        				 case "BDD":
        					 listeDatabase.put(parts[1].trim(), new HashMap<>());
        					 DatabaseEnCours = listeDatabase.get(parts[1].trim());
        					 RelationEnCours = null;
        					 break;
        					 
        				 case "Table":
        					 if(DatabaseEnCours == null) {
        						 RemoveDatabases();
        						 throw new IOException("La sauvegarde n'est pas complète");
        					 }else {
        						 String[] infos = parts[1].trim().split(";", 3);
        						 String nomRel = infos[0].trim();
        						 PageId headerPage = new PageId(Integer.parseInt(infos[1].trim()), Integer.parseInt(infos[2].trim()));
        						 Relation rel = new Relation(nomRel, new ArrayList<>(), headerPage, dskm, bm);
        						 DatabaseEnCours.put(nomRel, rel);
        						 RelationEnCours = rel;
        					 }
        					 break;
        					 
        				 case "Colonne":
        					 if(RelationEnCours == null) {
        						 RemoveDatabases();
        						 throw new IOException("La sauvegarde n'est pas complète");
        					 }else {
        						 String[] infos = parts[1].trim().split(";", 3);
        						 DataType typeCol = DataType.valueOf(infos[1].trim());
        						 Data dataCol;
        						 if((typeCol == DataType.CHAR) || (typeCol == DataType.VARCHAR)) {
        							 dataCol = new Data(typeCol, Integer.parseInt(infos[2].trim()));
        						 }else {
        							 dataCol = new Data(typeCol);
        						 }
        						 RelationEnCours.setOneAttribut(new Pair<>(infos[0].trim(), dataCol));
        					 }
        					 break;
        				 default:
        					 throw new IOException("La sauvegarde n'est pas complète");
        				 }
        			}
        		}
        	}
        }
    }
    
    /**
     * Lis les bytes de la sauvegarde
     * 
     * @return Une chaîne de caractère contenant la sauvegarde
     */
    public String readSave() throws IOException{
    	// Lire tout le contenu du fichier et le retourner sous forme de chaîne
        return new String(Files.readAllBytes(Paths.get(DBConfig.dbpath + "/databases.save")));
    }

    /**
     * Récupère la liste des Bases de données de ce gestionnaire
     * 
     * @return la liste des Bases de données de ce gestionnaire
     */
    public HashMap<String, HashMap<String, Relation>> getListeDatabase(){
    	return listeDatabase;
    }

	/**
	 * Vérifie si une table existe dans la base de données courante.
	 * 
	 * Cette méthode vérifie si une table, identifiée par son nom, existe dans la base de données courante.
	 * Elle retourne un booléen indiquant si la table existe ou non.
	 * 
	 * @param str Le nom de la table à vérifier.
	 * @return true si la table existe dans la base de données courante, sinon false.
	 * @throws IllegalArgumentException Si la base de données courante n'a pas été définie.
	 */
	public boolean tableExiste(String str) throws IllegalArgumentException{
		if (current == null) {
			// Si aucune base de données courante n'a été définie, retourner false
			throw new IllegalArgumentException("La Base de Données de travail n'a pas été définie");
		}

		// Vérifie si la table existe dans la base de données courante
		return current.containsKey(str);
	}

	/**
     * Récupère la Base de Données courante
     * 
     * @return la Base de Données courante
     */
    public HashMap<String, Relation> getCurrentDatabase(){
    	return current;
    }
}