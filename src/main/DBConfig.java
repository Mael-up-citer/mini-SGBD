import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Classe représentant la configuration d'une base de données.
 * Elle permet de charger, modifier et sauvegarder la configuration,
 * ainsi que de tester la validité du chemin de la base de données.
 */
public class DBConfig {
    public static String dbpath; // Le chemin de la base de données
    public static int pagesize; // Taille d'une page en octet
    public static int dm_maxfilesize;  // Taille maximum d'un fichier en octet
    public static int bm_buffercount; // Nombre de buffers gérés par le BufferManager
    public static String bm_policy; // Politique de remplacement des buffers


    /**
     * Constructeur pour initialiser la configuration avec un Map de valeurs.
     * 
     * @param configValues Map contenant les paires clé-valeur à assigner aux champs de la classe.
     */
    public DBConfig(Map<String, String> configValues){
        // Itère sur chaque entrée du Map et assigne les valeurs aux champs correspondants
        for(Map.Entry<String, String> entry : configValues.entrySet())
            assignValue(entry.getKey(), entry.getValue());

        // Assure que la taille maximum d'un fichier est un multiple de la taille d'une page
        dm_maxfilesize -= (dm_maxfilesize % pagesize);
    }

    /**
     * Méthode pour charger la configuration depuis un fichier.
     * 
     * @param configFile Le chemin du fichier de configuration.
     * @return Une instance de DBConfig initialisée avec les valeurs du fichier.
     */
    public static DBConfig loadConfig(String configFile){
        // Lire le contenu du fichier de configuration
        String content = readConfigFile(configFile);

        if (content != null){
            // Analyser le contenu du fichier pour créer un Map des valeurs de configuration
            Map<String, String> configValues = parseConfig(content);
            // Retourner une nouvelle instance de DBConfig avec ces valeurs
            return new DBConfig(configValues);
        }
        return null; // Retourne null si le contenu du fichier est vide
    }

    /**
     * Méthode pour lire le contenu d'un fichier de configuration.
     * 
     * @param configFile Le chemin du fichier de configuration.
     * @return Le contenu du fichier sous forme de chaîne de caractères.
     */
    private static String readConfigFile(String configFile){
        try {
            // Lire tout le contenu du fichier et le retourner sous forme de chaîne
            return new String(Files.readAllBytes(Paths.get(configFile)));
        } catch(IOException e){
            System.err.println("Erreur lors de la lecture du fichier : " + e.getMessage());
            return null; // Retourner null en cas d'erreur
        }
    }

    /**
     * Méthode pour analyser le contenu d'un fichier de configuration et extraire les paires clé-valeur.
     * 
     * @param content Le contenu du fichier de configuration sous forme de chaîne.
     * @return Un Map contenant les clés et valeurs de configuration.
     */
    private static Map<String, String> parseConfig(String content){
        Map<String, String> configValues = new HashMap<>(); // Création du Map pour stocker les valeurs
        String[] lines = content.split("\n"); // Séparer le contenu en lignes

        // Itérer sur chaque ligne pour extraire les paires clé-valeur
        for (String line : lines){
            line = line.trim(); // Enlever les espaces en début et fin de ligne

            if(!line.isEmpty()){ // Vérifie que la ligne n'est pas vide
                String[] parts = line.split("=", 2); // Diviser la ligne en clé et valeur

                if(parts.length == 2){
                    String key = parts[0].trim(); // Extraire la clé
                    String value = parts[1].trim().replace("'", "").replace("\"", ""); // Extraire et nettoyer la valeur
                    configValues.put(key, value); // Ajouter la paire clé-valeur au Map
                }
            }
        }
        return configValues; // Retourner le Map des valeurs de configuration
    }

    /**
     * Méthode pour assigner une valeur à un champ en fonction de sa clé et du type de donnée.
     * 
     * @param key La clé du champ à initialiser.
     * @param value La valeur à assigner au champ.
     */
    private void assignValue(String key, String value){
        try {
            // Récupérer le champ correspondant à la clé
            Field field = this.getClass().getDeclaredField(key);
            field.setAccessible(true); // Rendre le champ accessible

            // Récupérer le type du champ
            Class<?> fieldType = field.getType();
    
            // Assigner la valeur au champ en fonction de son type
            switch (fieldType.getSimpleName()){
                case "String":
                    field.set(this, value); // Assignation de la valeur si c'est une chaîne
                    break;

                case "int":
                    field.set(this, Integer.parseInt(value)); // Assignation de la valeur si c'est un entier
                    break;

                case "boolean":
                    field.set(this, Boolean.parseBoolean(value)); // Assignation de la valeur si c'est un boolean
                    break;

                case "double":
                    field.set(this, Double.parseDouble(value)); // Assignation de la valeur si c'est un double
                    break;

                case "float":
                    field.set(this, Float.parseFloat(value)); // Assignation de la valeur si c'est un float
                    break;

                case "long":
                    field.set(this, Long.parseLong(value)); // Assignation de la valeur si c'est un long
                    break;

                case "short":
                    field.set(this, Short.parseShort(value)); // Assignation de la valeur si c'est un short
                    break;

                case "byte":
                    field.set(this, Byte.parseByte(value)); // Assignation de la valeur si c'est un byte
                    break;

                default:
                    System.err.println("Type non supporté pour la variable : " + key);
            }
    
        } catch(NoSuchFieldException | IllegalAccessException e){
            // Gestion des exceptions si le champ n'est pas trouvé ou si son accès échoue
            System.err.println("Variable non reconnue : " + key);
        }
    }

    /**
     * Méthode pour afficher les valeurs de configuration.
     */
    public void displayConfig(){
        // Récupérer tous les champs de la classe
        Field[] fields = this.getClass().getDeclaredFields();

        for(Field field : fields){
            try{
                field.setAccessible(true); // Rendre le champ accessible
                Object value = field.get(this); // Obtenir la valeur du champ
                System.out.println(field.getName() + " = " + value); // Afficher le nom et la valeur du champ
            } catch(IllegalAccessException e){
                System.err.println("Erreur d'accès au champ : " + field.getName());
            }
        }
    }

    /**
     * Méthode pour sauvegarder la configuration actuelle dans un fichier.
     * 
     * @param configFile Le chemin du fichier où sauvegarder la configuration.
     */
    public void pushConfig(String configFile){
        try (FileOutputStream fos = new FileOutputStream(configFile);
             FileChannel channel = fos.getChannel()){

            StringBuilder configContent = new StringBuilder();  // Créer une chaîne pour contenir la configuration
            Field[] fields = this.getClass().getDeclaredFields(); // Récupérer tous les champs de la classe

            for(Field field : fields){
                field.setAccessible(true); // Rendre le champ accessible
                String value = String.valueOf(field.get(this)); // Obtenir la valeur du champ

                // Ajouter les guillemets uniquement pour les chaînes de caractères
                if (field.getType() == String.class){
                    configContent.append(field.getName())
                            .append(" = \"")
                            .append(value)
                            .append("\"\n");
                }
                else{
                    configContent.append(field.getName())
                            .append(" = ")
                            .append(value)
                            .append("\n");
                }
            }
            // Convertir le contenu de la configuration en bytes et l'écrire dans le fichier
            ByteBuffer buffer = ByteBuffer.wrap(configContent.toString().getBytes());
            channel.write(buffer);

        } catch (IOException | IllegalAccessException e){
            System.err.println("Erreur lors de l'écriture dans le fichier : " + e.getMessage());
        }
    }

    /**
     * Méthode pour tester la validité du chemin de la base de données.
     * 
     * @return true si le chemin est valide, false sinon.
     */
    public static boolean testDbpath(){
        boolean flag = true; // Flag pour tester si le chemin est valide

        // Vérifie si le chemin est vide
        if(dbpath == null || dbpath.isEmpty()){
            System.err.println("Le chemin dbpath est vide ou null.");
            flag = false;
        }

        // Vérifie si le chemin existe
        else if(!Files.exists(Paths.get(dbpath))){
            System.err.println("Le chemin dbpath n'existe pas : " + dbpath);
            flag = false;
        }

        // Vérifie si le chemin est un répertoire
        else if(!Files.isDirectory(Paths.get(dbpath))){
            System.err.println("Le chemin dbpath n'est pas un répertoire : " + dbpath);
            flag = false;
        }

        // Si un test échoue, on met dbpath à null
        if(!flag) {
            dbpath = null;
        }

        return flag; // Retourne l'état du flag
    }
}