import java.util.*;
import java.util.regex.*;

/**
 * La classe TreeAlgebra transforme une requête SQL en un arbre algébrique et optimise cet arbre.
 */
public class TreeAlgebra {

    // Définition des types d'opérations dans l'arbre algébrique
    enum OperationType {
        SELECT,        // Sélection σ
        PROJECTION,    // Projection π
        JOIN,          // Jointure ⋈
        AGGREGATE,     // Agrégation γ
    }

    // Classe interne représentant un noeud de l'arbre
    class AlgebraNode {
        OperationType operationType;
        String tableName;     // Nom de la table ou alias
        String condition;     // Condition de jointure ou de sélection
        List<AlgebraNode> children;  // Enfants de l'arbre

        // Constructeur pour un noeud avec opération
        AlgebraNode(OperationType operationType) {
            this.operationType = operationType;
            this.children = new ArrayList<>();
        }
    }

    private String sqlQuery;

    public TreeAlgebra(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    /**
     * Analyser la requête SQL et la transformer en un arbre algébrique.
     * @return Le noeud racine de l'arbre algébrique.
     */
    public AlgebraNode parseQuery() {
        // D'abord, enlever les espaces inutiles et normaliser la requête
        String normalizedQuery = normalizeQuery(sqlQuery);

        // Exemple d'analyse des différentes parties d'une requête simple
        return parseSelectQuery(normalizedQuery);
    }

    /**
     * Normaliser la requête SQL (par exemple, retirer les espaces inutiles et uniformiser les majuscules).
     * @param query La requête SQL brute.
     * @return La requête normalisée.
     */
    private String normalizeQuery(String query) {
        // Pour simplifier, on transforme tout en majuscules et on nettoie les espaces
        return query.replaceAll("\\s+", " ").toUpperCase().trim();
    }

    /**
     * Analyser une requête SELECT et la transformer en arbre algébrique.
     * @param query La requête SELECT à analyser.
     * @return Le nœud racine de l'arbre algébrique de la requête SELECT.
     */
    public AlgebraNode parseSelectQuery(String query) {
        // Extraction des colonnes
        String columns = extractColumns(query);

        // Extraction de la table
        String tableName = extractTable(query);

        // Extraction de la condition WHERE (qui peut contenir une sous-requête)
        String condition = extractCondition(query);

        // Créer un noeud de projection pour les colonnes
        AlgebraNode projectionNode = new AlgebraNode(OperationType.PROJECTION);
        projectionNode.condition = columns;

        // Créer un noeud de sélection pour la condition WHERE
        AlgebraNode selectionNode = new AlgebraNode(OperationType.SELECT);
        selectionNode.condition = condition;

        // Créer un noeud de table
        AlgebraNode tableNode = new AlgebraNode(OperationType.SELECT);
        tableNode.tableName = tableName;

        // Vérifier s'il y a une sous-requête dans la condition WHERE
        if (condition.contains("(")) {
            // Extraire et analyser la sous-requête uniquement si elle est dans un SELECT valide
            AlgebraNode subQueryNode = parseConditionWithSubQuery(condition);
            // Remplacer la condition WHERE par la sous-requête dans l'arbre
            selectionNode.condition = "Subquery"; // Marquer comme sous-requête
            selectionNode.children.add(subQueryNode);
        }

        // Relier les noeuds
        selectionNode.children.add(tableNode);
        projectionNode.children.add(selectionNode);

        return projectionNode;  // Le noeud racine de l'arbre algébrique
    }

    // Fonction d'extraction des colonnes
    public String extractColumns(String query) {
        // Extraction simple des colonnes après SELECT et avant FROM
        Pattern pattern = Pattern.compile("SELECT\\s+(.*?)\\s+FROM");
        Matcher matcher = pattern.matcher(query);

        if (matcher.find())
            return matcher.group(1);  // Colonnes sélectionnées

        return "";
    }

    // Fonction d'extraction de la table
    public String extractTable(String query) {
        Pattern pattern = Pattern.compile("FROM\\s+(\\S+)");
        Matcher matcher = pattern.matcher(query);

        if (matcher.find())
            return matcher.group(1);  // Nom de la table

        return "";
    }

    // Fonction d'extraction de la condition WHERE
    public String extractCondition(String query) {
        Pattern pattern = Pattern.compile("WHERE\\s+(.*)");
        Matcher matcher = pattern.matcher(query);

        if (matcher.find())
            return matcher.group(1);  // Condition WHERE

        return "";
    }

    // Fonction pour extraire une sous-requête
    public String extractSubQuery(String condition) {
        // Recherche des parenthèses pour extraire la sous-requête
        Pattern pattern = Pattern.compile("\\((SELECT.*)\\)");
        Matcher matcher = pattern.matcher(condition);

        if (matcher.find()) {
            return matcher.group(1);  // Retourne la sous-requête
        }
        return null;
    }

    /**
     * Analyser la condition WHERE qui peut contenir des sous-requêtes imbriquées.
     * @param condition La condition WHERE qui peut contenir des sous-requêtes.
     * @return Le noeud racine de l'arbre algébrique représentant la condition avec les sous-requêtes.
     */
    public AlgebraNode parseConditionWithSubQuery(String condition) {
        // Fonction récursive pour gérer plusieurs niveaux de sous-requêtes imbriquées
        // L'idée est d'analyser la condition WHERE, d'extraire toutes les sous-requêtes
        // et de les traiter récursivement.
        AlgebraNode rootNode = new AlgebraNode(OperationType.SELECT);
        Pattern pattern = Pattern.compile("\\((SELECT[^()]*\\))");  // Recherche des sous-requêtes avec des parenthèses

        // Trouver toutes les sous-requêtes dans la condition
        Matcher matcher = pattern.matcher(condition);
        StringBuffer resultCondition = new StringBuffer();
        int lastMatchEnd = 0;

        while (matcher.find()) {
            // Ajouter la partie avant la sous-requête
            resultCondition.append(condition.substring(lastMatchEnd, matcher.start()));
            
            // Traiter la sous-requête et ajouter un "placeholder" pour la sous-requête
            String subQuery = matcher.group(1);
            AlgebraNode subQueryNode = parseSelectQuery(subQuery);  // Traiter la sous-requête comme une nouvelle requête SELECT
            rootNode.children.add(subQueryNode);  // Ajouter la sous-requête dans l'arbre

            resultCondition.append("SUBQUERY");  // Remplacer la sous-requête par un "placeholder"
            lastMatchEnd = matcher.end();
        }

        // Ajouter la partie restante de la condition qui n'est pas une sous-requête
        resultCondition.append(condition.substring(lastMatchEnd));

        // Mettre à jour la condition avec les sous-requêtes traitées
        rootNode.condition = resultCondition.toString();
        return rootNode;
    }

    /**
     * Optimiser l'arbre algébrique en réorganisant les opérations pour améliorer les performances.
     * @param root Le nœud racine de l'arbre algébrique.
     * @return L'arbre optimisé.
     */
    public AlgebraNode optimizeTree(AlgebraNode root) {
        // Optimisation simple de réorganisation
        if (root != null && root.operationType == OperationType.PROJECTION) {
            AlgebraNode selectionNode = root.children.get(0);

            if (selectionNode.operationType == OperationType.SELECT) {
                root.children.set(0, selectionNode.children.get(0));
                selectionNode.children.get(0).children.add(selectionNode);
            }
        }
        return root;
    }

    /**
     * Afficher l'arbre algébrique sous une forme lisible.
     * @param node Le nœud de l'arbre à afficher.
     * @param indent Le niveau d'indentation.
     */
    public void printTree(AlgebraNode node, String indent) {
        if (node == null)
            return;

        System.out.println(indent + node.operationType + " (" + node.condition + ")");
        for (AlgebraNode child : node.children) {
            printTree(child, indent + "  ");
        }
    }
}