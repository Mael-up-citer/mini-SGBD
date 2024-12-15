import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * La classe {@code treeAlgebra} représente une version simplifiée de l'arbre d'exécution 
 * pour traiter les requêtes relationnelles. Elle gère la sélection, 
 * la projection et les jointures nécessaires pour obtenir les résultats 
 * d'une requête relationnelle et les afficher.
 *
 * <p>C'est une première version simplifiée de l'arbre d'exécution, destinée à 
 * traiter des requêtes sur une ou plusieurs relations avec des conditions internes 
 * (par relation) et des conditions de jointure. Les résultats sont projetés 
 * selon une liste d'attributs et affichés.</p>
 *
 * <h3>Fonctionnalités principales :</h3>
 * <ul>
 *     <li>Création d'un opérateur SELECT pour chaque relation, 
 *     en appliquant les conditions internes associées.</li>
 *     <li>Construction des opérateurs de jointure pour combiner plusieurs relations.</li>
 *     <li>Application d'un opérateur de projection pour restreindre les attributs affichés.</li>
 *     <li>Affichage des résultats à l'aide d'un opérateur d'impression.</li>
 * </ul>
 *
 * <h3>Conventions et prérequis :</h3>
 * <ul>
 *     <li>Les relations doivent être fournies avec leurs conditions associées 
 *     sous forme de {@code ArrayList} ou de {@code HashMap}.</li>
 *     <li>Les indices et noms des attributs à projeter doivent être clairement spécifiés.</li>
 *     <li>La classe s'appuie sur un gestionnaire de buffers pour accéder aux pages des relations.</li>
 * </ul>
 *
 * <p>Cette version est simplifiée pour faciliter la compréhension et l'expérimentation 
 * de l'arbre d'exécution, mais elle ne prend pas en charge tous les cas d'usage avancés.</p>
 *
 * <p>En cas de problème lors de la construction de l'arbre (comme une liste de relations vide), 
 * une exception est levée. Toutes les erreurs d'exécution sont capturées et affichées.</p>
 */
public class TreeAlgebra {
    private RecordPrinter root; // L'opérateur en haut de l'arbre, chargé d'afficher les résultats.

    /**
     * Construit l'arbre d'exécution pour une requête.
     *
     * @param relations      Liste des relations impliquées.
     * @param joinConditions Liste des conditions pour les jointures.
     * @param innerConditions Conditions internes à chaque relation.
     * @param attbToPrint    Liste des indices des attributs à projeter.
     * @param attrbName      Liste des noms des attributs à afficher.
     * @param bm             Instance du gestionnaire de buffers.
     */
    public TreeAlgebra(
        ArrayList<Relation> relations, 
        ArrayList<Condition> joinConditions, 
        HashMap<String, ArrayList<Condition>> innerConditions, 
        ArrayList<Integer> attbToPrint, 
        ArrayList<String> attrbName, 
        BufferManager bm
    ) {
        // Vérifie que la liste des relations n'est pas vide.
        if (relations == null || relations.isEmpty())
            throw new IllegalArgumentException("La liste des relations ne peut pas être vide.");

        try {
            ProjectOperator projectionOp; // Opérateur de projection qui restreint les attributs affichés.

            // Cas 1 : Une seule relation (aucune jointure à effectuer).
            if (relations.size() == 1) {
                // Crée un opérateur SELECT pour la relation unique.
                selectOperator select = createSelectOperator(relations.get(0), innerConditions, bm);
                // Applique la projection sur l'opérateur SELECT.
                projectionOp = new ProjectOperator(select, attbToPrint);
            }
            // Cas 2 : Plusieurs relations (jointures nécessaires).
            else {
                // Crée l'opérateur de jointure combinant toutes les relations.
                IRecordIterator joinOperator = createJoinOperator(relations, innerConditions, joinConditions, bm);
                // Applique la projection sur le résultat des jointures.
                projectionOp = new ProjectOperator(joinOperator, attbToPrint);
            }

            // Configure l'affichage à partir de l'opérateur de projection.
            root = new RecordPrinter(projectionOp, attrbName);
        } catch (Exception e) {
            e.printStackTrace();
            // Capture et affiche les erreurs survenues lors de la construction de l'arbre.
            System.out.println("Erreur d'exécution de la requête : " + e.getMessage());
        }
    }

    /**
     * Crée un opérateur de sélection pour une relation.
     *
     * @param relation       La relation à sélectionner.
     * @param innerConditions Les conditions internes pour cette relation.
     * @param bm             Le gestionnaire de buffers.
     * @return Un opérateur de sélection configuré.
     */
    private selectOperator createSelectOperator(Relation relation, HashMap<String, ArrayList<Condition>> innerConditions, BufferManager bm) throws Exception {
        // Récupère les conditions internes associées à la relation. Si aucune condition n'existe, retourne une liste vide.
        ArrayList<Condition> conditions = innerConditions.getOrDefault(relation.getRelationName(), new ArrayList<>());

        // Retourne un opérateur SELECT configuré pour appliquer les conditions internes sur la relation.
        return new selectOperator(
            new PageDirectoryIterator(relation, bm), 
            relation, 
            conditions, 
            bm
        );
    }

    /**
     * Crée un opérateur de jointure pour une liste de relations.
     *
     * @param relations      Liste des relations à joindre.
     * @param innerConditions Conditions internes à chaque relation.
     * @param joinConditions Liste des conditions pour les jointures.
     * @param bm             Le gestionnaire de buffers.
     * @return L'opérateur de jointure combiné.
     */
    private IRecordIterator createJoinOperator(
        List<Relation> relations, 
        HashMap<String, ArrayList<Condition>> innerConditions, 
        ArrayList<Condition> joinConditions, 
        BufferManager bm
    ) throws Exception {
        // Initialise une liste pour stocker les opérateurs SELECT de chaque relation.
        List<IRecordIterator> selectOperators = new ArrayList<>();

        // Parcourt chaque relation pour créer un opérateur SELECT.
        for (Relation relation : relations)
            // Ajoute un opérateur SELECT pour chaque relation, en appliquant les conditions internes.
            selectOperators.add(createSelectOperator(relation, innerConditions, bm));

        // Initialise l'opérateur de jointure avec le premier opérateur SELECT de la liste.
        IRecordIterator joinOperator = selectOperators.get(0);

        // Parcourt les relations restantes pour construire les jointures sauf la dernière
        for (int i = 1; i < selectOperators.size()-1; i++) {
            // Combine l'opérateur de jointure actuel avec le prochain opérateur SELECT.
            joinOperator = new JoinOperator(
                new Pair<>(joinOperator, selectOperators.get(i)), 
                new ArrayList<>() // Conditions pour la jointure actuelle.
            );
        }
        // Combine l'opérateur de jointure actuel avec le prochain opérateur SELECT.
        joinOperator = new JoinOperator(
            new Pair<>(joinOperator, selectOperators.get(selectOperators.size()-1)), 
            joinConditions // Conditions pour la jointure actuelle.
        );
        // Retourne l'opérateur de jointure combiné.
        return joinOperator;
    }

    /**
     * Exécute l'arbre d'exécution pour afficher les résultats.
     */
    public void execute() {
        // Si la racine de l'arbre existe, imprime tous les enregistrements.
        if (root != null) root.printAllRecord();
        else
            // Sinon, affiche un message indiquant que l'arbre est vide.
            System.out.println("L'arbre d'exécution est vide.");
    }
}