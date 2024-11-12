import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class requete {

    /**
     * Cette méthode décompose une requête SQL en sous-requêtes et les exécute avant de traiter la requête principale.
     *
     * @param query La requête SQL à analyser et exécuter.
     */
    private void decomposeQueryInSubqueries(String query) {
        // Expression régulière pour capturer les sous-requêtes SELECT imbriquées
        String regex = "\\(SELECT\\b(?:[^()]*|\\((?:[^()]*|\\([^()]*\\))*\\))*";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(query);

        // Liste pour stocker les sous-requêtes trouvées
        List<String> subqueries = new ArrayList<>();

        // Capturer toutes les sous-requêtes
        while (matcher.find()) {
            subqueries.add(matcher.group());
        }

        // Exécuter chaque sous-requête et la remplacer par son résultat dans la requête principale
        for (int i = subqueries.size() - 1; i >= 0; i--) {
            String subquery = subqueries.get(i);
            String result = processSELECTCommand(subquery);
            query = query.replace(subquery, result);
            System.out.println("Après remplacement de la sous-requête : " + query);
        }

        // Après le traitement des sous-requêtes, associer la requête principale à une méthode via réflexion
        assocQuery(query);
    }
}
