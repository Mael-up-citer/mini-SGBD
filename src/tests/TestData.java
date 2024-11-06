import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestData{
    private Data d; // Instance de Data pour les tests

    @Test
    void testConstructeur(){
        // Test du constructeur avec un argument null (devrait lever une exception)
        Exception exception0 = assertThrows(Exception.class, () -> {
            new Data(null); // On s'attend à ce qu'une exception soit levée
        });
        
        // Affiche le message de l'exception levée
        System.out.println("Exception levée pour le premier constructeur : " + exception0.getMessage());

        // Test du constructeur avec DataType.INT (devrait lever une IllegalArgumentException)
        IllegalArgumentException exception1 = assertThrows(IllegalArgumentException.class, () -> {
            new Data(DataType.INT, 5); // On s'attend à une IllegalArgumentException ici
        });
        
        // Affiche le message de l'exception levée
        System.out.println("Exception levée pour le constructeur avec DataType.INT : " + exception1.getMessage());

        // Test du constructeur avec DataType.CHAR (devrait également lever une IllegalArgumentException)
        IllegalArgumentException exception2 = assertThrows(IllegalArgumentException.class, () -> {
            new Data(DataType.CHAR); // On s'attend à une IllegalArgumentException ici
        });

        // Affiche le message de l'exception levée
        System.out.println("Exception levée pour le constructeur avec DataType.CHAR : " + exception2.getMessage());
    }


    @Test
    void testSetLength() {
        d = new Data(DataType.INT); // Crée une instance de Data avec DataType.INT

        // Tenter de définir une longueur invalide (devrait lever une IllegalArgumentException)
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            d.setLenght(-1); // Exemple d'appel de méthode qui devrait lever une exception
        });

        // Affiche le message de l'exception levée
        System.out.println("Exception levée lors de la définition d'une longueur invalide : " + exception.getMessage());
    }
}