import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TestDate{

    @Test
    public void testValidDateCreation() {
        // Test de la création d'une date valide
        Date date = new Date(15, 5, 2023);
        assertEquals(15, date.getDay());
        assertEquals(5, date.getMonth());
        assertEquals(2023, date.getYear());
        assertEquals("15/05/2023", date.toString());
    }

    @Test
    public void testInvalidDateCreation() {
        // Test de la création d'une date invalide (jour)
        assertThrows(IllegalArgumentException.class, () -> {
            new Date(32, 1, 2023); // 32 janvier n'est pas valide
        });

        // Test de la création d'une date invalide (mois)
        assertThrows(IllegalArgumentException.class, () -> {
            new Date(15, 13, 2023); // Mois 13 n'est pas valide
        });

        // Test de la création d'une date invalide (année)
        assertThrows(IllegalArgumentException.class, () -> {
            new Date(150, 5, -2023); // Année négative n'est pas valide
        });
    }

    @Test
    public void testLeapYear() {
        // Test des années bissextiles
        assertDoesNotThrow(() -> new Date(29, 2, 2020)); // 2020 est une année bissextile
        assertThrows(IllegalArgumentException.class, () -> {
            new Date(29, 2, 2021); // 2021 n'est pas bissextile
        });
    }

    @Test
    public void testCompareTo() {
        Date date1 = new Date(1, 1, 2020);
        Date date2 = new Date(1, 1, 2021);
        Date date3 = new Date(2, 1, 2020);
        Date date4 = new Date(1, 1, 2020);

        assertTrue(date1.compareTo(date2) < 0); // date1 est avant date2
        assertTrue(date2.compareTo(date1) > 0); // date2 est après date1
        assertTrue(date1.compareTo(date4) == 0); // date1 est égale à date4
        assertTrue(date1.compareTo(date3) < 0); // date1 est avant date3
    }

    @Test
    public void testSettersWithValidation() {
        Date date = new Date(1, 1, 2023);

        // Test de la mise à jour du jour
        date.setDay(15);
        assertEquals(15, date.getDay());

        // Test de la mise à jour du mois
        date.setMonth(5);
        assertEquals(5, date.getMonth());

        // Test de la mise à jour de l'année
        date.setYear(2024);
        assertEquals(2024, date.getYear());

        // Test des valeurs invalides
        assertThrows(IllegalArgumentException.class, () -> {
            date.setDay(32); // 32 n'est pas un jour valide
        });

        assertThrows(IllegalArgumentException.class, () -> {
            date.setMonth(13); // 13 n'est pas un mois valide
        });
    }
}