import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestListe{
    private Liste head;
    private Liste second;
    private Liste last;

    @BeforeEach
    public void setUp(){
        // Création des éléments
        PageId id1 = new PageId(1, 100);
        PageId id2 = new PageId(1, 200);
        PageId id3 = new PageId(2, 300);

        // Création de la liste
        head = new Liste(id1);
        second = new Liste(id2);
        last = new Liste(id3);

        // Configuration des liens
        head.suivant = second;
        second.precedent = head;

        second.suivant = last;
        last.precedent = second;
    }

    @Test
    public void testAddToHead(){
        Liste newHead = new Liste(new PageId(3, 400));
        Liste exHead = head;
        head = head.add(newHead); // Ajouter au début de la liste

        // Vérifier que le nouvel élément est en tête de la liste
        assertEquals(newHead, head, "Le nouvel élément devrait être la tête de la liste");
        assertEquals(newHead.suivant, exHead, "Le suivant du nouvel élément devrait être l'ancien premier élément");
        assertEquals(second.precedent.precedent, newHead, "Le prédécesseur de l'ancien premier élément devrait être le nouvel élément");
    }

    @Test
    public void testRemoveFirstElement(){
        // Suppression du premier élément (head)
        Liste newHead = head.remove();

        // Vérifier que le premier élément a bien été supprimé
        assertEquals(newHead, second, "Le nouvel élément de tête devrait être l'élément suivant");
        assertNull(newHead.precedent, "Le prédécesseur du nouvel élément de tête devrait être null");
        assertEquals(newHead.suivant, last, "Le suivant du nouvel élément de tête devrait être l'ancien dernier élément");
    }

    @Test
    public void testRemoveLastElement(){
        // Suppression du dernier élément (last)
        Liste newLast = last.remove();

        // Vérifier que le dernier élément a bien été supprimé
        assertEquals(newLast, second, "Le nouvel élément de fin devrait être l'élément précédent");
        assertNull(newLast.suivant, "Le suivant du nouvel élément de fin devrait être null");
        assertEquals(newLast.precedent, head, "Le prédécesseur du nouvel élément de fin devrait être l'ancien premier élément");
    }

    @Test
    public void testRemoveElementInMiddle(){
        // Essayer de supprimer un élément du milieu (second)
        assertThrows(IllegalArgumentException.class, () -> {
            second.remove();
        }, "Une exception devrait être lancée pour la suppression d'un élément au milieu de la liste.");
    }

    @Test
    public void testRemoveFromEmptyList(){
        // Tester la suppression sur une liste vide
        Liste emptyList = null;

        assertNull(emptyList, "La liste devrait être vide.");
    }
}
