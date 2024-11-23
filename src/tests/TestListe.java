import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Classe de tests pour la classe Liste, vérifiant les fonctionnalités d'ajout en tête et de suppression
 * uniquement de la tête ou de la queue, conformément aux restrictions spécifiées.
 */
public class TestListe {

    private Liste head;   // Élément en tête
    private Liste middle; // Élément intermédiaire
    private Liste last;   // Élément en queue

    /**
     * Configuration initiale exécutée avant chaque test.
     * Elle crée une liste chaînée avec plusieurs éléments.
     */
    @BeforeEach
    public void setUp() {
        // Création des identifiants pour les pages
        PageId id1 = new PageId(1, 100);
        PageId id2 = new PageId(1, 200);
        PageId id3 = new PageId(2, 300);
        PageId id4 = new PageId(3, 400);
        PageId id5 = new PageId(4, 500);

        // Initialisation des éléments de la liste
        head = new Liste(id1);
        Liste second = new Liste(id2);
        middle = new Liste(id3);
        Liste fourth = new Liste(id4);
        last = new Liste(id5);

        // Liaison entre les éléments
        head.suivant = second; 
        second.precedent = head;

        second.suivant = middle;
        middle.precedent = second;

        middle.suivant = fourth;
        fourth.precedent = middle;

        fourth.suivant = last;
        last.precedent = fourth;
    }

    /**
     * Teste l'ajout d'un nouvel élément en tête de la liste.
     */
    @Test
    public void testAddToHead() {
        // Création d'un nouvel élément à ajouter en tête
        Liste newHead = new Liste(new PageId(5, 600));
        Liste exHead = head; // Sauvegarde de l'ancienne tête

        // Ajout en tête
        head = head.add(newHead);

        // Vérifications :
        assertEquals(newHead, head, "Le nouvel élément devrait être la tête de la liste");
        assertEquals(newHead.suivant, exHead, "L'ancien head devrait être le suivant du nouvel head");
        assertNull(newHead.precedent, "Le précédent du nouvel head devrait être null");
        assertEquals(exHead.precedent, newHead, "Le prédécesseur de l'ancien head devrait être le nouvel élément");
    }

    /**
     * Teste la suppression de l'élément en tête dans une liste plus grande.
     */
    @Test
    public void testRemoveFirstElementInLargerList() {
        // Suppression du premier élément
        Liste newHead = head.remove();

        // Vérifications :
        assertEquals(newHead, head.suivant, "Le nouvel head devrait être le second élément initial");
        assertNull(newHead.precedent, "Le prédécesseur du nouvel head devrait être null");
        assertEquals(newHead.suivant.id, middle.id, "Le suivant du nouvel head devrait être l'élément intermédiaire");
    }

    /**
     * Teste la suppression de l'élément en queue dans une liste plus grande.
     */
    @Test
    public void testRemoveLastElementInLargerList() {
        // Suppression du dernier élément
        Liste newLast = last.remove();

        // Vérifications :
        assertEquals(newLast, middle.suivant, "Le nouvel élément de fin devrait être l'avant-dernier élément initial");
        assertNull(newLast.suivant, "Le suivant du nouvel élément de fin devrait être null");
        assertEquals(newLast.precedent.id, middle.id, "Le prédécesseur du nouvel élément de fin devrait être le milieu");
    }

    /**
     * Teste l'ajout successif de plusieurs éléments en tête.
     */
    @Test
    public void testAddMultipleToHead() {
        // Ajouter plusieurs éléments en tête
        for (int i = 6; i <= 10; i++) {
            head = head.add(new Liste(new PageId(i, i * 100)));
        }

        // Vérifier que la taille est correcte en parcourant la liste
        int count = 0;
        Liste current = head;
        while (current != null) {
            count++;
            current = current.suivant;
        }

        assertEquals(10, count, "La liste devrait contenir 10 éléments après ajout de plusieurs éléments en tête.");
    }

    /**
     * Teste la suppression de tous les éléments un par un depuis la tête.
     */
    @Test
    public void testRemoveAllFromHead() {
        // Supprimer tous les éléments depuis la tête
        Liste current = head;
        int count = 0;

        while (current != null) {
            current = current.remove();
            count++;
        }

        // Vérifier que tous les éléments ont été supprimés
        assertNull(current, "La liste devrait être vide après suppression de tous les éléments.");
        assertEquals(5, count, "Le nombre d'éléments supprimés devrait être égal à 5.");
    }

    /**
     * Teste la suppression de tous les éléments un par un depuis la queue.
     */
    @Test
    public void testRemoveAllFromTail() {
        // Supprimer tous les éléments depuis la queue
        Liste current = last;
        int count = 0;

        while (current != null) {
            current = current.remove();
            count++;
        }

        // Vérifier que tous les éléments ont été supprimés
        assertNull(current, "La liste devrait être vide après suppression de tous les éléments.");
        assertEquals(5, count, "Le nombre d'éléments supprimés devrait être égal à 5.");
    }
}