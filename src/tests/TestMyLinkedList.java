import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TestMyLinkedList {

    private MyLinkedList<PageId> liste;  // Spécification du type générique MyLinkedList<PageId>
    private PageId[] pagesId;

    @BeforeEach
    void setUp() {
        // Initialisation avant chaque test
        liste = new MyLinkedList<>();  // Initialisation de la liste générique avec PageId
        pagesId = new PageId[100000];  // Crée un tableau de 1000 PageId fictifs

        // Initialisation des 100 000 éléments fictifs
        for (int i = 0; i < pagesId.length; i++)
            pagesId[i] = new PageId(i, i);  // Crée des PageId avec des valeurs uniques
    }

    // Test pour ajouter un grand nombre d'éléments et vérifier la taille de la liste
    @Test
    void testAddLargeNumberOfElements() {
        for (PageId page : pagesId)
            liste.add(page);

        assertFalse(liste.isEmpty(), "La liste ne devrait pas être vide après l'ajout de 1000 éléments.");
        assertEquals(pagesId.length, liste.size(), "La taille de la liste devrait être de 1000 éléments.");

        // Parcours de la liste pour vérifier chaque élément
        MyLinkedList.Cellule<PageId> current = liste.getRoot().getSuivant();  // On commence après la sentinelle
        for (int i = 0; i < pagesId.length; i++) {
            // Vérifier que la valeur de l'élément actuel correspond à celle attendue dans `pagesId`
            assertEquals(pagesId[i], current.getValue(), "L'élément à l'indice " + i + " ne correspond pas à la valeur attendue.");

            // Passer au noeud suivant dans la liste
            current = current.getSuivant();
        }
        // Vérifier que le dernier élément ajouté est bien celui pointé par `tail`
        assertEquals(pagesId[pagesId.length - 1], liste.getTail().getValue(), "Le tail devrait pointer vers le dernier élément ajouté.");
    }

    @Test
    void testGet() {
        // Ajouter un grand nombre d'éléments
        for (PageId page : pagesId)
            liste.add(page);

        // Tester la récupération d'un élément existant
        PageId existingPageId = pagesId[pagesId.length - 1];
        assertNotNull(liste.get(existingPageId), "La méthode get doit retourner un élément existant.");
        assertEquals(existingPageId, liste.get(existingPageId).getValue(), "L'élément récupéré doit avoir la même valeur.");

        // Tester la récupération d'un élément inexistant
        PageId nonExistingPageId = new PageId(99999, 0);  // Une valeur qui n'est pas dans la liste
        assertNull(liste.get(nonExistingPageId), "La méthode get doit retourner null pour un élément inexistant.");
    }

    // Test pour supprimer tous les éléments d'une grande liste et vérifier si elle est vide ensuite
    @Test
    void testRemoveLargeNumberOfElements() {
        // Ajouter un grand nombre d'éléments
        for (PageId page : pagesId)
            liste.add(page);
    
        // On garde une référence du dernier élément ajouté pour la vérification initiale de tail
        PageId lastPageId = pagesId[pagesId.length - 1];
    
        // Vérifier que tail pointe sur le dernier élément ajouté avant de commencer la suppression
        assertEquals(lastPageId, liste.getTail().getValue(), "Le tail devrait pointer vers le dernier élément ajouté.");
    
        // Suppression des éléments un par un
        for (int i = 0; i < pagesId.length; i++) {
            // Sauvegarder le premier élément attendu avant suppression
            PageId firstPageId = liste.getRoot().getSuivant().getValue(); // Le premier élément actuel de la liste
    
            // Effectuer la suppression
            liste.remove();
    
            // Vérifier que le premier élément a bien changé, sauf lorsque la liste devient vide
            if (i < pagesId.length - 1) { // Tant qu'il reste des éléments
                assertNotNull(liste.getRoot().getSuivant(), "Il devrait y avoir un premier élément après la suppression.");
                assertNotEquals(firstPageId, liste.getRoot().getSuivant().getValue(), "Le premier élément devrait avoir changé après suppression.");
            }
    
            // Vérifier le comportement de `tail`
            if (i < pagesId.length - 1)
                // Si la liste n'est pas vide, le tail doit toujours pointer sur le dernier élément ajouté
                assertEquals(lastPageId, liste.getTail().getValue(), "Le tail ne devrait pas changer avant que la liste ne soit vide.");
            else {
                // Une fois la liste vide, le tail doit pointer sur la sentinelle, dont la valeur est `null`
                assertNull(liste.getTail().getValue(), "Le tail doit pointer vers la sentinelle une fois que la liste est vide.");
                assertEquals(liste.getRoot(), liste.getTail());
            }
        }
    
        // Vérifier que la liste est vide à la fin
        assertTrue(liste.isEmpty(), "La liste devrait être vide après la suppression de tous les éléments.");
    }

    @Test
    void testRemoveByValue() {
        // Ajouter un grand nombre d'éléments
        for (PageId page : pagesId)
            liste.add(page);

        // On garde une référence du premier et du dernier élément ajoutés pour la vérification
        PageId firstPageId = pagesId[0];
        PageId lastPageId = pagesId[pagesId.length - 1];

        // Vérifier que root pointe sur le premier élément
        assertEquals(firstPageId, liste.getRoot().getSuivant().getValue(), "Le root doit pointer vers le premier élément ajouté.");
        // Vérifier que tail pointe vers le dernier élément
        assertEquals(lastPageId, liste.getTail().getValue(), "Le tail doit pointer vers le dernier élément ajouté.");

        // Supprimer le premier élément
        liste.remove(firstPageId);

        // Vérifier que le premier élément a été supprimé et que root a changé
        assertNotEquals(firstPageId, liste.getRoot().getSuivant().getValue(), "Le premier élément devrait avoir changé après suppression.");
        
        // Supprimer le dernier élément
        liste.remove(lastPageId);
        // Vérifier que tail a été mis à jour et ne pointe plus vers le dernier élément
        assertNotEquals(lastPageId, liste.getTail().getValue(), "Le tail doit avoir changé après suppression.");

        // Vérifier que la liste est correcte après la suppression des éléments
        assertFalse(liste.isEmpty(), "La liste ne devrait pas être vide après la suppression.");
        assertEquals(pagesId.length - 2, liste.size(), "La taille de la liste devrait être réduite de 2 éléments.");
    }

    @Test
    void testRemoveFromTail() {
        // Ajouter un grand nombre d'éléments
        for (PageId page : pagesId)
            liste.add(page);

        // On garde une référence du premier élément ajouté pour la vérification initiale de root
        PageId firstPageId = pagesId[0];

        // Vérifier que root pointe sur le premier élément ajouté
        assertEquals(firstPageId, liste.getRoot().getSuivant().getValue(), "Le root doit pointer vers le premier élément ajouté.");

        // Suppression des éléments un par un en commençant par le tail
        for (int i = pagesId.length - 1; i >= 0; i--) {
            // Sauvegarder la valeur actuelle de `tail`
            PageId currentTailId = liste.getTail().getValue();

            // Effectuer la suppression
            liste.removeFromTail();

            // Vérifier que `tail` a changé, sauf lorsque la liste devient vide
            if (i > 0) {
                assertNotNull(liste.getTail(), "Le tail ne devrait pas être null tant que la liste n'est pas vide.");
                assertNotEquals(currentTailId, liste.getTail().getValue(), "Le tail doit avoir changé après la suppression.");
            } else {
                // Une fois la liste vide, `tail` doit pointer vers la sentinelle (dont la valeur est `null`)
                assertNull(liste.getTail().getValue(), "Le tail doit pointer vers la sentinelle une fois que la liste est vide.");
                assertEquals(liste.getRoot(), liste.getTail());
            }
        }

        // Vérifier que la liste est vide à la fin
        assertTrue(liste.isEmpty(), "La liste devrait être vide après la suppression de tous les éléments.");
    }

    // Test pour vider la liste puis la remplir à nouveau, en vérifiant son état à chaque étape
    @Test
    void testClearAndRefill() {
        // 1.Test avec remove()
        // Ajouter des éléments
        for (PageId page : pagesId)
            liste.add(page);

        assertFalse(liste.isEmpty(), "La liste ne devrait pas être vide après l'ajout des éléments.");
        assertEquals(pagesId.length, liste.size(), "La taille de la liste après l'ajout de 1000 éléments devrait être 1000.");

        // Vider la liste
        for (int i = 0; i < pagesId.length; i++)
            liste.remove();

        assertTrue(liste.isEmpty(), "La liste devrait être vide après la suppression de tous les éléments.");

        // Remplir à nouveau la liste
        for (PageId page : pagesId)
            liste.add(page);

        assertFalse(liste.isEmpty(), "La liste ne devrait pas être vide après avoir été remplie à nouveau.");
        assertEquals(pagesId.length, liste.size(), "La taille de la liste après l'ajout de 1000 éléments devrait être 1000.");

        liste.clear();

        // 2.Test avec remove(obj)
        // Ajouter des éléments
        for (PageId page : pagesId)
            liste.add(page);

        assertFalse(liste.isEmpty(), "La liste ne devrait pas être vide après l'ajout des éléments.");
        assertEquals(pagesId.length, liste.size(), "La taille de la liste après l'ajout de 1000 éléments devrait être 1000.");

        // Vider la liste
        for (int i = 0; i < pagesId.length; i++)
            liste.remove(pagesId[i]);

        assertTrue(liste.isEmpty(), "La liste devrait être vide après la suppression de tous les éléments.");

        // Remplir à nouveau la liste
        for (PageId page : pagesId)
            liste.add(page);

        assertFalse(liste.isEmpty(), "La liste ne devrait pas être vide après avoir été remplie à nouveau.");
        assertEquals(pagesId.length, liste.size(), "La taille de la liste après l'ajout de 1000 éléments devrait être 1000.");

        liste.clear();

        // 3.Test avec removeFromTail
                // Ajouter des éléments
        for (PageId page : pagesId)
            liste.add(page);

        assertFalse(liste.isEmpty(), "La liste ne devrait pas être vide après l'ajout des éléments.");
        assertEquals(pagesId.length, liste.size(), "La taille de la liste après l'ajout de 1000 éléments devrait être 1000.");

        // Vider la liste
        for (int i = 0; i < pagesId.length; i++)
            liste.removeFromTail();

        assertTrue(liste.isEmpty(), "La liste devrait être vide après la suppression de tous les éléments.");

        // Remplir à nouveau la liste
        for (PageId page : pagesId)
            liste.add(page);

        assertFalse(liste.isEmpty(), "La liste ne devrait pas être vide après avoir été remplie à nouveau.");
        assertEquals(pagesId.length, liste.size(), "La taille de la liste après l'ajout de 1000 éléments devrait être 1000.");
    }

    // Test pour vérifier que l'ordre des éléments est respecté après l'ajout de nombreux éléments
    @Test
    void testOrderAfterAdditions() {
        for (PageId page : pagesId)
            liste.add(page);

        MyLinkedList.Cellule<PageId> current = liste.getRoot().getSuivant();
        for (int i = 0; i < pagesId.length; i++) {
            assertNotNull(current, "La cellule ne devrait pas être nulle.");
            assertEquals(pagesId[i], current.getValue(), "L'ordre des éléments ne devrait pas être modifié.");
            current = current.getSuivant();
        }
    }

    // Test pour ajouter puis supprimer de manière aléatoire et vérifier l'intégrité de la liste
    @Test
    void testAddRemoveRandomly() {
        for (PageId page : pagesId)
            liste.add(page);

        assertEquals(pagesId.length, liste.size(), "La taille initiale de la liste devrait être de 1000.");

        // Suppression aléatoire d'éléments
        for (int i = 0; i < (int)Math.floor(pagesId.length/2); i++)
            liste.remove();  // Suppression des n/2 premiers éléments

        assertEquals((int)Math.floor(pagesId.length/2), liste.size(), "La taille de la liste après suppression de 500 éléments devrait être de 500.");

        // Vérification de l'ordre des éléments restants
        MyLinkedList.Cellule<PageId> current = liste.getRoot().getSuivant();
        for (int i = (int)Math.floor(pagesId.length/2); i < pagesId.length; i++) {
            assertNotNull(current, "La cellule ne devrait pas être nulle.");
            assertEquals(pagesId[i], current.getValue(), "L'ordre des éléments restants ne devrait pas être modifié.");
            current = current.getSuivant();
        }
    }
}