import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;

public class TestAVL{

    private static AVL arbre;

    @BeforeEach
    public void setUp(){
        arbre = new AVL();
    }

    @Test
    public void testInsertAndSearch(){
        // Insertion
        for(int i = 0; i < 10; i++){
            arbre.insert(new AVLNode(new PageId(0, i * 2), ByteBuffer.allocate(10)));
        }

        // Vérification de la recherche
        for(int i = 0; i < 10; i++){
            AVLNode node = arbre.search(new PageId(0, i * 2));
            assertNotNull(node, "Le noeud " + i * 2 + " devrait être trouvé.");
        }

        // Vérification de l'équilibre de l'arbre après insertion
        assertTrue(isBalanced(arbre), "L'arbre doit être équilibré après les insertions.");
    }

    @Test
    public void testDelete(){
        // Insertion des noeuds dans l'arbre
        for(int i = 0; i < 100; i++){
            arbre.insert(new AVLNode(new PageId(0, i * 2), ByteBuffer.allocate(10)));
        }

        // Capture de l'état initial des buffers des noeuds à supprimer
        ByteBuffer[] deletedBuffers = new ByteBuffer[50];
        for(int i = 0; i < 50; i++){
            AVLNode node = arbre.search(new PageId(0, i * 2));
            deletedBuffers[i] = node.buffer;  // Capturer le buffer du noeud à supprimer
        }

        // Vérification que le buffer retourné lors de la suppression est celui du premier noeud supprimé
        for(int i = 0; i < 50; i++){
            // Vérifie que le buffer retourné lors de la suppression correspond bien à celui du nœud supprimé
            ByteBuffer bufferReturned = arbre.delete(new PageId(0, i * 2)); // Supprimer un autre noeud

            assertEquals(deletedBuffers[i], bufferReturned, "Le buffer retourné pour le noeud " + i * 2 + " doit être le même que celui capturé.");
        }

        // Vérification des suppressions (on s'assure que les noeuds sont supprimés)
        for(int i = 0; i < 50; i++){
            AVLNode node = arbre.search(new PageId(0, i * 2));
            assertNull(node, "Le noeud " + i * 2 + " devrait avoir été supprimé.");
        }

        // Vérification des noeuds restants (on s'assure que les noeuds restants sont encore présents)
        for(int i = 50; i < 100; i++){
            AVLNode node = arbre.search(new PageId(0, i * 2));
            assertNotNull(node, "Le noeud " + i * 2 + " devrait être trouvé.");
        }

        // Vérification de l'équilibre après suppression
        assertTrue(isBalanced(arbre), "L'arbre doit être équilibré après les suppressions.");
    }

    @Test
    public void testEmptyTree(){
        // Vérification de la recherche dans un arbre vide
        AVLNode node = arbre.search(new PageId(0, 2));
        assertNull(node, "La recherche dans un arbre vide devrait retourner null.");
        
        // Vérification de la suppression dans un arbre vide
        ByteBuffer buffer = arbre.delete(new PageId(0, 2));
        assertNull(buffer, "La suppression dans un arbre vide devrait retourner null.");
    }    

    @Test
    public void testNodeBalanceFactor(){
        // Insertion qui déséquilibre l'arbre
        arbre.insert(new AVLNode(new PageId(0, 10), ByteBuffer.allocate(10)));
        arbre.insert(new AVLNode(new PageId(0, 20), ByteBuffer.allocate(10)));
        arbre.insert(new AVLNode(new PageId(0, 30), ByteBuffer.allocate(10))); // Cela doit provoquer un déséquilibre

        // Vérification de l'équilibre de l'arbre après déséquilibre
        assertTrue(isBalanced(arbre), "L'arbre doit être équilibré après la réorganisation.");
    }

    // Méthode utilitaire pour vérifier si l'arbre est équilibré
    private boolean isBalanced(AVL arbre){
        return isBalancedNode(arbre.search(new PageId(0, 0)));
    }

    private boolean isBalancedNode(AVLNode node){
        if(node == null)
            return true;

        int balanceFactor = arbre.getHeight(node.left) - arbre.getHeight(node.right);

        return Math.abs(balanceFactor) <= 1 && isBalancedNode(node.left) && isBalancedNode(node.right);
    }
}