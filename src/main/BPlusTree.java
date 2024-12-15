import java.util.ArrayList;
import java.util.List;

/**
 * Super classe abstraite répresentant un noeud d'un B+Tree. Un noeud peut être une feuille ou une branche (noeud intermédiaire)
 */
abstract class BPlusTreeNode {
	// Liste des entrées du noeud
	private List<Object> rec;
	// Père du noeud
	private BPlusTreeBranch pere;
	
	/**
	 * Calcule la taille du noeud (nombre d'entrées)
	 * 
	 * @return la taille du noeud
	 */
	public int getTaille() {
		return rec.size();
	}
	
	/**
	 * Récupère le noeud père du noeud actuel
	 * 
	 * @return le noeud père du noeud actuel
	 */
	public BPlusTreeBranch getPere() {
		return pere;
	}
	
	/**
	 * Ajoute ou change le père du noeud actuel
	 * 
	 * @param pere Nouveau noeud père du noeud actuel
	 */
	public void setPere(BPlusTreeBranch pere){
		this.pere = pere;
	}
	/**
	 * Récupère un des fils du noeud
	 * 
	 * @param index le numéro du fils
	 * @return le fils demandé
	 */
	public abstract BPlusTreeNode getFils(int index);
	
	/**
	 * Récupère une des clés du noeud
	 * 
	 * @param index indice de la clé
	 * @return la clé demandée
	 */
	public abstract Comparable<Object> getCle(int index);
	
	/**
	 * Scinde le noeud en deux si le noeud est trop rempli
	 * 
	 * @param index indice où la séparation des entrées se fait
	 * @return Un nouveau noeud contenant une partie des entrées du précédent
	 */
	public abstract BPlusTreeNode split(int index);
}

/**
 * Classe représentant une feuille d'un B+Tree
 */
class BPlusTreeLeaf extends BPlusTreeNode {
	// Liste des entrées de la Feuille
	// Les entrées sont de format Alternative 3 (clé, liste de Rids)
	private List<Pair<Comparable<Object>, ArrayList<RecordId>>> rec;
	// Père de la feuille, il s'agit d'une branche
	private BPlusTreeBranch pere;
	
	/**
	 *  Constructeur d'un feuille vide
	 */
	public BPlusTreeLeaf() {
		rec = new ArrayList<>();
		this.pere = null;
	}
	
	/**
	 * Constructeur d'une feuille à partir d'une liste d'entrées
	 */
	public BPlusTreeLeaf(List<Pair<Comparable<Object>, ArrayList<RecordId>>> rec) {
		this.rec = rec;
	}

	/**
	 * Récupère la liste des recordId d'une certaine clé
	 * 
	 * @return la liste des recordId de la clé ou null si la feuille ne contient pas cette clé
	 */
	public ArrayList<RecordId> getRecordId(Comparable<Object> element){
		for(Pair<Comparable<Object>, ArrayList<RecordId>> entree : rec) {
			// Si la clé correspond à celle cherchée
			if(entree.getFirst().compareTo(element) == 0) {
				return entree.getSecond();
			}
		}
		return null;
	}
	
	public void addEntrees(List<Pair<Comparable<Object>, ArrayList<RecordId>>> entrees) {
		rec.addAll(entrees);
	}
	
	/**
	 * Ajoute une entrée dans la feuille
	 * 
	 * @param cle Clé de l'entrée
	 * @param reference RecordId correspondant à cette clé
	 */
	public void addEntree(Comparable<Object> cle, RecordId reference) {
		int i = 0;
		boolean insere = false;
		// Parcourt la liste des entrée de la feuille pour trouver où placer la nouvelle entrée en respectant l'ordre croissant
		while(!insere && i<rec.size()) {
			// Resultat de la comparaison entre la clé de la nouvelle entrée et celle de la feuille inspectée
			int comp = cle.compareTo(rec.get(i).getFirst());
			// Si les clé sont égales, ajoute le RecordId à la liste de cette clé
			if(comp == 0) {
				rec.get(i).getSecond().add(reference);
				insere = true;
			// Si la clé est plus petite que cette entrée, l'insère à sa place
			// Si la boucle n'a pas fini à l'itération précédente, la clé est donc forcément supérieur à l'entrée précédente de la feuille
			}else if(comp < 0) {
				ArrayList<RecordId> listeRef = new ArrayList<>();
				listeRef.add(reference);
				rec.add(i, new Pair<Comparable<Object>, ArrayList<RecordId>>(cle, listeRef));
				insere = true;
			}
			i++;
		}
		// Si l'emplacement pour insérer la nouvelle entrée n'a pas été trouvé alors elle plus grande que toutes les autres
		// L'insère donc à la fin de la liste
		if (!insere) {
			ArrayList<RecordId> listeRef = new ArrayList<>();
			listeRef.add(reference);
			rec.add(new Pair<Comparable<Object>, ArrayList<RecordId>>(cle, listeRef));
		}
	}
	
	//Méthodes de Node
	public Comparable<Object> getCle(int index){
		// Si la feuille est vide, elle ne contient pas la clé
		if (rec.size() == 0) {
			return null;
		}
		return rec.get(index).getFirst();
	}
	
	public BPlusTreeNode getFils(int index) {
		// Une feuille n'a pas de fils
		return null;
	}
	
	public BPlusTreeLeaf split(int index) {
		List<Pair<Comparable<Object>, ArrayList<RecordId>>> firstHalf = rec.subList(0, index);
		List<Pair<Comparable<Object>, ArrayList<RecordId>>> lastHalf = rec.subList(index, rec.size());
		rec = firstHalf;
		return new BPlusTreeLeaf(lastHalf);
	}
}

/**
 * Classe correspondant à une branche ou noeud intermédiaire
 */
class BPlusTreeBranch extends BPlusTreeNode {
	// Liste des fils de ce noeud
	private List<BPlusTreeNode> fils;
	// Liste des entrées du noeud
	private List<Comparable<Object>> rec;
	// Branche père de ce noeud, null si le noeud est racine
	private BPlusTreeBranch pere;
	
	/**
	 * Constructeur de noeud à un élément (un noeud ne peut être vide)
	 * 
	 * @param element Première clé du noeud
	 * @param fils1 Noeuds fils dont les entrées sont inférieures à la clé
	 * @param fils2 Noeuds fils dont les entrées sont supérieures à la clé
	 * @throws IllegalArgumentException si les fils ne respectent pas les conditions
	 */
	public BPlusTreeBranch(Comparable<Object> element, BPlusTreeNode fils1, BPlusTreeNode fils2) throws IllegalArgumentException{
		if(fils1.getCle(0).compareTo(fils2.getCle(0)) > 0) {
			throw new IllegalArgumentException("Les fils ne sont pas ordonnés");
		}
		fils = new ArrayList<>();
		rec = new ArrayList<>();
		rec.add(element);
	}
	
	/**
	 * Constructeur de noeud à l'aide d'une liste de fils et d'une liste d'entrées
	 * 
	 * @param fils Liste des fils du nouveau noeud
	 * @param rec Liste des entrées du nouveau noeud
	 * @throws IllegalArgumentException si il n'y a pas un fils de plus qu'il n'y a d'entrées
	 */
	public BPlusTreeBranch(List<BPlusTreeNode> fils, List<Comparable<Object>> rec) throws IllegalArgumentException{
		if(fils.size()!=rec.size()+1) {
			throw new IllegalArgumentException("Les proportions de pointeurs et de clés ne sont pas respectées");
		}
		this.fils = fils;
		this.rec = rec;
	}
	
	/**
	 * Ajoute un fils (et donc une clé) au noeud
	 * 
	 * @param cle Clé de l'entrée à ajouter
	 * @param noeud Noeud fils à ajouter
	 */
	public void addFils(Comparable<Object> cle, BPlusTreeNode noeud) {
		int i = 0;
		boolean insere = false;
		// Parcourt la liste pour trouver l'emplacement où insérer le fils en respectant l'ordre du noeu
		while(!insere && i<rec.size()) {
			// Résultat de la comparaison entre la clé de la nouvelle entrée et celle inspectée
			int comp = cle.compareTo(rec.get(i));
			// Si la clé est plus petite que cette entrée, l'insère à sa place
			// Si la boucle n'a pas fini à l'itération précédente, la clé est donc forcément supérieur à l'entrée précédente du noeud		
			if(comp < 0) {
				rec.add(i, cle);
				//Le fils est toujours un indice après sa clé
				fils.add(i+1, noeud);
				insere = true;
			}
			i++;
		}
		// Si l'emplacement pour insérer la nouvelle entrée n'a pas été trouvé alors elle plus grande que toutes les autres
		// L'insère donc à la fin de la liste
		if(!insere) {
			rec.add(cle);
			fils.add(noeud);
		}
	}

	//Méthodes de Node
	public Comparable<Object> getCle(int index){
		return rec.get(index);
	}
	
	public BPlusTreeNode getFils(int index) {
		return fils.get(index);
	}
	
	public BPlusTreeBranch split(int index) {
		List<Comparable<Object>> firstHalfRec = rec.subList(0, index);
		List<Comparable<Object>> lastHalfRec = rec.subList(index + 1, rec.size());
		List<BPlusTreeNode> firstHalfFils = fils.subList(0, index +1);
		List<BPlusTreeNode> lastHalfFils = fils.subList(index+1, rec.size());
		rec = firstHalfRec;
		fils = firstHalfFils;
		return new BPlusTreeBranch(lastHalfFils, lastHalfRec);
	}
}

/**
 * Classe Représentant un B+Tree
 */
public class BPlusTree {
	// Noeud racine du B+Tree, peut être une feuille
	private BPlusTreeNode racine;
	// Ordre du BPlusTree
	private int ordre;
	
	/**
	 *  Constructeur du BPlusTree vide
	 *  
	 * @param ordre Ordre du B+Tree
	 */
	public BPlusTree(int ordre) {
		this.ordre = ordre;
		racine = new BPlusTreeLeaf();
	}
	
	/**
	 *  Constructeur d'un B+Tree à l'aide d'un Bulk loading
	 *  
	 * @param listeEntree Liste des entrées du B+Tree
	 * @param ordre Ordre du B+Tree
	 */
	public BPlusTree(ArrayList<Pair<Comparable<Object>, RecordId>> listeEntree, int ordre) {
		this.ordre = ordre;
		ArrayList<Pair<Comparable<Object>, ArrayList<RecordId>>> listeTriee = tri(listeEntree);
		ajouterFeuilles(listeTriee);
	}
	
	/**
	 * Insère une entrée dans le B+Tree
	 * 
	 * @param cle Clé de l'entrée à insérer
	 * @param reference RecordId de l'entrée à insérer
	 */
	public void addRecord(Comparable<Object> cle, RecordId reference) {
		// Recherche la feuille où insérer l'entrée
		BPlusTreeLeaf feuille = rechercherFeuille(cle, racine);
		// Ajoute l'entrée dans la feuille
		feuille.addEntree(cle, reference);
		// Si la feuille est trop grande
		if(feuille.getTaille() > ordre*2) {
			// Scinde la feuille
			splitFeuille(feuille);
		}
	}
	
	/**
	 * Récupère la liste des RecordId d'une clé
	 * 
	 * @param element Clé à rechercher
	 * @return la liste des RecordId d'une clé
	 */
	public ArrayList<RecordId> getRecordId(Comparable<Object> element) {
		// Recherche la feuille où se trouve la clé
		BPlusTreeLeaf feuille = rechercherFeuille(element, racine);
		return feuille.getRecordId(element);
	}
	
	/**
	 * Tri un liste d'entrée par ordre croissant
	 * 
	 * @param table Liste d'entrée à trier
	 * @return liste d'entrée triée par ordre croissant
	 */
	public ArrayList<Pair<Comparable<Object>, ArrayList<RecordId>>> tri(List<Pair<Comparable<Object>, RecordId>> table){
		// Crée une liste avec le bon format
		ArrayList<Pair<Comparable<Object>, ArrayList<RecordId>>> tableTrie = new ArrayList<>();
		// Ajoute le premier élément de la liste
		ArrayList<RecordId> nouveau = new ArrayList<>();
		nouveau.add(table.get(0).getSecond());
		tableTrie.add(new Pair<>(table.get(0).getFirst(), nouveau));
		
		boolean trouve = false;
		int j= 0;
		// Insère une à une les entrées de la liste
		for(int i=1; i<table.size(); i++) {
			// Entrée en cours d'insertion
			Pair<Comparable<Object>, RecordId> element = table.get(i);
			// Cherche l'emplement où insérer l'entrée
			while(!trouve && j<tableTrie.size()) {
				// Résultat de la comparaison entre l'entrée à insérer et l'entrée inspectée
				int comp = element.getFirst().compareTo(tableTrie.get(j).getFirst());
				// Si elles sont égales, ajoute la référence de l'entrée à insérer dans la liste de références de l'entrée inspectée
				if(comp == 0) {
					tableTrie.get(j).getSecond().add(element.getSecond());
					trouve = true;
					
				// Si l'entrée à insérer est plus petite que l'entrée inspectée, l'insère à sa place
				// Si la boucle n'a pas fini à l'itération précédente, l'entrée est donc forcément supérieur à l'entrée précédente de la liste		
				}else if(comp < 0) {
					nouveau = new ArrayList<>();
					nouveau.add(element.getSecond());
					tableTrie.add(j, new Pair<>(element.getFirst(), nouveau));
					trouve = true;
				}
			}
			
			// Si la clé est plus petite que cette entrée, l'insère à sa place
			// Si la boucle n'a pas fini à l'itération précédente, la clé est donc forcément supérieur à l'entrée précédente de la feuille					
			if(!trouve) {
				nouveau = new ArrayList<>();
				nouveau.add(element.getSecond());
				tableTrie.add(new Pair<>(element.getFirst(), nouveau));
			}
		}
		return tableTrie;
	}
	
	/**
	 * Recherche la feuille contenant ou pouvant contenir une entrée données
	 * 
	 * @param element clé de l'entrée à rechercher
	 * @param depart Noeud de départ de la recherche
	 * @return La feuille contenant ou pouvant contenir l'entrée donnée
	 */
	public BPlusTreeLeaf rechercherFeuille(Comparable<Object> element, BPlusTreeNode depart) {
		// Si c'est une feuille alors cette feuille est la solution
		if(depart.getClass() == BPlusTreeLeaf.class) {
			return (BPlusTreeLeaf) depart;
		}
		// Si c'est un noeud intermédiaire, parcourt le noeud pour trouver le fils concerné par l'entrée
		for(int i =0; i< depart.getTaille(); i++) {
			// Si l'entrée est inférieure à la clé, elle est contenue dans le fils avec le même indice que la clé
			if(depart.getCle(i).compareTo(element) < 0) {
				return rechercherFeuille(element, depart.getFils(i));
			}
		}
		// Si l'entrée ne peut être trouvée dans aucun fils inspectés, elle pourra être trouvée dans le dernier fils
		return rechercherFeuille(element, depart.getFils(depart.getTaille()));
	}
	
	/**
	 * Scinde une feuille trop grande
	 * 
	 * @param feuille Feuille à scinder
	 */
	public void splitFeuille(BPlusTreeLeaf feuille) {
		// Crée une nouvelle feuille en scindant en deux l'ancienne
		BPlusTreeLeaf nouvelleFeuille  = feuille.split(ordre);
		BPlusTreeBranch pere = feuille.getPere();
		// Si le père de la feuille est null alors c'est la racine
		// Crée donc une nouvelle branche qui sera la nouvelle racine
		if(pere == null) {
			try {
				racine = new BPlusTreeBranch(nouvelleFeuille.getCle(0), feuille, nouvelleFeuille);
				// Ajoute la nouvelle racine comme père des deux feuilles
				nouvelleFeuille.setPere((BPlusTreeBranch)racine);
				feuille.setPere((BPlusTreeBranch)racine);
			}catch(IllegalArgumentException e) {
				e.printStackTrace();
			}
		}else {
			// Ajoute la nouvelle feuille comme fils de ce père
			pere.addFils(nouvelleFeuille.getCle(0), nouvelleFeuille);
			// Ajoute le père de la nouvelle feuille
			nouvelleFeuille.setPere(pere);
			// Si le père est trop grand, le scinde
			if(pere.getTaille()>ordre*2) {
				splitNode(pere);
			}
		}
	}
	
	/**
	 * Scinde un noeud intermédiaire lorsqu'il est trop grand
	 * 
	 * @param noeud Branche à scinder
	 */
	public void splitNode(BPlusTreeBranch noeud) {
		// Récupère la clé pivot du noeud
		Comparable<Object> cle = noeud.getCle(ordre);
		// Crée une nouvelle branche en scindant l'ancienne
		BPlusTreeBranch nouvelleBranche = noeud.split(ordre);
		BPlusTreeBranch pere = noeud.getPere();
		// Si le père de la branche est null alors c'est la racine
		// Crée alors une nouvelle racine contenant la clé pivot
		if(pere == null) {
			try {
				racine = new BPlusTreeBranch(cle, noeud, nouvelleBranche);
				// Ajoute la nouvelle racine comme père des deux branches
				nouvelleBranche.setPere((BPlusTreeBranch)racine);
				noeud.setPere((BPlusTreeBranch)racine);
			}catch(IllegalArgumentException e) {
				e.printStackTrace();
			}
		}else {
			// Ajoute la nouvelle Branche comme fils du père de sa voisine
			pere.addFils(cle, nouvelleBranche);
			// Ajoute le père de la nouvelle Branche
			nouvelleBranche.setPere(pere);
			// Si le père est trop grand, le scinde
			if(pere.getTaille()>ordre*2) {
				splitNode(pere);
			}
		}
	}

	/**
	 * Ajouter une liste d'entrée dans un arbre vide
	 * 
	 * @param listeTrie Liste des entrées à ajouter
	 */
	public void ajouterFeuilles(List<Pair<Comparable<Object>, ArrayList<RecordId>>> listeTrie) {
		BPlusTreeLeaf precedente = (BPlusTreeLeaf) racine;
		while(listeTrie.size() != 0) {
			List<Pair<Comparable<Object>, ArrayList<RecordId>>> inserer;
			if(listeTrie.size()> ordre *2) {
				inserer = listeTrie.subList(0, ordre*2);
				listeTrie = listeTrie.subList(ordre*2, listeTrie.size());
			}else {
				inserer = listeTrie;
			}
			if(racine == null) {
				racine = new BPlusTreeLeaf(inserer);
			}else {
				BPlusTreeLeaf nouvelleFeuille = new BPlusTreeLeaf(inserer);
				BPlusTreeBranch pere = precedente.getPere();
				if(pere == null) {
					try {
						racine = new BPlusTreeBranch(nouvelleFeuille.getCle(0), precedente, nouvelleFeuille);
						// Ajoute la nouvelle racine comme père des deux feuilles
						nouvelleFeuille.setPere((BPlusTreeBranch)racine);
						precedente.setPere((BPlusTreeBranch)racine);
					}catch(IllegalArgumentException e) {
						e.printStackTrace();
					}
				}else {
					// Ajoute la nouvelle feuille comme fils de ce père
					pere.addFils(nouvelleFeuille.getCle(0), nouvelleFeuille);
					// Ajoute le père de la nouvelle feuille
					nouvelleFeuille.setPere(pere);
					// Si le père est trop grand, le scinde
					if(pere.getTaille()>ordre*2) {
						splitNode(pere);
					}
				}
				precedente = nouvelleFeuille;
			}
		}
	}
}