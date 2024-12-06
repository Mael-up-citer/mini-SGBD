import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

/**
 * Représente une relation dans une base de données, gérant ses attributs (colonnes), 
 * l'écriture et la lecture d'enregistrements, ainsi que la gestion des pages de données.
 */
public class Relation {
    
    private String relationName;    // Nom de la relation
    private ArrayList<Pair<String, Data>> attribut; // Liste des attributs (nom et type de chaque colonne)
    private PageId headerPageId;    // Identifiant de la première page d'en-tête de la relation
    private PageId LastHeaderPageId;    // Identifiant de la dernière page d'en-tête de la relation
    private DiskManager dskm;       // Gestionnaire de disque pour l'allocation et la gestion des pages
    private BufferManager bm;       // Gestionnaire de buffer pour la gestion des pages en mémoire

    /**
     * Constructeur avec initialisation des attributs.
     * 
     * @param relationName Le nom de la relation.
     * @param attribut La liste des attributs de la relation (nom et type).
     * @param headerPageId L'identifiant de la première page de la relation.
     * @param dsmk Le gestionnaire de disque.
     * @param bm Le gestionnaire de buffer.
     */
    public Relation(String relationName, ArrayList<Pair<String, Data>> attribut, PageId headerPageId, DiskManager dsmk, BufferManager bm) throws IllegalArgumentException{
        // Test si le nom est valide
        if(SGBD.isValidName(relationName))
            this.relationName = relationName.toUpperCase();
        else
            throw new IllegalArgumentException("Impossible de cree l'instance car le nom "+ relationName+"  est invalide");

        this.attribut = new ArrayList<>();

        setAttribut(attribut);  // Appelle le setter pour vérifier et init les attributs
        this.headerPageId = headerPageId;
        this.dskm = dsmk;
        this.bm = bm;
        LastHeaderPageId = headerPageId;    // La dernière header Page est initialisé à la 1er
    }

    /**
     * Écrit un enregistrement dans un buffer à une position donnée.
     * 
     * @param record L'enregistrement à écrire.
     * @param buffer Le buffer dans lequel l'enregistrement sera écrit.
     * @param pos La position de départ dans le buffer pour l'écriture.
     * @return La taille totale de l'enregistrement écrit.
     */
    public int writeRecordToBuffer(MyRecord record, ByteBuffer buffer, int pos) {
        int lgRecord = getNbAttribut();    // Nombre d'attributs dans le record
        int posValue; // Position où la prochaine valeur sera écrite
        int start = pos;    // Position de départ dans le buffer

        try {
            posValue = pos + ((lgRecord + 1) * Integer.BYTES); // La 1ère valeur commence après l'offset directory
            buffer.putInt(pos, posValue);   // Écriture de l'offset de la 1ère valeur
            pos += Integer.BYTES; // Mise à jour de la position

            // Parcourt la liste des attributs et écrit chaque valeur dans le buffer
            for (int i = 0; i < lgRecord; i++) {
                // Traitement du type de données pour chaque attribut
                switch (getType(i)) {
                    case INT:
                        buffer.putInt(posValue, (int) record.getValue(i));
                        posValue += getLength(i); // Mise à jour de la position après l'écriture
                        break;

                    case REAL:
                        buffer.putFloat(posValue, (float) record.getValue(i));
                        posValue += getLength(i); // Mise à jour de la position après l'écriture
                        break;

                    case CHAR:
                    case VARCHAR:
                        String str = (String) record.getValue(i);
                        int nbCharWrite = 0;    // Nombre de caractères écrits

                        for (int j = 0; j < str.length(); j++) {
                            buffer.putChar(posValue, str.charAt(j)); // Écrit caractère par caractère

                            posValue += Character.BYTES;
                            nbCharWrite += 2;  // Incrémente le nombre de caractères écrits
                        }
                        // Si c'est un char on alloue la prochaine val a la fin de la taille de la colonne
                        if(getType(i) == DataType.CHAR)
                            posValue += (getLength(i)*2) - nbCharWrite;  // Calcul du prochain offset *2 car 1 char = 2octets!!!
                        // Sinon la prochaine val peut commencer juste apres la fin de l'écriture de la valeur
                        else
                            posValue += (str.length()*2) - nbCharWrite;  // Calcul du prochain offset *2 car 1 char = 2octets!!!

                        break;

                    case DATE:
                        Date date = (Date)record.getValue(i);
                        // Écriture des valeurs jour, mois, année pour un type DATE
                        buffer.putInt(posValue, date.getDay());
                        posValue += Integer.BYTES;
                        buffer.putInt(posValue, date.getMonth());
                        posValue += Integer.BYTES;
                        buffer.putInt(posValue, date.getYear());
                        posValue += Integer.BYTES;
                        break;

                    default:
                        throw new IllegalArgumentException("Type non pris en charge pour l'écriture.");
                }
                // Mise à jour de l'offset pour la prochaine valeur
                buffer.putInt(pos, posValue);
                pos += Integer.BYTES;  // Mise à jour de la position d'écriture
            }
            return posValue - start; // Retourne la taille totale de l'enregistrement écrit

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;  // En cas d'erreur, retourne 0
    }

    /**
     * Lit un enregistrement à partir d'un buffer à une position donnée.
     * 
     * @param record L'enregistrement à remplir avec les données lues.
     * @param buffer Le buffer depuis lequel l'enregistrement sera lu.
     * @param pos La position de départ dans le buffer pour la lecture.
     * @return La taille totale de l'enregistrement lu.
     */
    public int readRecordFromBuffer(MyRecord record, ByteBuffer buffer, int pos) {
        int lgRelation = getNbAttribut();  // Nombre d'attributs dans la relation
        int posValue = pos + (lgRelation + 1) * Integer.BYTES;  // Première valeur après l'offset directory
        int start = pos;    // Position de départ dans le buffer

        try {
            // Parcourt l'offset directory pour récupérer les valeurs
            for (int i = 0; i < lgRelation; i++) {
                // Traitement du type de données pour chaque attribut
                switch (attribut.get(i).getSecond().getType()) {
                    case INT:
                        record.add(buffer.getInt(posValue), getType(i));
                        posValue += getLength(i); // Mise à jour de la position après lecture
                        break;

                    case REAL:
                        record.add(buffer.getFloat(posValue), getType(i));
                        posValue += getLength(i); // Mise à jour de la position après lecture
                        break;

                    case CHAR:
                    case VARCHAR:
                        // La chaine ce fini au début de la prochaine valeur donc recupere le prochain offset
                        int endOfString = buffer.getInt((pos + (i+1)*Integer.BYTES));
                        StringBuilder str = new StringBuilder();
                        char c;

                        // Parcours du buffer jusqu'a la fin de la chaine
                        while (endOfString > posValue) {
                            c = buffer.getChar(posValue);
                            str.append(c);
                            posValue += Character.BYTES;  // Avance de 2 octets
                        }
                        record.add(str.toString().trim(), getType(i));
                        posValue = endOfString;  // Mise à jour de la position

                        break;

                    case DATE:
                        int day = buffer.getInt(posValue);
                        posValue += Integer.BYTES;  // Avance au prochain
                        int month = buffer.getInt(posValue);
                        posValue += Integer.BYTES;  // Avance au prochain
                        int year = buffer.getInt(posValue);
                        posValue += Integer.BYTES;  // Avance au prochain

                        record.add(new Date(day, month, year), getType(i));
                        break;

                    default:
                        throw new IllegalArgumentException("Type non pris en charge pour la lecture.");
                }
            }
            return posValue - start; // Retourne la taille totale lue du buffer

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;  // En cas d'erreur, retourne 0
    }

    /**
     * Ajoute une nouvelle page de données à la relation.
     * Alloue une nouvelle page, met à jour le nombre de pages dans la page d'en-tête 
     * et ajoute la nouvelle page au répertoire des pages.
     * Si le répertoir des page est plein on lui alloue une nouvelle header Page chainé après la 1er.
     * @return un PageId, celui de la nouvelle data Page
    */
    public PageId addDataPage() {
        try {
            // Alloue une nouvelle page
            PageId id = dskm.AllocPage();
            // Charge la 1er header Page en mémoire
            ByteBuffer buffer = bm.getPage(headerPageId);

            // Calcul des limites des entrées pour chaque type de header Page
            int maxEntriesFirstHeader = (DBConfig.pagesize - 4 - 8) / 12; // Premières 4 octets pour le compteur + 8 pour l'@
            int maxEntriesOtherHeader = (DBConfig.pagesize - 8) / 12; // 8 octets pour le chaînage

            // Récupère et met à jour le nombre total de pages de données
            int nbDataPage = buffer.getInt(0);
            nbDataPage++;   // Incrémente le nombre total de pages
            buffer.putInt(0, nbDataPage); // Mise à jour dans la page d'en-tête

            // Calcule l'offset pour ajouter la nouvelle entrée
            int offset = 4+((nbDataPage-1)*12);

            // Si on a plusieurs header Page on peut libere la 1er
            if (! LastHeaderPageId.equals(headerPageId)) {
                bm.freePage(headerPageId, true);    // Relache la 1er header Page a true
                buffer = bm.getPage(LastHeaderPageId);  // Charge la dernière
                
                // Calcule l'offset pour ajouter la nouvelle entrée
                offset = (nbDataPage - 1) - maxEntriesFirstHeader;
                offset = ((offset % maxEntriesOtherHeader) * 12);
            }
            // Si l'offset est plus grand que la taille réel de la page (taille - chainage - espace d'une dataPage)
            if ((offset > (DBConfig.pagesize - (8+12))) || (offset <= 0)) {
                // Alloue une nouvelle header page
                PageId newHeaderPage = dskm.AllocPage();

                // Ajoute le chaînage vers la nouvelle page
                buffer.putInt(DBConfig.pagesize - 8, newHeaderPage.FileIdx);
                buffer.putInt(DBConfig.pagesize - 4, newHeaderPage.PageIdx);

                // Libère la page actuelle avec indication de modification
                bm.freePage(LastHeaderPageId, true);

                // MAJ la dernière header Page
                LastHeaderPageId = newHeaderPage;

                // Initialise la nouvelle header page
                // Charge dans le buffer
                buffer = bm.getPage(LastHeaderPageId);
                buffer.putInt(DBConfig.pagesize-4, -1); // Indique qu'il n'y a pas de page suivante

                // Remet l'offset à 0 dans la nouvelle page
                offset = 0;
            }
            // Écrit les informations de la nouvelle data page dans la dernière header Page
            buffer.putInt(offset, id.FileIdx);      // Fichier de la nouvelle page
            buffer.putInt(offset + 4, id.PageIdx);  // Index de la nouvelle page
            buffer.putInt(offset + 8, DBConfig.pagesize - 8);  // octets libre -8 pour page directory

            // Libère la dernière header page avec indication de modification
            bm.freePage(LastHeaderPageId, true);

            // Ecrit l'offset directory de la nouvelle data Page
            // Charge la nouvelle dataPage
            buffer = bm.getPage(id);

            buffer.putInt(DBConfig.pagesize - 4, 0);    // Ecrit le début de l'espace disponible
            buffer.putInt(DBConfig.pagesize - 8, 0);    // Ecrit le nombre de slot dedans
            bm.freePage(id, true);  // Libere la page

            return id;
        } catch (Exception e) {
            e.printStackTrace();
            // Gestion d'erreur : impression de la trace et arrêt du processus
            throw new RuntimeException("Erreur lors de l'ajout d'une page de données", e);
        }
    }

    /**
     * Récupère tous les identifiants de pages de données libre de la base de données.
     *
     * @return Une liste d'objets PageId représentant toutes les pages de données.
    */
    public List<PageId> getFreeDataPages() {
        // Initialiser la liste pour stocker les PageIds
        List<PageId> pageIds = new ArrayList<>();
        PageId currentPage = new PageId();  // Contient l'id de la header Page courrente
        int offset = 4;    // Premiers 4 octets pour le compteur + 8 pour l'@ de la 1er dataPage

        try {
            // Charger la 1er Header Page en mémoire
            ByteBuffer buffer = bm.getPage(headerPageId);
            
            // Récupère le nombre de page référencé
            int nbDataPage = buffer.getInt(0);

            currentPage = headerPageId; // Affecte  currentPage a la 1er header Page

            // tantque on n'a pas toute les pages
            while (pageIds.size() < nbDataPage) {
                // Parcourir chaque entrée d'une header Page pour lire les PageId
                while ((pageIds.size() < nbDataPage) && (offset <= DBConfig.pagesize - (8+12))) {
                    // Si la data Page est vide (8 pour le slot directory)
                    if (buffer.getInt(offset+8) <= (DBConfig.pagesize - 8)) {
                        // Lire le fileIdx et le pageIdx de l'entrée actuelle
                        int fileIdx = buffer.getInt(offset);
                        int pageIdx = buffer.getInt(offset + 4);

                        // Créer un PageId avec fileIdx et pageIdx, puis l'ajouter à la liste
                        pageIds.add(new PageId(fileIdx, pageIdx));
                    }
                    offset += 12;   // Avance dans la page de 8 pour sauter le pointage vers la data page
                }
                // Crée une nouvelle instance temporaire pour stocker la prochaine page
                PageId tempNextPage = new PageId(
                    buffer.getInt(DBConfig.pagesize - 8),
                    buffer.getInt(DBConfig.pagesize - 4)
                );
                // Libère la header Page courante
                bm.freePage(currentPage, false);

                // Si il y a du chainage
                if (tempNextPage.PageIdx != -1) {
                    currentPage = tempNextPage;
                    // charge la header page chainé suivante
                    buffer = bm.getPage(currentPage);
                    offset = 0; // RAZ l'offset
                }
                // Sinon c'est la fin de la boucle
            }
        } catch(Exception e){
            e.printStackTrace();
            bm.freePage(currentPage, false); // En cas d'erreur libere la header Page de travail
        }
        // Retourner la liste des PageIds
        return pageIds;
    }

    /**
     * Récupère tous les identifiants de pages de données de la base de données.
     *
     * @return Une liste d'objets PageId représentant toutes les pages de données.
     * @throws Exception Si une erreur se produit lors de la récupération des pages.
    */
    public List<PageId> getDataPages() throws Exception {
        // Initialiser la liste pour stocker les PageIds
        List<PageId> pageIds = new ArrayList<>();
        // Contient la header Page courante
        PageId currentPage = new PageId();
        currentPage = headerPageId; // Initialise à la 1er header page
        int offset = 4;  // Premiers 4 octets pour le nombre de data Page

        try {
            // Charger la 1er Header Page en mémoire
            ByteBuffer buffer = bm.getPage(headerPageId);
            // Recupere le nombre de data Page
            int nbDataPage = buffer.getInt(0);

            // tantqu'on a pas toutes les data Page
            while (pageIds.size() < nbDataPage) {

                //System.out.println("offset = "+offset);

                // Parcourir chaque entrée d'une header Page pour lire les PageId
                while ((pageIds.size() < nbDataPage) && (offset <= DBConfig.pagesize-(8+12))) {
                    // Lire le fileIdx et le pageIdx de l'entrée actuelle
                    int fileIdx = buffer.getInt(offset);
                    int pageIdx = buffer.getInt(offset + 4);

                    // Créer un PageId avec fileIdx et pageIdx, puis l'ajouter à la liste
                    pageIds.add(new PageId(fileIdx, pageIdx));

                    offset += 12;   // Avance dans la page de 4 pour sauter l'espace libre coder sur 4octets
                }
                // Crée une nouvelle instance temporaire pour stocker la prochaine page
                PageId tempNextPage = new PageId(
                    buffer.getInt(DBConfig.pagesize - 8),
                    buffer.getInt(DBConfig.pagesize - 4)
                );
                // Libère l'ancienne valeur
                bm.freePage(currentPage, false);
                // Passe à la page suivante
                currentPage = tempNextPage;

                // Si il y a du chainage
                if(tempNextPage.PageIdx != -1)
                    // charge la header page chainé suivante
                    buffer = bm.getPage(currentPage);

                offset = 0; // RAZ l'offset
            }
        } catch(Exception e) {
            e.printStackTrace();
            // Libérer la page courrante en cas d'excpetion
            bm.freePage(currentPage, false); // False car pas de modification
        }
        // Retourner la liste des PageIds
        return pageIds;
    }

/**
     * Insère un enregistrement dans la table. Si une page de données
     * avec suffisamment d'espace est trouvée, l'enregistrement est inséré dans celle-ci.
     * Sinon, une nouvelle page est ajoutée à la base de données.
     *
     * @param record L'enregistrement à insérer dans la base de données.
     * @return Un RecordId représentant l'identifiant unique de l'enregistrement inséré.
     * @throws Exception Si une erreur se produit lors de l'insertion de l'enregistrement.
    */
    public RecordId InsertRecord(MyRecord record) throws Exception {
        // Calculer la taille du record son offset compris
        int recordSize = record.getSizeOctet(this) + ((attribut.size()+1) * 4);   // taille du record + la taille de son l'offset directory

        System.out.println("le record à inserer occupe "+recordSize+" octets");

        // Contient la header Page courante
        PageId currentPage = new PageId();

        // Contient le pageId de la data page ou l'on insère le record
        PageId dataPageId = null;

        // header Page courante initialisé avec la 1er header Page
        currentPage.FileIdx = headerPageId.FileIdx;
        currentPage.PageIdx = headerPageId.PageIdx;

        // offset de l'espace libre d'une data page dans sa header Page
        int offset = 12;

        try {
            /*

            // Charge la 1er Header Page en mémoire
            ByteBuffer buffer = bm.getPage(headerPageId);
            // Récupère le nombre de data Page
            int nbDataPage = buffer.getInt(0);
            int cpt = 0;    // Compte combien de data Page on été essayé

            System.out.println("nombre de data Page =  "+nbDataPage);
            System.out.println("1rst header Page =  "+headerPageId);
            System.out.println("last header Page =  "+LastHeaderPageId+"\n");

            // Label pour quitter la boucle en la nommant
            outerLoop:
            // tant qu'il reste des header Page à explorer
            while (cpt < nbDataPage) {
                // Parcourir chaque entrée d'une header Page
                while ((offset < DBConfig.pagesize - 8) && (cpt < nbDataPage)) {

                    //System.out.println("offset = "+offset+" in header Page = "+currentPage);
                    //System.out.println(buffer.getInt(offset)+" >= "+(recordSize + 8)+" "+((buffer.getInt(offset)) >= (recordSize + 8)));

                    // Si l'espace libre >= à la taille du record + son offset directory
                    if ((buffer.getInt(offset)) >= (recordSize + 8)) {
                        // Récupère l'@ de la data Page
                        dataPageId = new PageId();
                        dataPageId.FileIdx = buffer.getInt(offset-8);
                        dataPageId.PageIdx = buffer.getInt(offset-4);

                        System.out.println("data Page: "+dataPageId);
                        
                        // Quitte la boucle nommé
                        break outerLoop;
                    }
                    // Avance dans la page de 12 pour sauter l'@ de la prochaine data Page et l'espace libre qu'on vient de lire
                    offset += 12;
                    cpt++;  // Incrémente le compteur de page
                }
                // Changement de header Page
                // Extrait la prochaine @
                PageId tempNextPage = new PageId(
                    buffer.getInt(DBConfig.pagesize - 8),
                    buffer.getInt(DBConfig.pagesize - 4)
                );

                // Si il y a du chainage
                if (tempNextPage.PageIdx != -1) {
                    // Libère la header Page courante
                    //bm.freePage(currentPage, false);
                    // charge la header page chainé suivante
                    //buffer = bm.getPage(tempNextPage);
                    currentPage = tempNextPage;

                    System.out.println("switch header Page: "+currentPage);

                    offset = 8; // RAZ l'offset
                }
            }

            */
            ByteBuffer buffer = null;

            // Si aucune data page n'a suffisamment d'espace, en ajouter une nouvelle
            if (dataPageId == null) {
                System.out.println("add new data Page");

                // Met de coter l'ancienne derniere header Page
                PageId tmp = new PageId(
                    LastHeaderPageId.FileIdx,
                    LastHeaderPageId.PageIdx
                );
                // Ajoute une nouvelle data page APRES la dernière header Page !
                //dataPageId = addDataPage();

                // Si on a une nouvelle header Page
                if (! tmp.equals(LastHeaderPageId)) {

                    System.out.println("new hp");

                    bm.freePage(tmp, false);  // Libère l'ancienne header Page
                    currentPage = LastHeaderPageId; // Met a jour current
                    buffer = bm.getPage(currentPage);  // Charge la dernière header Page
                    offset = 8; // RAZ l'offset
                }
            }
            // 'Buffer' contient la header Page dans laquelle on a la data Page qui va contenir le tuple
            // 'offset' contient la position de l'espace libre de la data Page qu'on utilise
            // 'currentPage' contient l'@ de la header Page
            // 'dataPageId' contient l'@ de la data Page

            // 1. Modifie la la header Page dans laquel la data Page choisie est
            // Récupère le nb d'octets libre auquel on enlève l'espace du record
            //int freeSpace = buffer.getInt(offset) - (recordSize+8);
            //buffer.putInt(offset, freeSpace);

            //System.out.println("pos ou ecrire l'espace libre dans la header Page = "+offset);
            //System.out.println("espace libre après ecriture = "+freeSpace);

            // Libere la header Page dans laquelle on va écrire
            //bm.freePage(currentPage, true);
/*
            // 2. Modifie la data Page
            // Charge la data Page en mémoir
            buffer = bm.getPage(dataPageId);

            // Insére le record dans la page sélectionnée
            int recordPos = buffer.getInt(DBConfig.pagesize - 4); // Position de l'espace libre

            System.out.println("\npage d'écriture = "+dataPageId);
            System.out.println("pos ecriture = "+recordPos);
 
            // Écrire le record dans le buffer et si on écrit pas exactement la taille du record c'est un échec
            if (writeRecordToBuffer(record, buffer, recordPos) != recordSize)
                throw new Exception("échec de l'écriture du record dans le buffer");

            // Si l'écriture à marché
            // Mettre à jour le Slot Directory
            int nbSlots = buffer.getInt(DBConfig.pagesize - 8); // Nombre de slots
            nbSlots++;
            buffer.putInt(DBConfig.pagesize - 8, nbSlots); // Ecris le nombre de slots incrémenté

            // récupere la position d'écriture du slotOffset
            int slotOffset = DBConfig.pagesize - 8 - (nbSlots * 8);

            System.out.println("slotOffset = "+slotOffset);

            buffer.putInt(slotOffset, recordPos); // Position du record
            buffer.putInt(slotOffset + 4, recordSize); // Taille du record

            // Actualise l'espace libre et le nombre de slots
            buffer.putInt(DBConfig.pagesize - 4, recordPos + recordSize);

            // Libére la page après modification
            bm.freePage(dataPageId, true); // True car la page a été modifiée

            // Retourne le RecordId du record composé d'un Page ID et l'index du slot
            return new RecordId(nbSlots, dataPageId);
*/
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("README: "+e.getMessage());
            // Libere les pages de travail en cas d'erreur à faux pour ne pas trop propager d'erreur
            bm.freePage(dataPageId, false);
            bm.freePage(LastHeaderPageId, false);
        }
        return null;    // Si on rencontre un problème retourne null
    }

    /**
     * Récupère tous les enregistrements de la base de données en parcourant toutes
     * les pages de données.
     *
     * @return Une liste de tous les enregistrements dans la base de données.
     * @throws Exception Si une erreur se produit lors de la récupération des enregistrements.
     */
    public ArrayList<MyRecord> GetAllRecords() throws Exception {
        ArrayList<MyRecord> allRecords = new ArrayList<>();

        List<PageId> dataPages = getDataPages();

        // Parcourir toutes les pages de données
        for (PageId pid : dataPages) {
            // Récupérer les records de la page actuelle
            allRecords.addAll(getRecordsInDataPage(pid)); // Ajouter tous les records de cette page à la liste finale
            // Libérer la page après lecture
            bm.freePage(pid, false); // False car il n'y a pas eu de modification
        }
        return allRecords;
    }

    /**
     * Récupère tous les enregistrements d'une page de données donnée.
     *
     * @param id L'identifiant de la page de données à traiter.
     * @return Une liste d'objets MyRecord représentant les enregistrements trouvés sur la page.
     * @throws Exception Si une erreur se produit lors de la lecture des enregistrements depuis la page.
    */
    public ArrayList<MyRecord> getRecordsInDataPage(PageId id) throws Exception {
        // Initialiser la liste de records
        ArrayList<MyRecord> records = new ArrayList<>();

        try {
            // Charger la page en mémoire
            ByteBuffer pageData = bm.getPage(id);
            int nbSlots = pageData.getInt(DBConfig.pagesize - 8); // Nombre de slots

            // Parcourir le Slot Directory pour récupérer chaque record
            for (int i = 0; i < nbSlots; i++) {
                // Calculer l'offset pour chaque slot (4 octets pour position et 4 pour taille)
                int slotOffset = DBConfig.pagesize - 8 - ((i + 1) * 8);
                // Lire la position et la taille du record depuis le Slot Directory
                int recordPos = pageData.getInt(slotOffset);
                int recordSize = pageData.getInt(slotOffset + 4);

                // Si la taille est non nulle, cela signifie que le record existe
                if (recordSize > 0){
                    records.add(new MyRecord());    // Initialise le tuple
                    // Lire le record en utilisant une méthode de lecture depuis le buffer
                    readRecordFromBuffer(records.get(i), pageData, recordPos);
                }
            }
        } finally {
            // Libérer la page après la lecture
            bm.freePage(id, false); // False car pas de modification
        }
        // Retourner la liste de records
        return records;
    }

    /**
     * Récupère le nom de l'attribut à l'index spécifié.
     *
     * @param index L'index de l'attribut dans la liste.
     * @return Le nom de l'attribut à l'index spécifié.
     */
    public String getNameAttribut(int index) throws IndexOutOfBoundsException{
        return attribut.get(index).getFirst();
    }

    /**
     * Récupère le type de données de l'attribut à l'index spécifié.
     *
     * @param index L'index de l'attribut dans la liste.
     * @return Le type de données de l'attribut à l'index spécifié.
     */
    public DataType getType(int index) {
        return attribut.get(index).getSecond().getType();
    }

    /**
     * Récupère la taille en octete de l'attribut à l'index spécifié.
     *
     * @param index L'index de l'attribut dans la liste.
     * @return La taille de l'attribut à l'index spécifié.
     */
    public int getLength(int index) {
        return attribut.get(index).getSecond().getLength();
    }

    /**
     * Récupère le nombre d'attribut.
     *
     * @return le nombre d'attribut de la relation.
     */
    public int getNbAttribut() {
        return attribut.size();
    }

    /**
     * Récupère le nom de la relation.
     *
     * @return Le nom de la relation.
     */
    public String getRelationName() {
        return relationName;
    }

    /**
     * Définit le nom de la relation.
     *
     * @param relationName Le nouveau nom de la relation.
     * @throws IllegalArgumentException Si le nom de la relation est null ou vide.
     */
    public void setRelationName(String relationName) throws IllegalArgumentException {
        if (relationName == null)
            throw new IllegalArgumentException("Le nom ne peut pas etre null");

        relationName = relationName.toUpperCase();  // Met en majuscule la chaine

        // Vérification si le nom est valide
        if (!SGBD.isValidName(relationName))
            throw new IllegalArgumentException("Le nom "+relationName+" est invalide");

        // Assignation du nom de la relation
        this.relationName = relationName;
    }    

    /**
     * Récupère la liste des attributs de la relation.
     *
     * @return La liste des attributs de la relation.
     */
    public ArrayList<Pair<String, Data>> getAttribut() {
        return attribut;
    }

    /**
     * Récupère les attributs de la relation a l'index donné.
     * @param un index de la liste
     * @return Une Pair nom, type.
     */
    public Pair<String, Data> getAttribut(int index) throws IndexOutOfBoundsException{
        return attribut.get(index);
    }

    /**
     * Définit la liste des attributs de la relation.
     *
     * @param attribut La nouvelle liste des attributs.
     * @throws IllegalArgumentException Si la liste des attributs est null ou vide.
     */
    public void setAttribut(ArrayList<Pair<String, Data>> attribut) throws IllegalArgumentException{
        if(attribut == null)
            throw new IllegalArgumentException("La liste des attributs ne peut pas être vide.");

        for(Pair<String, Data> attrb: attribut)
            setOneAttribut(attrb);
    }

    /**
     * Définit la liste des attributs de la relation.
     *
     * @param attribut La nouvelle liste des attributs.
     * @throws IllegalArgumentException Si la liste des attributs est null ou vide.
     */
    public void setOneAttribut(Pair<String, Data> attrb) throws IllegalArgumentException{
        // Test l'objet Puis ses composant
        if (attrb == null || attrb.getFirst() == null || attrb.getSecond() == null)
            throw new IllegalArgumentException("L' attributs ne peut pas être vide.");

        // Test le nom
        if (! SGBD.isValidName(attrb.getFirst()))
            throw new IllegalArgumentException("Le nom de l'attribut "+attrb.getFirst() +"  n'est pas valide");
        
        // Le type est verifier lors de la construction
        // Si on passe tout les tests on l'ajoute
        // Met en majuscule avant
        attrb.setFirst(attrb.getFirst().toUpperCase());
        attribut.add(attrb);
    }

    /**
     * Ajoute un attribut à la liste des attributs de la relation.
     *
     * @param elmts Un objet Pair représentant l'attribut (nom, type de données).
     */ /*
    public void addAttribut(Pair<String, Data> elmts) throws IllegalArgumentException{
        if(elmts == null)
            throw new IllegalArgumentException("Erreur l'attribut ne peut pas etre null");

        elmts.setFirst(elmts.getFirst().toUpperCase());  // Met en majuscule le nom de la colonne

        if(!SGBD.isValidName(elmts.getFirst()))
            throw new IllegalArgumentException("Erreur le nom de l'attribut: "+elmts.getFirst()+"    est invalide");

        attribut.add(elmts);    // L'ajoute dans le schéma de la relation
    }
*/
    public PageId getHeaderPageId(){
        return headerPageId;
    }
}