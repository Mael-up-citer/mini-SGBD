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
    private PageId headerPageId;    // Identifiant de la première page (page d'en-tête) de la relation
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

        this.attribut = attribut;
        this.headerPageId = headerPageId;
        this.dskm = dsmk;
        this.bm = bm;
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

        } catch (IndexOutOfBoundsException e) {
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

        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
        return 0;  // En cas d'erreur, retourne 0
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

        int offset = 12;    // Premiers 4 octets pour le compteur + 8 pour l'@ de la 1er dataPage

        try {
            // Charger la 1er Header Page en mémoire
            ByteBuffer headerData = bm.getPage(currentPage);
            // Récupère le nombre de page référencé
            int nbDataPage = headerData.getInt(0);

            currentPage = headerPageId; // Affecte  currentPage a la 1er header Page

            // tantque on n'a pas toute les pages
            while (pageIds.size() < nbDataPage) {
                // Parcourir chaque entrée d'une header Page pour lire les PageId
                while ((pageIds.size() < nbDataPage) && (offset <= DBConfig.pagesize-(8+12))) {
                    // Si la datapage est vide
                    if (headerData.getInt(offset) == 0) {
                        // Lire le fileIdx et le pageIdx de l'entrée actuelle
                        int fileIdx = headerData.getInt(offset - 8);
                        int pageIdx = headerData.getInt(offset - 4);

                        // Créer un PageId avec fileIdx et pageIdx, puis l'ajouter à la liste
                        pageIds.add(new PageId(fileIdx, pageIdx));
                    }
                    offset += 8;   // Avance dans la page de 8 pour sauter le pointage vers la data page
                }
                // Libère la header Page courante
                bm.freePage(currentPage, false);

                // Extrait l'adresse de la prochaine page de header Page
                currentPage.FileIdx = headerData.getInt(DBConfig.pagesize-8);
                currentPage.PageIdx = headerData.getInt(DBConfig.pagesize-4);

                // Si il y a du chainage
                if(currentPage.PageIdx != -1)
                    // charge la header page chainé suivante
                    headerData = bm.getPage(currentPage);

                offset = 8; // RAZ l'offset
            }
        } catch(Exception e){

        }finally {
            // Libérer la page après la lecture
            bm.freePage(currentPage, false); // False car pas de modification
        }
        // Retourner la liste des PageIds
        return pageIds;
    }

    // OPTIMISABLE !!! i1: Ne pas libere la 1er headerPage mais faire node.dirty = true :)
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

            // Charge la header Page en mémoire
            ByteBuffer buffer = bm.getPage(headerPageId);

            // Récupère et met à jour le nombre total de pages de données
            int nbDataPage = buffer.getInt(0);
            nbDataPage++;   // Incrémente le nombre total de pages
            buffer.putInt(0, nbDataPage); // Mise à jour dans la page d'en-tête

            //System.out.println("header Page = "+headerPageId.FileIdx+"  "+headerPageId.PageIdx+" nb data Page "+nbDataPage);

            bm.freePage(headerPageId, true);    // Relache la page a true

            // compteur de header Page initialisé à 1 car 0 impossible
            int nbHeaderPage = 1;

            // Calcul des limites des entrées pour chaque type de header Page
            int maxEntriesFirstHeader = (DBConfig.pagesize - 4 - 8) / 12; // Premières 4 octets pour le compteur + 8 pour l'@
            int maxEntriesOtherHeader = (DBConfig.pagesize - 8) / 12; // 8 octets pour le chaînage

            buffer = bm.getPage(headerPageId);  // Recharge la 1er header Page

            // PageId de la header Page courante
            PageId currentPage = new PageId(headerPageId.FileIdx, headerPageId.PageIdx);

            // PageId de la prochaine header Page
            PageId nextPage = new PageId(headerPageId.FileIdx, headerPageId.PageIdx);

            System.out.println("debut: dernier int ecris = "+buffer.getInt(DBConfig.pagesize - 4));

            // Parcourt les header Pages pour trouver la dernière
            while ((buffer.getInt(DBConfig.pagesize - 4)) != -1) {
                System.out.println("parcour... curret id = "+currentPage);
                // Charge l'identifiant de la prochaine header Page
                nextPage.FileIdx = buffer.getInt(DBConfig.pagesize - 8);
                nextPage.PageIdx = buffer.getInt(DBConfig.pagesize - 4);
                System.out.println("parcour... next id = "+nextPage);

                // Libère l'anciennes header pages
                bm.freePage(currentPage, false); // Non modifiée, donc false

                // Charge la prochaine page en mémoir
                buffer = bm.getPage(nextPage);

                // Sauvegarde l'@ de l'ancienne page
                currentPage.FileIdx = nextPage.FileIdx;
                currentPage.PageIdx = nextPage.PageIdx;

                // Incrémente le compteur de header pages
                nbHeaderPage++;
            }            
            // À ce moment, 'buffer' contient la dernière header page et 'nextPage' l'@ de la derniere header Page

            // Calcule le nombre d'entrées déjà utilisées dans les pages précédentes
            int entriesBefore = (nbHeaderPage == 1) 
                                ? 0 
                                : maxEntriesFirstHeader + (maxEntriesOtherHeader * (nbHeaderPage - 2));

            // Calcule l'index relatif de l'entrée dans la page courante
            int entryIndexInCurrentPage = nbDataPage - entriesBefore - 1;

            // Calcule l'offset pour ajouter la nouvelle entrée
            int offset = (entryIndexInCurrentPage * 12) + (nbHeaderPage == 1 ? 4 : 0); // 4 octets en première page

            // Si l'offset est plus grand que la taille de la page réel (taille - chainage - espace d'une dataPage)
            if (offset > (DBConfig.pagesize - (8+12))) {
                // Alloue une nouvelle header page
                PageId newHeaderPage = dskm.AllocPage();
                
                System.out.println("new header page = "+newHeaderPage);
                
                // Ajoute le chaînage vers la nouvelle page
                buffer.putInt(DBConfig.pagesize - 8, newHeaderPage.FileIdx);
                buffer.putInt(DBConfig.pagesize - 4, newHeaderPage.PageIdx);

                System.out.println("ecris dans le buffer: "+buffer.getInt(DBConfig.pagesize - 8)+"  "+buffer.getInt(DBConfig.pagesize - 4));

                // Libère la page actuelle avec indication de modification
                bm.freePage(nextPage, true);

                // Initialise la nouvelle header page
                // Charge dans le buffer
                buffer = bm.getPage(newHeaderPage);
                buffer.putInt(DBConfig.pagesize - 4, -1); // Indique qu'il n'y a pas de page suivante

                // Remet l'offset à 0 dans la nouvelle page
                offset = 0;

                // Maintenant la dernière page devient celle qu'on a alloué
                nextPage.FileIdx = newHeaderPage.FileIdx;
                nextPage.PageIdx = newHeaderPage.PageIdx;
            }
            // Écrit les informations de la nouvelle page dans la header Page
            buffer.putInt(offset, id.FileIdx);      // Fichier de la nouvelle page
            buffer.putInt(offset + 4, id.PageIdx);  // Index de la nouvelle page
            buffer.putInt(offset + 8, DBConfig.pagesize - 8);  // octets libre

            // Libère la dernière header page avec indication de modification
            bm.freePage(nextPage, true);

            System.out.println("avant le get dernier int ecris = "+bm.getPage(headerPageId).getInt(DBConfig.pagesize - 4));
            bm.freePage(headerPageId, false);

            // Ecrit l'offset directory de la nouvelle data Page
            buffer = bm.getPage(new PageId(id.FileIdx, id.PageIdx));    // Charge la nouvelle dataPage

            System.out.println("avant le get dernier int ecris = "+bm.getPage(headerPageId).getInt(DBConfig.pagesize - 4));
            bm.freePage(headerPageId, false);

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

        // PageId de la page suivante dans le chainage
        PageId nextPage = new PageId(headerPageId.FileIdx, headerPageId.PageIdx);

        int offset = 4;     // Premiers 4 octets pour le compteur

        try {
            // Charger la 1er Header Page en mémoire
            ByteBuffer buffer = bm.getPage(headerPageId);
            // Recupere le nombre de data Page
            int nbDataPage = buffer.getInt(0);

            // tantqu'il y a du chainage faire
            while (pageIds.size() < nbDataPage) {
                // Parcourir chaque entrée d'une header Page pour lire les PageId
                while ((pageIds.size() < nbDataPage) && (offset < DBConfig.pagesize-(8+12))) {
                    // Lire le fileIdx et le pageIdx de l'entrée actuelle
                    int fileIdx = buffer.getInt(offset);
                    int pageIdx = buffer.getInt(offset + 4);

                    // Créer un PageId avec fileIdx et pageIdx, puis l'ajouter à la liste
                    pageIds.add(new PageId(fileIdx, pageIdx));

                    offset += 4;   // Avance dans la page de 4 pour sauter l'espace libre coder sur 4octets
                }

                // current recoit l'ancienne valeur
                currentPage.FileIdx = nextPage.FileIdx; currentPage.PageIdx = nextPage.PageIdx;

                // Extrait l'adresse de la prochaine page de header Page
                nextPage.FileIdx = buffer.getInt(DBConfig.pagesize-8);
                nextPage.PageIdx = buffer.getInt(DBConfig.pagesize-4);

                // Libère l'ancienne valeur
                bm.freePage(currentPage, false);

                // Si il y a du chainage
                if(nextPage.PageIdx != -1)
                    // charge la header page chainé suivante
                    buffer = bm.getPage(nextPage);

                offset = 0; // RAZ l'offset
            }
        } catch(Exception e) {
            e.printStackTrace();
            // Libérer la page courrante en cas d'excpetion
            bm.freePage(nextPage, false); // False car pas de modification
        }
        // Retourner la liste des PageIds
        return pageIds;
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
     * Insère un enregistrement dans la table. Si une page de données
     * avec suffisamment d'espace est trouvée, l'enregistrement est inséré dans celle-ci.
     * Sinon, une nouvelle page est ajoutée à la base de données.
     *
     * @param record L'enregistrement à insérer dans la base de données.
     * @return Un RecordId représentant l'identifiant unique de l'enregistrement inséré.
     * @throws Exception Si une erreur se produit lors de l'insertion de l'enregistrement.
    */
    public RecordId InsertRecord(MyRecord record) throws Exception {
        // Calculer la taille du record (offset compris :) )
        int recordSize = record.getSizeOctet(this) + ((attribut.size()+1)*4);   // taille du record + la taille de l'offset directory du record

        // Contient la header Page courante
        PageId currentPage = new PageId();

        PageId dataPageId = null;   // Contient le pageId de la page ou l'on insère le record

        currentPage = headerPageId;

        int offset = 4;

        try {
            // Charger la 1er Header Page en mémoire
            ByteBuffer buffer = bm.getPage(headerPageId);
            // Recupere le nombre de data Page
            int nbDataPage = buffer.getInt(0);
            int cpt = 0;    // Compte combien de data Page on été essayer

            // tantqu'on a pas trouver de data Page pouvant aucceuillir le record ou qu'il reste des header Page a explorer
            while (cpt < nbDataPage && dataPageId == null) {
                // Parcourir chaque entrée d'une header Page pour lire les PageId
                while ((cpt < nbDataPage) && dataPageId == null) {
                    // Si la page a asser de place
                    if (buffer.getInt(offset) >= recordSize) {
                        // Récupère l'@ de la data Page
                        dataPageId = new PageId();
                        dataPageId.FileIdx = buffer.getInt(offset-8);
                        dataPageId.FileIdx = buffer.getInt(offset-8);
                    }                    
                    offset += 8;   // Avance dans la page de 8 pour sauter l'@ de la prochaine data Page
                    cpt++;
                }
                // Libère la header Page courante
                bm.freePage(currentPage, false);

                // Extrait l'adresse de la prochaine page de header Page
                currentPage.FileIdx = buffer.getInt(DBConfig.pagesize-8);
                currentPage.PageIdx = buffer.getInt(DBConfig.pagesize-4);

                // Si il y a du chainage
                if(currentPage.PageIdx != -1)
                    // charge la header page chainé suivante
                    buffer = bm.getPage(currentPage);

                offset = 8; // RAZ l'offset
            }

            // Si aucune page n'a suffisamment d'espace, en ajouter une nouvelle
            if (dataPageId == null){
                dataPageId = addDataPage();
                buffer = bm.getPage(dataPageId);
            }

            // Insére le record dans la page sélectionnée
            int recordPos = buffer.getInt(DBConfig.pagesize - 4); // Position de l'espace libre

            // Écrire le record dans le buffer
            writeRecordToBuffer(record, buffer, recordPos);

            // Mettre à jour le Slot Directory
            int nbSlots = buffer.getInt(DBConfig.pagesize - 8); // Nombre de slots
            nbSlots++;
            buffer.putInt(DBConfig.pagesize - 8, nbSlots); // Ecris le nombre de slots incrémenté

            // Met à jour le slot directory 
            int slotOffset = DBConfig.pagesize - 8 - (nbSlots * 8);

            buffer.putInt(slotOffset, recordPos); // Position du record
            buffer.putInt(slotOffset + 4, recordSize); // Taille du record

            // Actualiser l'espace libre et le nombre de slots
            buffer.putInt(DBConfig.pagesize - 4, recordPos + recordSize); // Nouvelle position libre

            // Libérer la page après modification
            bm.freePage(dataPageId, true); // True car la page a été modifiée

            // Retourner un RecordId
            return new RecordId(nbSlots, dataPageId); // Page ID et index du slot du record
        } finally{
            bm.freePage(dataPageId, false);
        }
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
     * Ajoute un attribut à la liste des attributs de la relation.
     *
     * @param elmts Un objet Pair représentant l'attribut (nom, type de données).
     */
    public void addAttribut(Pair<String, Data> elmts) throws IllegalArgumentException{
        if(elmts == null)
            throw new IllegalArgumentException("Erreur l'attribut ne peut pas etre null");

        elmts.setFirst(elmts.getFirst().toUpperCase());  // Met en majuscule le nom de la colonne

        if(!SGBD.isValidName(elmts.getFirst()))
            throw new IllegalArgumentException("Erreur le nom de l'attribut: "+elmts.getFirst()+"    est invalide");

        attribut.add(elmts);    // L'ajoute dans le schéma de la relation
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
     * Récupère la liste des attributs de la relation.
     *
     * @return La liste des attributs de la relation.
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
        if (attribut == null || attrb.getFirst() == null || attrb.getSecond() == null)
            throw new IllegalArgumentException("La liste des attributs ne peut pas être vide.");

        // Test le nom
        if (SGBD.isValidName(attrb.getFirst()))
            attribut.add(attrb);
        else
            throw new IllegalArgumentException("Le nom de l'attribut "+attrb.getFirst() +"  n'est pas valide");
        
        // Le type est verifier lors de la construction
        // Si on passe tout les tests on l'ajoute
        // Met en majuscule avant
        attrb.setFirst(attrb.getFirst().toUpperCase());
        attribut.add(attrb);
    }

    public PageId getHeaderPageId(){
        return headerPageId;
    }
}