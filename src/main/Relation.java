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
     * Constructeur par défaut. Initialise la liste des attributs pour la relation.
     */
    public Relation() {
        attribut = new ArrayList<>();   // Initialise la liste des attributs
    }

    /**
     * Constructeur avec initialisation des attributs.
     * 
     * @param relationName Le nom de la relation.
     * @param attribut La liste des attributs de la relation (nom et type).
     * @param headerPageId L'identifiant de la première page de la relation.
     * @param dsmk Le gestionnaire de disque.
     * @param bm Le gestionnaire de buffer.
     */
    public Relation(String relationName, ArrayList<Pair<String, Data>> attribut, PageId headerPageId, DiskManager dsmk, BufferManager bm) {
        this.relationName = relationName;
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
        int lgRecord = attribut.size();    // Nombre d'attributs dans le record
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
        int lgRelation = attribut.size();  // Nombre d'attributs dans la relation
        int posValue = pos + (lgRelation + 1) * Integer.BYTES;  // Première valeur après l'offset directory
        int start = pos;    // Position de départ dans le buffer

        try {
            // Parcourt l'offset directory pour récupérer les valeurs
            for (int i = 0; i < lgRelation; i++) {
                // Traitement du type de données pour chaque attribut
                switch (attribut.get(i).getSecond().getType()) {
                    case INT:
                        record.addValue(buffer.getInt(posValue), getType(i));
                        posValue += getLength(i); // Mise à jour de la position après lecture
                        break;

                    case REAL:
                        record.addValue(buffer.getFloat(posValue), getType(i));
                        posValue += getLength(i); // Mise à jour de la position après lecture
                        break;

                    case CHAR:
                    case VARCHAR:
                        int endOfString = buffer.getInt((pos + (i+1)*Integer.BYTES));
                        StringBuilder str = new StringBuilder();
                        char c;

                        // Parcours du buffer jusqu'a la fin de la chaine
                        while (endOfString > posValue) {
                            System.out.println(posValue);
                            c = buffer.getChar(posValue);
                            str.append(c);
                            posValue += Character.BYTES;  // Avance de 2 octets
                        }
                        record.addValue(str.toString().trim(), getType(i));
                        posValue = endOfString;  // Mise à jour de la position

                        break;

                    case DATE:
                        int day = buffer.getInt(posValue);
                        posValue += Integer.BYTES;  // Avance au prochain
                        int month = buffer.getInt(posValue);
                        posValue += Integer.BYTES;  // Avance au prochain
                        int year = buffer.getInt(posValue);
                        posValue += Integer.BYTES;  // Avance au prochain

                        record.addValue(new Date(day, month, year), getType(i));
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
     * Ajoute une nouvelle page de données à la relation.
     * Alloue une nouvelle page, met à jour le nombre de pages dans la page d'en-tête 
     * et ajoute la nouvelle page au répertoire des pages.
    */
    public void addDataPage() {
        try {
            // Alloue une nouvelle page de donnée
            PageId id = dskm.AllocPage();

            // Charge la première page d'en-tête en mémoire
            ByteBuffer buffer = bm.getPage(headerPageId);

            // Nombre de header pages
            int nbHeaderPage = 1;

            // PageId de la page courante
            PageId currentPage = new PageId();

            // PageId de la page suivante dans le chaînage
            PageId nextPage = new PageId();

            // Calcul des limites des entrées pour chaque type de header page
            int maxEntriesFirstHeader = (DBConfig.pagesize - 4 - 8) / 12; // Premières 4 octets pour le compteur
            int maxEntriesOtherHeader = (DBConfig.pagesize - 8) / 12; // 8 octets pour le chaînage

            // Récupère et met à jour le nombre total de pages de données
            int nbDataPage = buffer.getInt(0);
            nbDataPage++; // Incrémente le nombre total de pages
            buffer.putInt(0, nbDataPage); // Mise à jour dans la page d'en-tête

            // Parcourt les header pages pour trouver la dernière
            while (buffer.getInt(DBConfig.pagesize - 4) != -1) {
                // Sauvegarde l'identifiant de la page courante
                currentPage.FileIdx = nextPage.FileIdx;
                currentPage.PageIdx = nextPage.PageIdx;

                // Charge l'identifiant de la prochaine header page
                nextPage.FileIdx = buffer.getInt(DBConfig.pagesize - 8);
                nextPage.PageIdx = buffer.getInt(DBConfig.pagesize - 4);

                // Libère les anciennes header pages sauf la première (elle est modifiée)
                if (nbHeaderPage != 1)
                    bm.freePage(currentPage, false); // Non modifiée, donc false

                // Charge la page suivante
                buffer = bm.getPage(nextPage);
                // Incrémente le compteur de header pages
                nbHeaderPage++;
            }
            // À ce moment, `buffer` contient la dernière header page

            // Calcule le nombre d'entrées déjà utilisées dans les pages précédentes
            int entriesBefore = (nbHeaderPage == 1) 
                                ? 0 
                                : maxEntriesFirstHeader + (maxEntriesOtherHeader * (nbHeaderPage - 2));;

            // Calcule l'index relatif de l'entrée dans la page courante
            int entryIndexInCurrentPage = nbDataPage - entriesBefore - 1;

            // Calcule l'offset pour ajouter la nouvelle entrée
            int offset = (entryIndexInCurrentPage * 12) + (nbHeaderPage == 1 ? 4 : 0); // 4 octets en première page

            // Si la page est pleine, alloue une nouvelle header page
            if (offset > DBConfig.pagesize - (8+12)) {
                // Alloue une nouvelle header page
                PageId newHeaderPage = dskm.AllocPage();

                // Ajoute le chaînage vers la nouvelle page
                buffer.putInt(DBConfig.pagesize - 8, newHeaderPage.FileIdx);
                buffer.putInt(DBConfig.pagesize - 4, newHeaderPage.PageIdx);

                // Libère la page actuelle avec indication de modification
                bm.freePage(nextPage, true);

                // Initialise la nouvelle header page
                buffer = bm.getPage(newHeaderPage);
                buffer.putInt(DBConfig.pagesize - 4, -1); // Indique qu'il n'y a pas de page suivante

                // Remet l'offset à 0 dans la nouvelle page
                offset = 0;
            }

            // Écrit les informations de la nouvelle page dans le répertoire
            buffer.putInt(offset, id.FileIdx);      // Fichier de la nouvelle page
            buffer.putInt(offset + 4, id.PageIdx);  // Index de la nouvelle page
            buffer.putInt(offset + 8, DBConfig.pagesize - 4); // Taille libre initiale

            // Libère la dernière header page avec indication de modification
            bm.freePage(nextPage, true);

            // Libère la première page d'en-tête avec indication de modification
            bm.freePage(headerPageId, true);
        } catch (Exception e) {
            // Gestion d'erreur : impression de la trace et arrêt du processus
            e.printStackTrace();
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

        PageId currentPage = new PageId();

        // PageId de la page suivante dans le chainage
        PageId nextPage = new PageId(); // Cree un nouveau
        int end = -1;    // Pour arreter la while extérieur
        int offset = 4;

        currentPage = headerPageId;

        try {
            // Charger la 1er Header Page en mémoire
            ByteBuffer headerData = bm.getPage(currentPage);

            // tantqu'il y a du chainage faire
            while (end != 0) {
                if((headerData.getInt(DBConfig.pagesize-4) != -1))
                    end = 1;

                // Parcourir chaque entrée du Page Directory pour lire les PageId
                while (offset < DBConfig.pagesize - (8+12)) {
                    // Calculer l'offset de l'entrée dans le Page Directory (chaque entrée fait 12 octets)
                    offset += 12;

                    // Lire le fileIdx et le pageIdx de l'entrée actuelle
                    int fileIdx = headerData.getInt(offset);
                    int pageIdx = headerData.getInt(offset + 4);

                    // Créer un PageId avec fileIdx et pageIdx, puis l'ajouter à la liste
                    PageId pageId = new PageId(fileIdx, pageIdx);
                    pageIds.add(pageId);
                }
                end --; // Decrement end
                offset = 0; // RAZ l'offset

                // current recoit l'ancienne valeur
                currentPage.FileIdx = nextPage.FileIdx; currentPage.PageIdx = nextPage.PageIdx;

                // Extrait l'adresse de la prochaine page de header Page
                nextPage.FileIdx = headerData.getInt(DBConfig.pagesize-8);
                nextPage.PageIdx = headerData.getInt(DBConfig.pagesize-4);

                // Libère l'ancienne valeur
                bm.freePage(currentPage, false);

                // charge la header page chainé suivante
                headerData = bm.getPage(nextPage);
            }
        } finally {
            // Libérer la page après la lecture
            bm.freePage(headerPageId, false); // False car pas de modification
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
                if (recordSize > 0)
                    // Lire le record en utilisant une méthode de lecture depuis le buffer
                    readRecordFromBuffer(records.get(i), pageData, recordPos);
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
        // Calculer la taille du record
        int recordSize = record.getSizeOctet();

        PageId dataPageId = new PageId();
        ByteBuffer pageData = ByteBuffer.allocate(recordSize);

        // Chercher une page avec suffisamment d'espace libre
        for (PageId pid : getDataPages()) {
            pageData = bm.getPage(pid);
            int freeSpace = pageData.getInt(DBConfig.pagesize - 8); // Position espace libre

            // Si il y a assez de place
            if (freeSpace >= recordSize) {
                dataPageId = pid; // garde ce pid
                break;  // Quitte la boucle
            }
            bm.freePage(pid, false); // Libérer la page si elle ne convient pas
        }

        // Si aucune page n'a suffisamment d'espace, en ajouter une nouvelle
        if (dataPageId == null) {
            addDataPage();  // Rajoute une page à la fin
            pageData = bm.getPage(dataPageId);
        }

        // Insérer le record dans la page sélectionnée
        int recordPos = pageData.getInt(DBConfig.pagesize - 8); // Position de l'espace libre
        writeRecordToBuffer(record, pageData, recordPos); // Écrire le record dans le buffer

        // Mettre à jour le Slot Directory
        int numSlots = pageData.getInt(DBConfig.pagesize - 4); // Nombre de slots
        int slotOffset = DBConfig.pagesize - 8 - (numSlots + 1) * 8;

        pageData.putInt(slotOffset, recordPos); // Position du record
        pageData.putInt(slotOffset + 4, recordSize); // Taille du record

        // Actualiser l'espace libre et le nombre de slots
        pageData.putInt(DBConfig.pagesize - 8, recordPos + recordSize); // Nouvelle position libre
        pageData.putInt(DBConfig.pagesize - 4, numSlots + 1); // Nombre de slots incrémenté

        // Libérer la page après modification
        bm.freePage(dataPageId, true); // True car la page a été modifiée

        // Retourner un RecordId
        return new RecordId(numSlots, dataPageId); // Page ID et index du slot du record
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

        // Parcourir toutes les pages de données
        for (PageId pid : getDataPages()) {
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
    public void addAttribut(Pair<String, Data> elmts) {
        if(elmts == null)
            throw new IllegalArgumentException("Erreur l'attribut ne peut pas etre null");

        String attrbName = elmts.getFirst();

        if(attrbName == null || attrbName.isEmpty())
            throw new IllegalArgumentException("Erreur le nom de l'attribut ne peut pas");

        attrbName = attrbName.toUpperCase();

        // Vérification si le nom de la relation contient uniquement des lettres majuscules (A-Z)
        if (!attrbName.matches("^[A-Z]+$"))
            throw new IllegalArgumentException("Le nom de la relation doit contenir uniquement des lettres");

        attribut.add(elmts);
    }

    /**
     * Récupère le nom de l'attribut à l'index spécifié.
     *
     * @param index L'index de l'attribut dans la liste.
     * @return Le nom de l'attribut à l'index spécifié.
     */
    public String getNameAttribut(int index) {
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
    public void setRelationName(String relationName) {
        // Vérification si le nom est vide
        if (relationName == null || relationName.isEmpty())
            throw new IllegalArgumentException("Le nom de la relation ne peut pas être vide.");

        relationName = relationName.toUpperCase();  // Met le nom en maj

        // Vérification si le nom de la relation contient uniquement des lettres majuscules (A-Z)
        if (!relationName.matches("^[A-Z]+$"))
            throw new IllegalArgumentException("Le nom de la relation doit contenir uniquement des lettres.");
    
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
     * Définit la liste des attributs de la relation.
     *
     * @param attribut La nouvelle liste des attributs.
     * @throws IllegalArgumentException Si la liste des attributs est null ou vide.
     */
    public void setAttribut(ArrayList<Pair<String, Data>> attribut) {
        if (attribut == null || attribut.isEmpty())
            throw new IllegalArgumentException("La liste des attributs ne peut pas être vide.");

        this.attribut = attribut;
    }
}