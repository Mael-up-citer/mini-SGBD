/**
 * Classe représentant une date au format jour/mois/année.
 * Cette classe permet de manipuler des dates tout en validant leur format
 * et en vérifiant leur validité (y compris pour les années bissextiles).
 * Elle implémente l'interface {@link Comparable} pour permettre la comparaison entre deux dates.
 */
public class Date implements Comparable<Date> {
    private int day;   // Jour de la date
    private int month; // Mois de la date
    private int year;  // Année de la date

    /**
     * Constructeur pour initialiser une date.
     * 
     * @param day   Le jour de la date.
     * @param month Le mois de la date.
     * @param year  L'année de la date.
     * @throws IllegalArgumentException Si la date n'est pas valide.
     */
    public Date(int day, int month, int year){
        // Vérifie si les paramètres représentent une date valide
        if(!isValidDate(day, month, year))
            throw new IllegalArgumentException("Date non valide");  // Lève une exception si la date est invalide

        // Si la date est valide, les valeurs sont assignées
        this.day = day;
        this.month = month;
        this.year = year;
    }

    /**
     * Vérifie si une date est valide en fonction du jour, du mois et de l'année.
     * 
     * @param day   Le jour de la date.
     * @param month Le mois de la date.
     * @param year  L'année de la date.
     * @return true si la date est valide, sinon false.
     */
    private boolean isValidDate(int day, int month, int year){
        // Vérifie si le mois est compris entre 1 et 12
        if(month < 1 || month > 12) 
            return false;

        // Tableau contenant le nombre de jours pour chaque mois (index 0 = janvier, 11 = décembre)
        int[] daysInMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

        // Si l'année est bissextile, février aura 29 jours
        if(isBissextile(year))
            daysInMonth[1] = 29; // Février a 29 jours dans une année bissextile

        // Vérifie si le jour est valide pour le mois donné
        return day > 0 && day <= daysInMonth[month - 1];
    }

    /**
     * Vérifie si une année est bissextile.
     * 
     * @param year L'année à vérifier.
     * @return true si l'année est bissextile, sinon false.
     */
    private boolean isBissextile(int year){
        if(year % 4 == 0){
            if(year % 100 == 0)
                return year % 400 == 0;
            return true;
        }
        return false;
    }

    /**
     * Retourne une chaîne représentant la date au format "JJ/MM/AAAA".
     * 
     * @return La date sous forme de chaîne.
     */
    @Override
    public String toString(){
        return String.format("%02d/%02d/%04d", day, month, year);
    }

    /**
     * Compare la date courante avec une autre date.
     * 
     * @param other La date à comparer avec la date courante.
     * @return 0 si les dates sont égales, un nombre négatif si la date courante est avant l'autre,
     *         ou un nombre positif si la date courante est après l'autre.
     */
    @Override
    public int compareTo(Date other){
        if (this.year != other.year)
            return this.year - other.year;

        if (this.month != other.month)
            return this.month - other.month;

        return this.day - other.day;
    }

    // Getters

    /**
     * Retourne le jour de la date.
     * 
     * @return Le jour de la date.
     */
    public int getDay(){
        return day;
    }

    /**
     * Retourne le mois de la date.
     * 
     * @return Le mois de la date.
     */
    public int getMonth(){
        return month;
    }

    /**
     * Retourne l'année de la date.
     * 
     * @return L'année de la date.
     */
    public int getYear(){
        return year;
    }

    // Setters avec validation

    /**
     * Modifie le jour de la date.
     * 
     * @param day Le nouveau jour de la date.
     * @throws IllegalArgumentException Si le jour n'est pas valide pour la date actuelle.
     */
    public void setDay(int day){
        if (!isValidDate(day, this.month, this.year))
            throw new IllegalArgumentException("Jour non valide pour la date actuelle.");

        this.day = day;
    }

    /**
     * Modifie le mois de la date.
     * 
     * @param month Le nouveau mois de la date.
     * @throws IllegalArgumentException Si le mois n'est pas valide pour la date actuelle.
     */
    public void setMonth(int month){
        if (!isValidDate(this.day, month, this.year))
            throw new IllegalArgumentException("Mois non valide pour la date actuelle.");

        this.month = month;
    }

    /**
     * Modifie l'année de la date.
     * 
     * @param year La nouvelle année de la date.
     */
    public void setYear(int year){
        this.year = year;
    }
}