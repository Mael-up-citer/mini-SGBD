import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * Classe représentant une date au format jour/mois/année.
 * Cette classe permet de manipuler des dates tout en validant leur format
 * et en vérifiant leur validité (y compris pour les années bissextiles).
 * Elle implémente l'interface {@link Comparable} pour permettre la comparaison entre deux dates.
 * @author Mael Lecene
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
        if(!isValidDate(day, month, year)) {
            throw new IllegalArgumentException(String.format("Date non valide : %02d/%02d/%04d", day, month, year));
        }

        // Si la date est valide, les valeurs sont assignées
        this.day = day;
        this.month = month;
        this.year = year;
    }

    /**
     * Convertit une chaîne représentant une date au format "JJ/MM/AAAA" en une instance de la classe Date.
     * 
     * @param str La chaîne de caractères représentant la date sous le format "JJ/MM/AAAA".
     * @return Un objet {@link Date} représentant la date extraite de la chaîne.
     * @throws IllegalArgumentException Si la chaîne ne respecte pas le format attendu ou si la date est invalide.
     */
    public static Date toDate(String str) {
        // Vérifie si la chaîne est bien au format "JJ/MM/AAAA"
        if (str == null || str.length() != 10 || str.charAt(2) != '/' || str.charAt(5) != '/') {
            throw new IllegalArgumentException("Le format de la date doit être JJ/MM/AAAA.");
        }

        // Extraire le jour, le mois et l'année à partir de la chaîne
        int day = Integer.parseInt(str.substring(0, 2));   // Les 2 premiers caractères représentent le jour
        int month = Integer.parseInt(str.substring(3, 5)); // Les caractères de l'indice 3 à 4 représentent le mois
        int year = Integer.parseInt(str.substring(6, 10)); // Les caractères de l'indice 6 à 9 représentent l'année

        // Retourne un objet Date après avoir validé la date
        return new Date(day, month, year);
    }

    /**
     * Compare deux dates pour vérifier si elles sont égales.
     * 
     * @param obj L'objet à comparer.
     * @return true si les dates sont égales, sinon false.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true; // Même instance

        if (obj == null || getClass() != obj.getClass())
            return false; // Différente classe ou null

        Date other = (Date) obj;

        return this.day == other.day && this.month == other.month && this.year == other.year;
    }

    /**
     * Génère un code de hachage pour la date.
     * 
     * @return Le code de hachage de la date.
     */
    @Override
    public int hashCode() {
        return Objects.hash(day, month, year); // Crée un hash basé sur les attributs
    }

    /**
     * Retourne le timestamp Unix (nombre de secondes écoulées depuis le 1er janvier 1970)
     * pour la date représentée par cet objet {@link Date}.
     * 
     * @return Le nombre de secondes écoulées depuis le 1er janvier 1970 jusqu'à cette date.
     */
    public long timestamp() {
        // Créer une instance de LocalDate représentant le 1er janvier 1970
        LocalDate epoch = LocalDate.of(1970, 1, 1);
    
        // Créer une instance de LocalDate représentant la date actuelle
        LocalDate currentDate = LocalDate.of(this.year, this.month, this.day);
    
        // Calculer la différence en jours entre les deux dates
        long daysBetween = ChronoUnit.DAYS.between(epoch, currentDate);
    
        // Convertir la différence en secondes (1 jour = 86400 secondes)
        return daysBetween * 86400L; // Changement de int à long
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
        if(month < 1 || month > 12) {
            return false;
        }

        // Tableau contenant le nombre de jours pour chaque mois (index 0 = janvier, 11 = décembre)
        int[] daysInMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

        // Si l'année est bissextile, février aura 29 jours
        if(isBissextile(year)) {
            daysInMonth[1] = 29; // Février a 29 jours dans une année bissextile
        }

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
            if(year % 100 == 0) {
                return year % 400 == 0;
            }
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
        if (this.year != other.year) {
            return this.year - other.year;
        }

        if (this.month != other.month) {
            return this.month - other.month;
        }

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
        if (!isValidDate(day, this.month, this.year)) {
            throw new IllegalArgumentException(String.format("Jour %d non valide pour le mois %d/%d.", day, this.month, this.year));
        }
        
        this.day = day;
    }

    /**
     * Modifie le mois de la date.
     * 
     * @param month Le nouveau mois de la date.
     * @throws IllegalArgumentException Si le mois n'est pas valide pour la date actuelle.
     */
    public void setMonth(int month){
        if (!isValidDate(this.day, month, this.year)) {
            throw new IllegalArgumentException(String.format("Mois %d non valide pour le jour %d/%d.", month, this.day, this.year));
        }
        
        this.month = month;
    }

    /**
     * Modifie l'année de la date.
     * 
     * @param year La nouvelle année de la date.
     */
    public void setYear(int year){
        // Il n'est pas nécessaire de valider une année seule.
        this.year = year;
    }
}