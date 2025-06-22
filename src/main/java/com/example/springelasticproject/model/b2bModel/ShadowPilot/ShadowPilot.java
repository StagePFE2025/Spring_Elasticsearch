package com.example.springelasticproject.model.b2bModel.ShadowPilot;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Modèle corrigé pour FlexibleBusinessReview
 *
 * CORRECTIONS APPORTÉES:
 * 1. Nom d'index en minuscules (shadowpilot au lieu de ShadowPilot)
 * 2. Ajout de @Field pour l'ID
 * 3. Gestion correcte des collections nulles
 * 4. Validation des contraintes
 */
@Document(indexName = "shadowpilot") // ✅ CORRIGÉ: minuscules obligatoires
public class ShadowPilot {

    @Id
    @Field(type = FieldType.Keyword) // ✅ AJOUTÉ: spécifier le type pour l'ID
    private String id;

    // ===== CHAMPS ESSENTIELS DIRECTS =====

    @NotBlank(message = "Le nom de l'entreprise est obligatoire")
    @Field(type = FieldType.Text, analyzer = "standard")
    private String name;

    @Field(type = FieldType.Text, analyzer = "standard")
    private String category;
    @Field(type = FieldType.Text, analyzer = "standard")
    private String subCategory;

    @Field(type = FieldType.Text, analyzer = "standard")
    private String address;

    @Field(type = FieldType.Keyword)
    private String domain;

    @Field(type = FieldType.Text)
    private String phone;

    @Field(type = FieldType.Keyword)
    private String email;

    @Field(type = FieldType.Keyword)
    private String website;

    // ===== DONNÉES FLEXIBLES SOUS FORME DE JSON =====

    /**
     * Métriques business: trustscore, number_of_reviews, etc.
     */
    @Field(type = FieldType.Object)
    private Map<String, Object> businessMetrics = new HashMap<>(); // ✅ CORRIGÉ: initialisation directe

    /**
     * Réseaux sociaux
     */
    @Field(type = FieldType.Object)
    private Map<String, Object> socialMedia = new HashMap<>(); // ✅ CORRIGÉ: initialisation directe

    /**
     * Avis détaillés (10 maximum)
     */
    @Field(type = FieldType.Nested)
    private List<Map<String, Object>> enhancedReviews = new ArrayList<>(); // ✅ CORRIGÉ: initialisation directe

    /**
     * Avis 5 étoiles (5 maximum)
     */
    @Field(type = FieldType.Nested)
    private List<Map<String, Object>> fiveStarReviews = new ArrayList<>(); // ✅ CORRIGÉ: initialisation directe

    /**
     * Avis 1 étoile (5 maximum)
     */
    @Field(type = FieldType.Nested)
    private List<Map<String, Object>> oneStarReviews = new ArrayList<>(); // ✅ CORRIGÉ: initialisation directe

    /**
     * Distribution des sentiments par étoiles
     */
    @Field(type = FieldType.Object)
    private Map<String, Object> sentimentDistribution = new HashMap<>(); // ✅ CORRIGÉ: initialisation directe

    /**
     * Évaluations par étoiles
     */
    @Field(type = FieldType.Object)
    private Map<String, Object> starRatings = new HashMap<>(); // ✅ CORRIGÉ: initialisation directe

    /**
     * Entreprises similaires (5 maximum)
     */
    @Field(type = FieldType.Nested)
    private List<Map<String, Object>> similarCompanies = new ArrayList<>(); // ✅ CORRIGÉ: initialisation directe

    // ===== CONSTRUCTEURS =====

    public ShadowPilot() {
        // ✅ CORRIGÉ: Les collections sont déjà initialisées au niveau des champs
        // Plus besoin d'initialisation dans le constructeur
    }

    public ShadowPilot(String name, String address, String domain) {
        this(); // Appel du constructeur par défaut
        this.name = name;
        this.address = address;
        this.domain = domain;
    }

    // ===== GETTERS ET SETTERS =====

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public Map<String, Object> getBusinessMetrics() {
        return businessMetrics;
    }

    public void setBusinessMetrics(Map<String, Object> businessMetrics) {
        this.businessMetrics = businessMetrics != null ? businessMetrics : new HashMap<>(); // ✅ CORRIGÉ: protection null
    }

    public Map<String, Object> getSocialMedia() {
        return socialMedia;
    }

    public void setSocialMedia(Map<String, Object> socialMedia) {
        this.socialMedia = socialMedia != null ? socialMedia : new HashMap<>(); // ✅ CORRIGÉ: protection null
    }

    public List<Map<String, Object>> getEnhancedReviews() {
        return enhancedReviews;
    }

    public void setEnhancedReviews(List<Map<String, Object>> enhancedReviews) {
        this.enhancedReviews = enhancedReviews != null ? enhancedReviews : new ArrayList<>(); // ✅ CORRIGÉ: protection null
    }

    public List<Map<String, Object>> getFiveStarReviews() {
        return fiveStarReviews;
    }

    public void setFiveStarReviews(List<Map<String, Object>> fiveStarReviews) {
        this.fiveStarReviews = fiveStarReviews != null ? fiveStarReviews : new ArrayList<>(); // ✅ CORRIGÉ: protection null
    }

    public List<Map<String, Object>> getOneStarReviews() {
        return oneStarReviews;
    }

    public void setOneStarReviews(List<Map<String, Object>> oneStarReviews) {
        this.oneStarReviews = oneStarReviews != null ? oneStarReviews : new ArrayList<>(); // ✅ CORRIGÉ: protection null
    }

    public Map<String, Object> getSentimentDistribution() {
        return sentimentDistribution;
    }

    public void setSentimentDistribution(Map<String, Object> sentimentDistribution) {
        this.sentimentDistribution = sentimentDistribution != null ? sentimentDistribution : new HashMap<>(); // ✅ CORRIGÉ: protection null
    }

    public Map<String, Object> getStarRatings() {
        return starRatings;
    }

    public void setStarRatings(Map<String, Object> starRatings) {
        this.starRatings = starRatings != null ? starRatings : new HashMap<>(); // ✅ CORRIGÉ: protection null
    }

    public List<Map<String, Object>> getSimilarCompanies() {
        return similarCompanies;
    }

    public void setSimilarCompanies(List<Map<String, Object>> similarCompanies) {
        this.similarCompanies = similarCompanies != null ? similarCompanies : new ArrayList<>(); // ✅ CORRIGÉ: protection null
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getSubCategory() {
        return subCategory;
    }

    public void setSubCategory(String subcCategory) {
        this.subCategory = subcCategory;
    }
    // ===== MÉTHODES UTILITAIRES =====

    /**
     * Ajoute une métrique business
     */
    public void addBusinessMetric(String key, Object value) {
        if (this.businessMetrics == null) {
            this.businessMetrics = new HashMap<>();
        }
        this.businessMetrics.put(key, value);
    }

    /**
     * Récupère une métrique business avec type générique
     */
    @SuppressWarnings("unchecked")
    public <T> T getBusinessMetric(String key, Class<T> type) {
        if (this.businessMetrics == null) {
            return null;
        }
        Object value = this.businessMetrics.get(key);
        if (value != null && type.isInstance(value)) {
            return (T) value;
        }
        return null;
    }

    /**
     * Ajoute une URL de réseau social
     */
    public void addSocialMediaUrl(String platform, String url) {
        if (this.socialMedia == null) {
            this.socialMedia = new HashMap<>();
        }
        this.socialMedia.put(platform + "_url", url);
    }

    /**
     * Ajoute un avis enhanced
     */
    public void addEnhancedReview(Map<String, Object> review) {
        if (this.enhancedReviews == null) {
            this.enhancedReviews = new ArrayList<>();
        }
        if (this.enhancedReviews.size() < 10) {
            this.enhancedReviews.add(review);
        }
    }

    /**
     * Ajoute une entreprise similaire
     */
    public void addSimilarCompany(String name, String url) {
        if (this.similarCompanies == null) {
            this.similarCompanies = new ArrayList<>();
        }
        if (this.similarCompanies.size() < 5) {
            Map<String, Object> company = new HashMap<>();
            company.put("name", name);
            company.put("url", url);
            this.similarCompanies.add(company);
        }
    }

    /**
     * Récupère le score de confiance (trustscore)
     */
    public Double getTrustscore() {
        return getBusinessMetric("trustscore", Double.class);
    }

    /**
     * Récupère le nombre total d'avis
     */
    public Double getNumberOfReviews() {
        return getBusinessMetric("number_of_reviews", Double.class);
    }

    /**
     * Vérifie si l'entreprise a des réseaux sociaux
     */
    public Boolean hasSocialMedia() {
        if (socialMedia == null) return false;
        Object hasSocial = socialMedia.get("has_social_media");
        return hasSocial instanceof Boolean ? (Boolean) hasSocial : false;
    }

    // ===== MÉTHODES DE VALIDATION =====

    /**
     * ✅ AJOUTÉ: Valide que l'objet est correctement formé
     */
    public boolean isValid() {
        return name != null && !name.trim().isEmpty();
    }

    /**
     * ✅ AJOUTÉ: Prépare l'objet pour la sérialisation
     */
    public void prepareForSerialization() {
        // S'assurer que toutes les collections sont initialisées
        if (businessMetrics == null) businessMetrics = new HashMap<>();
        if (socialMedia == null) socialMedia = new HashMap<>();
        if (enhancedReviews == null) enhancedReviews = new ArrayList<>();
        if (fiveStarReviews == null) fiveStarReviews = new ArrayList<>();
        if (oneStarReviews == null) oneStarReviews = new ArrayList<>();
        if (sentimentDistribution == null) sentimentDistribution = new HashMap<>();
        if (starRatings == null) starRatings = new HashMap<>();
        if (similarCompanies == null) similarCompanies = new ArrayList<>();
    }

    // ===== MÉTHODES STANDARD =====

    @Override
    public String toString() {
        return "FlexibleBusinessReview{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", domain='" + domain + '\'' +
                ", trustscore=" + getTrustscore() +
                ", numberOfReviews=" + getNumberOfReviews() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShadowPilot that = (ShadowPilot) o;
        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}