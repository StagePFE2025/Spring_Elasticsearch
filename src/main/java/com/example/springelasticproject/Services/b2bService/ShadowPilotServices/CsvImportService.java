package com.example.springelasticproject.Services.b2bService.ShadowPilotServices;

import com.example.springelasticproject.model.b2bModel.ShadowPilot.ShadowPilot;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Service d'importation CSV pour les données ShadowPilot
 * MODIFIÉ: Ajout des paramètres category et subcategory
 */
@Service
public class CsvImportService {

    private static final Logger logger = LoggerFactory.getLogger(CsvImportService.class);
    private static final int BATCH_SIZE = 100;

    @Autowired
    private ShadowPilotService businessReviewService;

    /**
     * 🆕 NOUVELLES MÉTHODES avec category et subcategory
     */
    @Transactional
    public ImportResult importCsvFile(String filePath, String category, String subcategory) throws IOException, CsvException {
        File file = new File(filePath);
        return importCsvFile(new FileInputStream(file), file.getName(), category, subcategory);
    }

    @Transactional
    public ImportResult importCsvFile(MultipartFile file, String category, String subcategory) throws IOException, CsvException {
        return importCsvFile(file.getInputStream(), file.getOriginalFilename(), category, subcategory);
    }

    /**
     * 🔄 MÉTHODES DE COMPATIBILITÉ (utilisent des valeurs par défaut)
     */
    @Transactional
    public ImportResult importCsvFile(String filePath) throws IOException, CsvException {
        return importCsvFile(filePath, "DEFAULT", "DEFAULT");
    }

    @Transactional
    public ImportResult importCsvFile(MultipartFile file) throws IOException, CsvException {
        return importCsvFile(file, "DEFAULT", "DEFAULT");
    }

    @Transactional
    public ImportResult importCsvFile(InputStream inputStream, String fileName) throws IOException, CsvException {
        return importCsvFile(inputStream, fileName, "DEFAULT", "DEFAULT");
    }

    /**
     * 🔧 MÉTHODE PRINCIPALE MODIFIÉE avec category et subcategory
     */
    @Transactional
    public ImportResult importCsvFile(InputStream inputStream, String fileName, String category, String subcategory) throws IOException, CsvException {
        logger.info("=== DÉBUT IMPORT AVEC CATÉGORIES : {} ===", fileName);
        logger.info("Category: {}, Subcategory: {}", category, subcategory);

        // Validation et nettoyage des paramètres
        if (category == null || category.trim().isEmpty()) {
            category = "UNCATEGORIZED";
        }
        if (subcategory == null || subcategory.trim().isEmpty()) {
            subcategory = "UNCATEGORIZED";
        }

        ImportResult result = new ImportResult();
        result.setFileName(fileName);
        result.setCategory(category.trim()); // 🆕 NOUVEAU
        result.setSubcategory(subcategory.trim()); // 🆕 NOUVEAU
        result.setStartTime(LocalDateTime.now());

        try (CSVReader csvReader = new CSVReaderBuilder(new InputStreamReader(inputStream))
                .withSkipLines(1) // Skip header
                .build()) {

            // Lire toutes les lignes
            List<String[]> allRows = csvReader.readAll();
            result.setTotalRows(allRows.size());
            logger.info("Nombre total de lignes à traiter: {}", allRows.size());

            // Traitement par batch
            List<ShadowPilot> batch = new ArrayList<>();
            int processedCount = 0;
            int lineNumber = 1; // Pour le tracking des lignes

            for (String[] row : allRows) {
                lineNumber++;
                try {
                    // 🔧 CORRECTION: Passer category et subcategory au mapping
                    ShadowPilot business = mapRowToBusinessReview(row, lineNumber, category, subcategory);

                    if (business != null && businessReviewService.validateBusinessData(business)) {
                        batch.add(business);
                        result.incrementSuccessCount();

                        // Sauvegarder par batch
                        if (batch.size() >= BATCH_SIZE) {
                            logger.info("Sauvegarde batch de {} documents", batch.size());
                            businessReviewService.saveAll(batch);
                            batch.clear();
                            processedCount += BATCH_SIZE;
                            logger.info("Traité {} lignes sur {}", processedCount, allRows.size());
                        }
                    } else {
                        result.incrementErrorCount();
                        result.addError("Ligne " + lineNumber + ": Données invalides");
                        logger.warn("Validation/mapping échoué ligne {}", lineNumber);
                    }
                } catch (Exception e) {
                    result.incrementErrorCount();
                    result.addError("Ligne " + lineNumber + ": " + e.getMessage());
                    logger.error("Erreur ligne {}: {}", lineNumber, e.getMessage(), e);
                }
                processedCount++;
            }

            // Sauvegarder le dernier batch
            if (!batch.isEmpty()) {
                logger.info("Sauvegarde batch final de {} documents", batch.size());
                businessReviewService.saveAll(batch);
            }
        }

        result.setEndTime(LocalDateTime.now());
        logger.info("=== IMPORT TERMINÉ ===");
        logger.info("Succès: {}, Erreurs: {} pour Category: {}, Subcategory: {}",
                result.getSuccessCount(), result.getErrorCount(), category, subcategory);

        return result;
    }

    /**
     * 🔧 MÉTHODE DE MAPPING MODIFIÉE avec category et subcategory
     */
    private ShadowPilot mapRowToBusinessReview(String[] row, int lineNumber, String category, String subcategory) {
        if (row.length < 262) {
            logger.warn("Ligne {} CSV incomplète: {} colonnes au lieu de 262", lineNumber, row.length);
        }

        ShadowPilot business = new ShadowPilot();

        try {
            // ===== CHAMPS DIRECTS =====
            String name = getStringValue(row, "name");
            String address = getStringValue(row, "address");
            String domain = getStringValue(row, "domain");

            // Validation minimum
            if (name == null || name.trim().isEmpty()) {
                logger.warn("Ligne {}: Nom manquant, ligne ignorée", lineNumber);
                return null;
            }

            business.setName(name);
            business.setAddress(address);
            business.setDomain(domain);

            // 🆕 AJOUT DES NOUVELLES PROPRIÉTÉS
            business.setCategory(category);
            business.setSubCategory(subcategory);
            logger.debug("Ligne {}: Category='{}', Subcategory='{}'", lineNumber, category, subcategory);

            // ID unique OBLIGATOIRE
            String uniqueId = generateUniqueId(name, address, domain, lineNumber);
            business.setId(uniqueId);
            logger.debug("Ligne {}: ID généré = {}", lineNumber, uniqueId);

            business.setPhone(getStringValue(row, "phone"));
            business.setEmail(getStringValue(row, "email"));
            business.setWebsite(getStringValue(row, "website"));

            // ===== BUSINESS METRICS =====
            Map<String, Object> businessMetrics = new HashMap<>();
            businessMetrics.put("trustscore", getDoubleValue(row, "trustscore"));
            businessMetrics.put("number_of_reviews", getDoubleValue(row, "number_of_reviews"));
            businessMetrics.put("avg_reviews_per_month", getDoubleValue(row, "avg_reviews_per_month"));
            businessMetrics.put("business_age_days", getIntegerValue(row, "business_age_days"));
            businessMetrics.put("business_age_years", getDoubleValue(row, "business_age_years"));
            businessMetrics.put("competitor_count", getIntegerValue(row, "competitor_count"));
            businessMetrics.put("contact_completeness", getDoubleValue(row, "contact_completeness"));
            businessMetrics.put("response_rate", getDoubleValue(row, "response_rate"));
            businessMetrics.put("reviews_last_30_days", getIntegerValue(row, "reviews_last_30_days"));
            businessMetrics.put("total_categories", getIntegerValue(row, "total_categories"));
            businessMetrics.put("total_helpful_votes", getIntegerValue(row, "total_helpful_votes"));
            businessMetrics.put("verified_reviews_count", getIntegerValue(row, "verified_reviews_count"));
            businessMetrics.put("business_size_indicator", getStringValue(row, "business_size_indicator"));
            businessMetrics.put("categories", getStringValue(row, "categories"));
            businessMetrics.put("is_claimed", getBooleanValue(row, "is_claimed"));
            businessMetrics.put("trustpilot_domain", getStringValue(row, "trustpilot_domain"));
            businessMetrics.put("logo_url", getStringValue(row, "logo_url"));
            businessMetrics.put("num_reviews", getStringValue(row, "num_reviews"));
            businessMetrics.put("reviews", getStringValue(row, "reviews"));

            // 🆕 AJOUT CATEGORY/SUBCATEGORY DANS LES METRICS AUSSI (pour faciliter les requêtes)
            businessMetrics.put("import_category", category);
            businessMetrics.put("import_subcategory", subcategory);

            business.setBusinessMetrics(businessMetrics);

            // ===== SOCIAL MEDIA =====
            Map<String, Object> socialMedia = new HashMap<>();
            socialMedia.put("facebook_url", getStringValue(row, "facebook_url"));
            socialMedia.put("instagram_url", getStringValue(row, "instagram_url"));
            socialMedia.put("linkedin_url", getStringValue(row, "linkedin_url"));
            socialMedia.put("twitter_url", getStringValue(row, "twitter_url"));
            socialMedia.put("youtube_url", getStringValue(row, "youtube_url"));
            socialMedia.put("has_social_media", getBooleanValue(row, "has_social_media"));
            business.setSocialMedia(socialMedia);

            // ===== ENHANCED REVIEWS =====
            List<Map<String, Object>> enhancedReviews = new ArrayList<>();
            for (int i = 1; i <= 10; i++) {
                Map<String, Object> review = createReviewMap(row, "enhanced_review_" + i + "_");
                if (!review.isEmpty()) {
                    enhancedReviews.add(review);
                }
            }
            business.setEnhancedReviews(enhancedReviews);

            // ===== FIVE STAR REVIEWS =====
            List<Map<String, Object>> fiveStarReviews = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                Map<String, Object> review = createReviewMap(row, "five_star_reviews_" + i + "_");
                if (!review.isEmpty()) {
                    fiveStarReviews.add(review);
                }
            }
            business.setFiveStarReviews(fiveStarReviews);

            // ===== ONE STAR REVIEWS =====
            List<Map<String, Object>> oneStarReviews = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                Map<String, Object> review = createReviewMap(row, "one_star_reviews_" + i + "_");
                if (!review.isEmpty()) {
                    oneStarReviews.add(review);
                }
            }
            business.setOneStarReviews(oneStarReviews);

            // ===== SENTIMENT DISTRIBUTION =====
            Map<String, Object> sentimentDistribution = new HashMap<>();
            for (int i = 1; i <= 5; i++) {
                sentimentDistribution.put(i + "_star_avg_words",
                        getDoubleValue(row, "sentiment_distribution_" + i + "_star_avg_words"));
                sentimentDistribution.put(i + "_star_count",
                        getIntegerValue(row, "sentiment_distribution_" + i + "_star_count"));
            }
            business.setSentimentDistribution(sentimentDistribution);

            // ===== STAR RATINGS =====
            Map<String, Object> starRatings = new HashMap<>();
            String[] ratingTypes = {"one", "two", "three", "four", "five"};
            for (String type : ratingTypes) {
                starRatings.put("star_rating_percentages_" + type,
                        getStringValue(row, "star_rating_percentages_" + type));
                starRatings.put("star_ratings_" + type,
                        getStringValue(row, "star_ratings_" + type));
            }
            business.setStarRatings(starRatings);

            // ===== SIMILAR COMPANIES =====
            List<Map<String, Object>> similarCompanies = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                String nameS = getStringValue(row, "similar_companies_" + i + "_name");
                String url = getStringValue(row, "similar_companies_" + i + "_url");
                if (nameS != null && !nameS.isEmpty()) {
                    Map<String, Object> company = new HashMap<>();
                    company.put("name", nameS);
                    company.put("url", url);
                    similarCompanies.add(company);
                }
            }
            business.setSimilarCompanies(similarCompanies);

            return business;

        } catch (Exception e) {
            logger.error("Erreur mapping ligne {}: {}", lineNumber, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Génération d'ID unique garanti
     */
    private String generateUniqueId(String name, String address, String domain, int lineNumber) {
        String baseId = null;

        // Stratégie 1: Utiliser le domaine s'il existe (généralement unique)
        if (domain != null && !domain.trim().isEmpty() && !domain.equalsIgnoreCase("null")) {
            baseId = cleanId(domain);
        }
        // Stratégie 2: Combiner nom et adresse
        else if (name != null && address != null) {
            baseId = cleanId(name.trim() + "_" + address.trim());
        }
        // Stratégie 3: Utiliser seulement le nom si disponible
        else if (name != null && !name.trim().isEmpty()) {
            baseId = cleanId(name);
        }

        // Garantir l'unicité avec hash des données + ligne + timestamp
        String uniqueSalt = String.valueOf(Objects.hash(name, address, domain, lineNumber, System.nanoTime()));
        String finalId = (baseId != null ? baseId : "business") + "_" + uniqueSalt;

        return finalId;
    }

    /**
     * Nettoie une chaîne pour en faire un ID Elasticsearch valide
     */
    private String cleanId(String input) {
        if (input == null || input.trim().isEmpty()) {
            return "business_" + UUID.randomUUID().toString();
        }

        // Remplacer les caractères non autorisés par des underscores
        String cleaned = input.toLowerCase()
                .replaceAll("[^a-z0-9._-]", "_")
                .replaceAll("_{2,}", "_")  // Remplacer les underscores multiples
                .replaceAll("^_+|_+$", ""); // Supprimer les underscores en début/fin

        // S'assurer que l'ID n'est pas vide et pas trop long
        if (cleaned.isEmpty()) {
            return "business_" + UUID.randomUUID().toString();
        }

        // Tronquer si trop long (Elasticsearch limite à 512 bytes)
        if (cleaned.length() > 100) {
            cleaned = cleaned.substring(0, 100);
        }

        return cleaned;
    }

    // ===== MÉTHODES UTILITAIRES INCHANGÉES =====

    /**
     * Crée un map pour un avis à partir des colonnes CSV
     */
    private Map<String, Object> createReviewMap(String[] row, String prefix) {
        Map<String, Object> review = new HashMap<>();

        String date = getStringValue(row, prefix + "date");
        String text = getStringValue(row, prefix + "text");

        // Un avis est valide s'il a au moins une date ou un texte
        if ((date != null && !date.isEmpty()) || (text != null && !text.isEmpty())) {
            review.put("date", date);
            review.put("has_title", getStringValue(row, prefix + "has_title"));
            review.put("helpful_votes", getDoubleValue(row, prefix + "helpful_votes"));
            review.put("is_verified", getStringValue(row, prefix + "is_verified"));
            review.put("rating", getDoubleValue(row, prefix + "rating"));
            review.put("reviewer_location", getStringValue(row, prefix + "reviewer_location"));
            review.put("reviewer_name", getStringValue(row, prefix + "reviewer_name"));
            review.put("text", text);
            review.put("title", getStringValue(row, prefix + "title"));
            review.put("word_count", getDoubleValue(row, prefix + "word_count"));
        }

        return review;
    }

    private String getStringValue(String[] row, String columnName) {
        int index = getColumnIndex(columnName);
        if (index >= 0 && index < row.length) {
            String value = row[index];
            return (value != null && !value.trim().isEmpty() && !value.equals("null")) ? value.trim() : null;
        }
        return null;
    }

    private Double getDoubleValue(String[] row, String columnName) {
        String value = getStringValue(row, columnName);
        if (value != null) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                logger.debug("Impossible de convertir '{}' en Double pour la colonne '{}'", value, columnName);
            }
        }
        return null;
    }

    private Integer getIntegerValue(String[] row, String columnName) {
        String value = getStringValue(row, columnName);
        if (value != null) {
            try {
                return (int) Double.parseDouble(value); // Conversion via double pour gérer les décimaux
            } catch (NumberFormatException e) {
                logger.debug("Impossible de convertir '{}' en Integer pour la colonne '{}'", value, columnName);
            }
        }
        return null;
    }

    private Boolean getBooleanValue(String[] row, String columnName) {
        String value = getStringValue(row, columnName);
        if (value != null) {
            return "true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value);
        }
        return null;
    }

    /**
     * Obtient l'index d'une colonne par son nom
     */
    private int getColumnIndex(String columnName) {
        Map<String, Integer> columnIndexMap = createColumnIndexMap();
        return columnIndexMap.getOrDefault(columnName, -1);
    }

    /**
     * Crée la map des indices de colonnes
     */
    private Map<String, Integer> createColumnIndexMap() {
        Map<String, Integer> map = new HashMap<>();

        // Liste des colonnes dans l'ordre du CSV
        String[] columns = {
                "address", "avg_reviews_per_month", "business_age_days", "business_age_years",
                "business_size_indicator", "categories", "competitor_count", "contact_completeness",
                "domain", "email", "enhanced_review_10_date", "enhanced_review_10_has_title",
                "enhanced_review_10_helpful_votes", "enhanced_review_10_is_verified",
                "enhanced_review_10_rating", "enhanced_review_10_reviewer_location",
                "enhanced_review_10_reviewer_name", "enhanced_review_10_text",
                "enhanced_review_10_title", "enhanced_review_10_word_count",
                "enhanced_review_1_date", "enhanced_review_1_has_title",
                "enhanced_review_1_helpful_votes", "enhanced_review_1_is_verified",
                "enhanced_review_1_rating", "enhanced_review_1_reviewer_location",
                "enhanced_review_1_reviewer_name", "enhanced_review_1_text",
                "enhanced_review_1_title", "enhanced_review_1_word_count",
                "enhanced_review_2_date", "enhanced_review_2_has_title",
                "enhanced_review_2_helpful_votes", "enhanced_review_2_is_verified",
                "enhanced_review_2_rating", "enhanced_review_2_reviewer_location",
                "enhanced_review_2_reviewer_name", "enhanced_review_2_text",
                "enhanced_review_2_title", "enhanced_review_2_word_count",
                "enhanced_review_3_date", "enhanced_review_3_has_title",
                "enhanced_review_3_helpful_votes", "enhanced_review_3_is_verified",
                "enhanced_review_3_rating", "enhanced_review_3_reviewer_location",
                "enhanced_review_3_reviewer_name", "enhanced_review_3_text",
                "enhanced_review_3_title", "enhanced_review_3_word_count",
                "enhanced_review_4_date", "enhanced_review_4_has_title",
                "enhanced_review_4_helpful_votes", "enhanced_review_4_is_verified",
                "enhanced_review_4_rating", "enhanced_review_4_reviewer_location",
                "enhanced_review_4_reviewer_name", "enhanced_review_4_text",
                "enhanced_review_4_title", "enhanced_review_4_word_count",
                "enhanced_review_5_date", "enhanced_review_5_has_title",
                "enhanced_review_5_helpful_votes", "enhanced_review_5_is_verified",
                "enhanced_review_5_rating", "enhanced_review_5_reviewer_location",
                "enhanced_review_5_reviewer_name", "enhanced_review_5_text",
                "enhanced_review_5_title", "enhanced_review_5_word_count",
                "enhanced_review_6_date", "enhanced_review_6_has_title",
                "enhanced_review_6_helpful_votes", "enhanced_review_6_is_verified",
                "enhanced_review_6_rating", "enhanced_review_6_reviewer_location",
                "enhanced_review_6_reviewer_name", "enhanced_review_6_text",
                "enhanced_review_6_title", "enhanced_review_6_word_count",
                "enhanced_review_7_date", "enhanced_review_7_has_title",
                "enhanced_review_7_helpful_votes", "enhanced_review_7_is_verified",
                "enhanced_review_7_rating", "enhanced_review_7_reviewer_location",
                "enhanced_review_7_reviewer_name", "enhanced_review_7_text",
                "enhanced_review_7_title", "enhanced_review_7_word_count",
                "enhanced_review_8_date", "enhanced_review_8_has_title",
                "enhanced_review_8_helpful_votes", "enhanced_review_8_is_verified",
                "enhanced_review_8_rating", "enhanced_review_8_reviewer_location",
                "enhanced_review_8_reviewer_name", "enhanced_review_8_text",
                "enhanced_review_8_title", "enhanced_review_8_word_count",
                "enhanced_review_9_date", "enhanced_review_9_has_title",
                "enhanced_review_9_helpful_votes", "enhanced_review_9_is_verified",
                "enhanced_review_9_rating", "enhanced_review_9_reviewer_location",
                "enhanced_review_9_reviewer_name", "enhanced_review_9_text",
                "enhanced_review_9_title", "enhanced_review_9_word_count",
                "facebook_url", "five_star_reviews_1_date", "five_star_reviews_1_has_title",
                "five_star_reviews_1_helpful_votes", "five_star_reviews_1_is_verified",
                "five_star_reviews_1_rating", "five_star_reviews_1_reviewer_location",
                "five_star_reviews_1_reviewer_name", "five_star_reviews_1_text",
                "five_star_reviews_1_title", "five_star_reviews_1_word_count",
                "five_star_reviews_2_date", "five_star_reviews_2_has_title",
                "five_star_reviews_2_helpful_votes", "five_star_reviews_2_is_verified",
                "five_star_reviews_2_rating", "five_star_reviews_2_reviewer_location",
                "five_star_reviews_2_reviewer_name", "five_star_reviews_2_text",
                "five_star_reviews_2_title", "five_star_reviews_2_word_count",
                "five_star_reviews_3_date", "five_star_reviews_3_has_title",
                "five_star_reviews_3_helpful_votes", "five_star_reviews_3_is_verified",
                "five_star_reviews_3_rating", "five_star_reviews_3_reviewer_location",
                "five_star_reviews_3_reviewer_name", "five_star_reviews_3_text",
                "five_star_reviews_3_title", "five_star_reviews_3_word_count",
                "five_star_reviews_4_date", "five_star_reviews_4_has_title",
                "five_star_reviews_4_helpful_votes", "five_star_reviews_4_is_verified",
                "five_star_reviews_4_rating", "five_star_reviews_4_reviewer_location",
                "five_star_reviews_4_reviewer_name", "five_star_reviews_4_text",
                "five_star_reviews_4_title", "five_star_reviews_4_word_count",
                "five_star_reviews_5_date", "five_star_reviews_5_has_title",
                "five_star_reviews_5_helpful_votes", "five_star_reviews_5_is_verified",
                "five_star_reviews_5_rating", "five_star_reviews_5_reviewer_location",
                "five_star_reviews_5_reviewer_name", "five_star_reviews_5_text",
                "five_star_reviews_5_title", "five_star_reviews_5_word_count",
                "has_social_media", "instagram_url", "is_claimed", "linkedin_url",
                "logo_url", "name", "num_reviews", "number_of_reviews",
                "one_star_reviews_1_date", "one_star_reviews_1_has_title",
                "one_star_reviews_1_helpful_votes", "one_star_reviews_1_is_verified",
                "one_star_reviews_1_rating", "one_star_reviews_1_reviewer_location",
                "one_star_reviews_1_reviewer_name", "one_star_reviews_1_text",
                "one_star_reviews_1_title", "one_star_reviews_1_word_count",
                "one_star_reviews_2_date", "one_star_reviews_2_has_title",
                "one_star_reviews_2_helpful_votes", "one_star_reviews_2_is_verified",
                "one_star_reviews_2_rating", "one_star_reviews_2_reviewer_location",
                "one_star_reviews_2_reviewer_name", "one_star_reviews_2_text",
                "one_star_reviews_2_title", "one_star_reviews_2_word_count",
                "one_star_reviews_3_date", "one_star_reviews_3_has_title",
                "one_star_reviews_3_helpful_votes", "one_star_reviews_3_is_verified",
                "one_star_reviews_3_rating", "one_star_reviews_3_reviewer_location",
                "one_star_reviews_3_reviewer_name", "one_star_reviews_3_text",
                "one_star_reviews_3_title", "one_star_reviews_3_word_count",
                "one_star_reviews_4_date", "one_star_reviews_4_has_title",
                "one_star_reviews_4_helpful_votes", "one_star_reviews_4_is_verified",
                "one_star_reviews_4_rating", "one_star_reviews_4_reviewer_location",
                "one_star_reviews_4_reviewer_name", "one_star_reviews_4_text",
                "one_star_reviews_4_title", "one_star_reviews_4_word_count",
                "one_star_reviews_5_date", "one_star_reviews_5_has_title",
                "one_star_reviews_5_helpful_votes", "one_star_reviews_5_is_verified",
                "one_star_reviews_5_rating", "one_star_reviews_5_reviewer_location",
                "one_star_reviews_5_reviewer_name", "one_star_reviews_5_text",
                "one_star_reviews_5_title", "one_star_reviews_5_word_count",
                "phone", "response_rate", "reviews", "reviews_last_30_days",
                "scrape_timestamp", "sentiment_distribution_1_star_avg_words",
                "sentiment_distribution_1_star_count", "sentiment_distribution_2_star_avg_words",
                "sentiment_distribution_2_star_count", "sentiment_distribution_3_star_avg_words",
                "sentiment_distribution_3_star_count", "sentiment_distribution_4_star_avg_words",
                "sentiment_distribution_4_star_count", "sentiment_distribution_5_star_avg_words",
                "sentiment_distribution_5_star_count", "similar_companies_1_name",
                "similar_companies_1_url", "similar_companies_2_name",
                "similar_companies_2_url", "similar_companies_3_name",
                "similar_companies_3_url", "similar_companies_4_name",
                "similar_companies_4_url", "similar_companies_5_name",
                "similar_companies_5_url", "star_rating_percentages_five",
                "star_rating_percentages_four", "star_rating_percentages_one",
                "star_rating_percentages_three", "star_rating_percentages_two",
                "star_ratings_five", "star_ratings_four", "star_ratings_one",
                "star_ratings_three", "star_ratings_two", "total_categories",
                "total_helpful_votes", "trustpilot_domain", "trustscore",
                "twitter_url", "verified_reviews_count", "website", "youtube_url"
        };

        for (int i = 0; i < columns.length; i++) {
            map.put(columns[i], i);
        }

        return map;
    }

    /**
     * 🔧 CLASSE MODIFIÉE: ImportResult avec category et subcategory
     */
    public static class ImportResult {
        private String fileName;
        private String category;        // 🆕 NOUVEAU
        private String subcategory;     // 🆕 NOUVEAU
        private LocalDateTime startTime;
        private LocalDateTime endTime;
        private int totalRows;
        private int successCount;
        private int errorCount;
        private List<String> errors = new ArrayList<>();

        // Getters et setters existants
        public String getFileName() { return fileName; }
        public void setFileName(String fileName) { this.fileName = fileName; }

        // 🆕 NOUVEAUX GETTERS/SETTERS
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }

        public String getSubcategory() { return subcategory; }
        public void setSubcategory(String subcategory) { this.subcategory = subcategory; }

        public LocalDateTime getStartTime() { return startTime; }
        public void setStartTime(LocalDateTime startTime) { this.startTime = startTime; }

        public LocalDateTime getEndTime() { return endTime; }
        public void setEndTime(LocalDateTime endTime) { this.endTime = endTime; }

        public int getTotalRows() { return totalRows; }
        public void setTotalRows(int totalRows) { this.totalRows = totalRows; }

        public int getSuccessCount() { return successCount; }
        public void incrementSuccessCount() { this.successCount++; }

        public int getErrorCount() { return errorCount; }
        public void incrementErrorCount() { this.errorCount++; }

        public List<String> getErrors() { return errors; }
        public void addError(String error) { this.errors.add(error); }

        public long getDurationInSeconds() {
            if (startTime != null && endTime != null) {
                return java.time.Duration.between(startTime, endTime).getSeconds();
            }
            return 0;
        }

        @Override
        public String toString() {
            return String.format(
                    "ImportResult{fileName='%s', category='%s', subcategory='%s', totalRows=%d, success=%d, errors=%d, duration=%ds}",
                    fileName, category, subcategory, totalRows, successCount, errorCount, getDurationInSeconds()
            );
        }
    }
}