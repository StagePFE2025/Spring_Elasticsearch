package com.example.springelasticproject.util;

import com.example.springelasticproject.model.b2bModel.ShadowPilot.ShadowPilot;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utilitaire amélioré pour transformer les données CSV en objets FlexibleBusinessReview
 * Compatible avec toutes les 262 colonnes du fichier shadowpilot_v5_sales_marketing.csv
 *
 * Améliorations:
 * - Import dynamique de toutes les colonnes
 * - Catégorisation automatique des colonnes
 * - Gestion robuste des types de données
 * - Validation complète des données
 */
@Component
public class CsvToBusinessReviewTransformer {

    // Patterns pour identifier les types de colonnes
    private static final Pattern ENHANCED_REVIEW_PATTERN = Pattern.compile("^enhanced_review_(\\d+)_(.+)$");
    private static final Pattern FIVE_STAR_REVIEW_PATTERN = Pattern.compile("^five_star_reviews_(\\d+)_(.+)$");
    private static final Pattern ONE_STAR_REVIEW_PATTERN = Pattern.compile("^one_star_reviews_(\\d+)_(.+)$");
    private static final Pattern SENTIMENT_PATTERN = Pattern.compile("^sentiment_distribution_(\\d+)_star_(.+)$");
    private static final Pattern STAR_RATING_PATTERN = Pattern.compile("^star_rating(?:s|_percentages)_(.+)$");
    private static final Pattern SIMILAR_COMPANY_PATTERN = Pattern.compile("^similar_companies_(\\d+)_(.+)$");

    // Colonnes de base à traiter directement
    private static final Set<String> DIRECT_FIELDS = Set.of(
            "name", "address", "domain", "phone", "email", "website"
    );

    // Colonnes de social media
    private static final Set<String> SOCIAL_MEDIA_FIELDS = Set.of(
            "facebook_url", "instagram_url", "linkedin_url", "twitter_url",
            "youtube_url", "has_social_media"
    );

    // Colonnes de métriques business (liste exhaustive)
    private static final Set<String> BUSINESS_METRIC_FIELDS = Set.of(
            "trustscore", "number_of_reviews", "avg_reviews_per_month", "business_age_days",
            "business_age_years", "competitor_count", "contact_completeness", "response_rate",
            "reviews_last_30_days", "total_categories", "total_helpful_votes",
            "verified_reviews_count", "business_size_indicator", "categories", "is_claimed",
            "trustpilot_domain", "logo_url", "num_reviews", "reviews", "scrape_timestamp"
    );

    /**
     * Transforme une ligne CSV (Map) en FlexibleBusinessReview avec import de toutes les colonnes
     */
    public ShadowPilot transformCsvRow(Map<String, String> csvRow) {
        ShadowPilot business = new ShadowPilot();

        // ===== TRAITEMENT DYNAMIQUE DE TOUTES LES COLONNES =====
        Set<String> processedColumns = new HashSet<>();

        // 1. Traiter les champs directs
        extractDirectFields(csvRow, business, processedColumns);

        // 2. Traiter les métriques business (avec toutes les colonnes restantes)
        Map<String, Object> businessMetrics = extractAllBusinessMetrics(csvRow, processedColumns);
        business.setBusinessMetrics(businessMetrics);

        // 3. Traiter les réseaux sociaux
        Map<String, Object> socialMedia = extractSocialMedia(csvRow, processedColumns);
        business.setSocialMedia(socialMedia);

        // 4. Traiter tous les types d'avis
        business.setEnhancedReviews(extractReviewsByPattern(csvRow, ENHANCED_REVIEW_PATTERN, processedColumns));
        business.setFiveStarReviews(extractReviewsByPattern(csvRow, FIVE_STAR_REVIEW_PATTERN, processedColumns));
        business.setOneStarReviews(extractReviewsByPattern(csvRow, ONE_STAR_REVIEW_PATTERN, processedColumns));

        // 5. Traiter la distribution des sentiments
        Map<String, Object> sentimentDistribution = extractSentimentDistribution(csvRow, processedColumns);
        business.setSentimentDistribution(sentimentDistribution);

        // 6. Traiter les évaluations par étoiles
        Map<String, Object> starRatings = extractStarRatings(csvRow, processedColumns);
        business.setStarRatings(starRatings);

        // 7. Traiter les entreprises similaires
        List<Map<String, Object>> similarCompanies = extractSimilarCompanies(csvRow, processedColumns);
        business.setSimilarCompanies(similarCompanies);

        // 8. Traiter les colonnes non catégorisées (les ajouter aux business metrics)
        extractRemainingColumns(csvRow, processedColumns, businessMetrics);

        return business;
    }

    /**
     * Extrait les champs directs
     */
    private void extractDirectFields(Map<String, String> csvRow, ShadowPilot business,
                                     Set<String> processedColumns) {
        business.setName(csvRow.get("name"));
        business.setAddress(csvRow.get("address"));
        business.setDomain(csvRow.get("domain"));
        business.setPhone(csvRow.get("phone"));
        business.setEmail(csvRow.get("email"));
        business.setWebsite(csvRow.get("website"));

        processedColumns.addAll(DIRECT_FIELDS);
    }

    /**
     * Extrait toutes les métriques business de manière dynamique
     */
    private Map<String, Object> extractAllBusinessMetrics(Map<String, String> csvRow,
                                                          Set<String> processedColumns) {
        Map<String, Object> metrics = new HashMap<>();

        // Traiter toutes les colonnes de métriques définies
        for (String field : BUSINESS_METRIC_FIELDS) {
            if (csvRow.containsKey(field)) {
                Object value = parseValueByFieldName(field, csvRow.get(field));
                putIfNotEmpty(metrics, field, value);
                processedColumns.add(field);
            }
        }

        return metrics;
    }

    /**
     * Extrait les données de réseaux sociaux
     */
    private Map<String, Object> extractSocialMedia(Map<String, String> csvRow,
                                                   Set<String> processedColumns) {
        Map<String, Object> socialMedia = new HashMap<>();

        for (String field : SOCIAL_MEDIA_FIELDS) {
            if (csvRow.containsKey(field)) {
                Object value = parseValueByFieldName(field, csvRow.get(field));
                putIfNotEmpty(socialMedia, field, value);
                processedColumns.add(field);
            }
        }

        return socialMedia;
    }

    /**
     * Extrait les avis selon un pattern donné
     */
    private List<Map<String, Object>> extractReviewsByPattern(Map<String, String> csvRow,
                                                              Pattern pattern,
                                                              Set<String> processedColumns) {
        Map<Integer, Map<String, Object>> reviewsMap = new HashMap<>();

        for (Map.Entry<String, String> entry : csvRow.entrySet()) {
            String columnName = entry.getKey();
            var matcher = pattern.matcher(columnName);

            if (matcher.matches()) {
                int reviewIndex = Integer.parseInt(matcher.group(1));
                String fieldName = matcher.group(2);
                String value = entry.getValue();

                reviewsMap.computeIfAbsent(reviewIndex, k -> new HashMap<>());

                Object parsedValue = parseReviewField(fieldName, value);
                putIfNotEmpty(reviewsMap.get(reviewIndex), fieldName, parsedValue);

                processedColumns.add(columnName);
            }
        }

        // Convertir en liste ordonnée et filtrer les avis vides
        return reviewsMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .filter(review -> !review.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Extrait la distribution des sentiments de manière dynamique
     */
    private Map<String, Object> extractSentimentDistribution(Map<String, String> csvRow,
                                                             Set<String> processedColumns) {
        Map<String, Object> sentiment = new HashMap<>();

        for (Map.Entry<String, String> entry : csvRow.entrySet()) {
            String columnName = entry.getKey();
            var matcher = SENTIMENT_PATTERN.matcher(columnName);

            if (matcher.matches()) {
                String starLevel = matcher.group(1);
                String metricType = matcher.group(2);
                String value = entry.getValue();

                String key = starLevel + "_star_" + metricType;
                Object parsedValue = parseValueByFieldName(metricType, value);
                putIfNotEmpty(sentiment, key, parsedValue);

                processedColumns.add(columnName);
            }
        }

        return sentiment;
    }

    /**
     * Extrait les évaluations par étoiles de manière dynamique
     */
    private Map<String, Object> extractStarRatings(Map<String, String> csvRow,
                                                   Set<String> processedColumns) {
        Map<String, Object> starRatings = new HashMap<>();

        for (Map.Entry<String, String> entry : csvRow.entrySet()) {
            String columnName = entry.getKey();
            var matcher = STAR_RATING_PATTERN.matcher(columnName);

            if (matcher.matches()) {
                String starType = matcher.group(1);
                String value = entry.getValue();

                String key = columnName.contains("percentages") ?
                        "percentages_" + starType : "ratings_" + starType;

                putIfNotEmpty(starRatings, key, value);
                processedColumns.add(columnName);
            }
        }

        return starRatings;
    }

    /**
     * Extrait les entreprises similaires de manière dynamique
     */
    private List<Map<String, Object>> extractSimilarCompanies(Map<String, String> csvRow,
                                                              Set<String> processedColumns) {
        Map<Integer, Map<String, Object>> companiesMap = new HashMap<>();

        for (Map.Entry<String, String> entry : csvRow.entrySet()) {
            String columnName = entry.getKey();
            var matcher = SIMILAR_COMPANY_PATTERN.matcher(columnName);

            if (matcher.matches()) {
                int companyIndex = Integer.parseInt(matcher.group(1));
                String fieldName = matcher.group(2);
                String value = entry.getValue();

                companiesMap.computeIfAbsent(companyIndex, k -> new HashMap<>());
                putIfNotEmpty(companiesMap.get(companyIndex), fieldName, value);

                processedColumns.add(columnName);
            }
        }

        return companiesMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .filter(company -> !company.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Traite les colonnes restantes non catégorisées
     */
    private void extractRemainingColumns(Map<String, String> csvRow,
                                         Set<String> processedColumns,
                                         Map<String, Object> businessMetrics) {
        for (Map.Entry<String, String> entry : csvRow.entrySet()) {
            String columnName = entry.getKey();
            if (!processedColumns.contains(columnName)) {
                String value = entry.getValue();
                Object parsedValue = parseValueByFieldName(columnName, value);
                putIfNotEmpty(businessMetrics, columnName, parsedValue);
            }
        }
    }

    /**
     * Parse une valeur selon le nom du champ
     */
    private Object parseValueByFieldName(String fieldName, String value) {
        if (!isNotEmpty(value)) {
            return null;
        }

        // Champs booléens
        if (fieldName.matches(".*(is_|has_).*") ||
                fieldName.equals("is_claimed") ||
                fieldName.equals("has_social_media") ||
                fieldName.equals("is_verified") ||
                fieldName.equals("has_title")) {
            return parseBoolean(value);
        }

        // Champs numériques (float)
        if (fieldName.matches(".*(score|rate|count|age|votes|rating|completeness).*") ||
                fieldName.matches(".*_\\d+") ||
                fieldName.matches(".*(avg_|number_of_|total_).*")) {
            // Essayer d'abord Integer, puis Double
            Integer intValue = parseInt(value);
            if (intValue != null) {
                return intValue;
            }
            return parseDouble(value);
        }

        // Champs de date
        if (fieldName.contains("date") || fieldName.contains("timestamp")) {
            return parseDate(value);
        }

        // Par défaut, retourner la chaîne
        return value;
    }

    /**
     * Parse un champ spécifique d'avis
     */
    private Object parseReviewField(String fieldName, String value) {
        switch (fieldName) {
            case "has_title":
            case "is_verified":
                return parseBoolean(value);
            case "helpful_votes":
            case "rating":
            case "word_count":
                return parseDouble(value);
            case "date":
                return parseDate(value);
            default:
                return value;
        }
    }

    /**
     * Parse une date avec plusieurs formats possibles
     */
    private String parseDate(String value) {
        if (!isNotEmpty(value)) {
            return null;
        }

        // Formats de date couramment utilisés
        String[] dateFormats = {
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss.SSS",
                "yyyy-MM-dd",
                "dd/MM/yyyy",
                "MM/dd/yyyy"
        };

        for (String format : dateFormats) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
                LocalDateTime.parse(value, formatter);
                return value; // Retourner la valeur originale si elle est valide
            } catch (DateTimeParseException e) {
                // Continuer avec le format suivant
            }
        }

        // Si aucun format ne correspond, retourner la valeur originale
        return value;
    }

    /**
     * Transforme une liste de lignes CSV en liste de FlexibleBusinessReview
     */
    public List<ShadowPilot> transformCsvData(List<Map<String, String>> csvData) {
        return csvData.stream()
                .map(this::transformCsvRow)
                .collect(Collectors.toList());
    }

    /**
     * Génère un rapport détaillé sur les colonnes traitées
     */
    public Map<String, Object> generateColumnProcessingReport(Map<String, String> sampleRow) {
        Map<String, Object> report = new HashMap<>();
        Set<String> processedColumns = new HashSet<>();

        // Simuler le traitement pour identifier les colonnes
        ShadowPilot business = new ShadowPilot();
        extractDirectFields(sampleRow, business, processedColumns);
        extractAllBusinessMetrics(sampleRow, processedColumns);
        extractSocialMedia(sampleRow, processedColumns);
        extractReviewsByPattern(sampleRow, ENHANCED_REVIEW_PATTERN, processedColumns);
        extractReviewsByPattern(sampleRow, FIVE_STAR_REVIEW_PATTERN, processedColumns);
        extractReviewsByPattern(sampleRow, ONE_STAR_REVIEW_PATTERN, processedColumns);
        extractSentimentDistribution(sampleRow, processedColumns);
        extractStarRatings(sampleRow, processedColumns);
        extractSimilarCompanies(sampleRow, processedColumns);

        Set<String> remainingColumns = new HashSet<>(sampleRow.keySet());
        remainingColumns.removeAll(processedColumns);

        report.put("totalColumns", sampleRow.size());
        report.put("processedColumns", processedColumns.size());
        report.put("remainingColumns", remainingColumns.size());
        report.put("directFields", DIRECT_FIELDS.size());
        report.put("businessMetricFields", BUSINESS_METRIC_FIELDS.size());
        report.put("socialMediaFields", SOCIAL_MEDIA_FIELDS.size());
        report.put("remainingColumnsList", new ArrayList<>(remainingColumns));
        report.put("coveragePercentage",
                sampleRow.size() > 0 ? (double) processedColumns.size() / sampleRow.size() * 100 : 0);

        return report;
    }

    // ===== MÉTHODES UTILITAIRES =====

    private Double parseDouble(String value) {
        if (!isNotEmpty(value)) {
            return null;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Integer parseInt(String value) {
        if (!isNotEmpty(value)) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Boolean parseBoolean(String value) {
        if (!isNotEmpty(value)) {
            return null;
        }

        value = value.toLowerCase().trim();
        if ("true".equals(value) || "1".equals(value) || "yes".equals(value)) {
            return true;
        } else if ("false".equals(value) || "0".equals(value) || "no".equals(value)) {
            return false;
        }

        return null;
    }

    private boolean isNotEmpty(String value) {
        return value != null && !value.trim().isEmpty() && !"null".equalsIgnoreCase(value.trim());
    }

    private void putIfNotEmpty(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            if (value instanceof String) {
                if (isNotEmpty((String) value)) {
                    map.put(key, value);
                }
            } else {
                map.put(key, value);
            }
        }
    }

    public boolean validateTransformedData(ShadowPilot business) {
        if (business.getName() == null || business.getName().trim().isEmpty()) {
            return false;
        }

        Double trustscore = business.getTrustscore();
        if (trustscore != null && (trustscore < 0 || trustscore > 5)) {
            return false;
        }

        Double reviews = business.getNumberOfReviews();
        if (reviews != null && reviews < 0) {
            return false;
        }

        return true;
    }

    public Map<String, Object> generateTransformationReport(List<ShadowPilot> transformedData) {
        Map<String, Object> report = new HashMap<>();

        int total = transformedData.size();
        int valid = 0;
        int withSocialMedia = 0;
        int withReviews = 0;
        int withSimilarCompanies = 0;

        for (ShadowPilot business : transformedData) {
            if (validateTransformedData(business)) {
                valid++;
            }

            if (business.hasSocialMedia()) {
                withSocialMedia++;
            }

            if (!business.getEnhancedReviews().isEmpty()) {
                withReviews++;
            }

            if (!business.getSimilarCompanies().isEmpty()) {
                withSimilarCompanies++;
            }
        }

        report.put("totalRecords", total);
        report.put("validRecords", valid);
        report.put("invalidRecords", total - valid);
        report.put("recordsWithSocialMedia", withSocialMedia);
        report.put("recordsWithReviews", withReviews);
        report.put("recordsWithSimilarCompanies", withSimilarCompanies);
        report.put("validationRate", total > 0 ? (double) valid / total * 100 : 0);
        report.put("socialMediaRate", total > 0 ? (double) withSocialMedia / total * 100 : 0);
        report.put("reviewsRate", total > 0 ? (double) withReviews / total * 100 : 0);

        return report;
    }
}