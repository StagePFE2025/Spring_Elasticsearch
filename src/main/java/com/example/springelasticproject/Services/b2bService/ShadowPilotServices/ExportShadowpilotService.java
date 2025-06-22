package com.example.springelasticproject.Services.b2bService.ShadowPilotServices;

import com.example.springelasticproject.model.b2bModel.ShadowPilot.ShadowPilot;
import com.example.springelasticproject.repository.ShadowPilotRepository.ShadowPilotRepository;
import com.opencsv.CSVWriter;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public class ExportShadowpilotService {
    private final ShadowPilotRepository shadowPilotRepository;
    private final ShadowPilotService shadowPilotService;
    private final ElasticsearchOperations elasticsearchOperations;

    @Autowired
    public ExportShadowpilotService (ShadowPilotService shadowPilotService , ShadowPilotRepository shadowPilotRepository,ElasticsearchOperations elasticsearchOperations){
        this.shadowPilotRepository=shadowPilotRepository;
        this.shadowPilotService=shadowPilotService;
        this.elasticsearchOperations=elasticsearchOperations;
    }

    @Async
    public void ExportSearchDataShadowPilotFuzzy(Map<String, String> attributes, HttpServletResponse response) throws IOException {

        response.setContentType("text/csv");
        response.setHeader("Content-Disposition", "attachment; filename=\"shadowpilot-search-results-fuzzy.csv\"");

        try (CSVWriter csvWriter = new CSVWriter(response.getWriter())) {
            String[] header = {
                    "ID", "Name", "Address", "Domain", "Phone", "Email", "Website",
                    "Trust Score", "Number of Reviews", "Social Media Platforms",
                    "Enhanced Reviews Count", "Five Star Reviews Count", "One Star Reviews Count",
                    "Dominant Sentiment", "Star Distribution", "Similar Companies Count",
                    "Business Metrics", "Social Media Details", "Review Summary"
            };
            csvWriter.writeNext(header);

            try {
                final int BATCH_SIZE = 1000;
                int currentPage = 0;
                boolean hasMoreResults = true;
                int totalProcessed = 0;

                System.out.println("Début de l'export ShadowPilot Fuzzy...");

                while (hasMoreResults) {
                    try {
                        // UTILISER VOTRE FONCTION DE RECHERCHE FUZZY EXISTANTE
                        Pageable pageable = PageRequest.of(currentPage, BATCH_SIZE);
                        Map<String, Object> searchResult = shadowPilotService.searchShadowPilotByAttributesFuzzy(attributes, pageable);

                        Page<ShadowPilot> page = (Page<ShadowPilot>) searchResult.get("page");
                        List<ShadowPilot> hits = page.getContent();

                        if (hits.isEmpty()) {
                            hasMoreResults = false;
                            break;
                        }

                        // Traitement des résultats
                        for (ShadowPilot shadowPilot : hits) {
                            if (shadowPilot != null) {
                                String[] row = {
                                        nullSafe(shadowPilot.getId()),
                                        nullSafe(shadowPilot.getName()),
                                        nullSafe(shadowPilot.getAddress()),
                                        nullSafe(shadowPilot.getDomain()),
                                        nullSafe(shadowPilot.getPhone()),
                                        nullSafe(shadowPilot.getEmail()),
                                        nullSafe(shadowPilot.getWebsite()),
                                        extractBusinessMetric(shadowPilot.getBusinessMetrics(), "trustscore"),
                                        extractBusinessMetric(shadowPilot.getBusinessMetrics(), "number_of_reviews"),
                                        socialMediaPlatformsAsString(shadowPilot.getSocialMedia()),
                                        shadowPilot.getEnhancedReviews() != null ? String.valueOf(shadowPilot.getEnhancedReviews().size()) : "0",
                                        shadowPilot.getFiveStarReviews() != null ? String.valueOf(shadowPilot.getFiveStarReviews().size()) : "0",
                                        shadowPilot.getOneStarReviews() != null ? String.valueOf(shadowPilot.getOneStarReviews().size()) : "0",
                                        extractDominantSentiment(shadowPilot.getSentimentDistribution()),
                                        starRatingsAsString(shadowPilot.getStarRatings()),
                                        shadowPilot.getSimilarCompanies() != null ? String.valueOf(shadowPilot.getSimilarCompanies().size()) : "0",
                                        businessMetricsAsString(shadowPilot.getBusinessMetrics()),
                                        socialMediaDetailsAsString(shadowPilot.getSocialMedia()),
                                        reviewsSummaryAsString(shadowPilot.getEnhancedReviews())
                                };
                                csvWriter.writeNext(row);
                                totalProcessed++;
                            }
                        }

                        // Vérifier s'il y a plus de résultats
                        if (hits.size() < BATCH_SIZE || page.isLast()) {
                            hasMoreResults = false;
                        } else {
                            currentPage++;
                        }

                        // Limite de sécurité
                        if (totalProcessed >= 100000) {
                            System.out.println("Limite de 100,000 enregistrements atteinte, arrêt de l'export fuzzy ShadowPilot.");
                            break;
                        }

                    } catch (Exception batchException) {
                        System.err.println("Erreur batch fuzzy ShadowPilot " + (currentPage + 1) + ": " + batchException.getMessage());
                        if (batchException.getMessage().contains("search_phase_execution_exception")) {
                            break;
                        }
                        currentPage++;
                    }
                }

                System.out.println("Export ShadowPilot Fuzzy terminé. Total: " + totalProcessed + " enregistrements.");

                if (totalProcessed == 0) {
                    String[] noResultRow = {"Aucun résultat trouvé (fuzzy)", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""};
                    csvWriter.writeNext(noResultRow);
                }

            } catch (Exception e) {
                System.err.println("Erreur export ShadowPilot Fuzzy: " + e.getMessage());
                String[] errorRow = {"Erreur fuzzy: " + e.getMessage(), "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""};
                csvWriter.writeNext(errorRow);
            }
        }
    }

// ===================== EXPORT EXACT (utilise votre fonction existante) =====================

    @Async
    public void ExportSearchDataShadowPilotExact(Map<String, String> attributes, HttpServletResponse response) throws IOException {

        response.setContentType("text/csv");
        response.setHeader("Content-Disposition", "attachment; filename=\"shadowpilot-search-results-exact.csv\"");

        try (CSVWriter csvWriter = new CSVWriter(response.getWriter())) {
            String[] header = {
                    "ID", "Name", "Address", "Domain", "Phone", "Email", "Website",
                    "Trust Score", "Number of Reviews", "Social Media Platforms",
                    "Enhanced Reviews Count", "Five Star Reviews Count", "One Star Reviews Count",
                    "Dominant Sentiment", "Star Distribution", "Similar Companies Count",
                    "Business Metrics", "Social Media Details", "Review Summary"
            };
            csvWriter.writeNext(header);

            try {
                final int BATCH_SIZE = 1000;
                int currentPage = 0;
                boolean hasMoreResults = true;
                int totalProcessed = 0;

                System.out.println("Début de l'export ShadowPilot Exact...");

                while (hasMoreResults) {
                    try {
                        // UTILISER VOTRE FONCTION DE RECHERCHE EXACT EXISTANTE
                        Pageable pageable = PageRequest.of(currentPage, BATCH_SIZE);
                        Map<String, Object> searchResult = shadowPilotService.searchShadowPilotByAttributesExact(attributes, pageable);

                        Page<ShadowPilot> page = (Page<ShadowPilot>) searchResult.get("page");
                        List<ShadowPilot> hits = page.getContent();

                        if (hits.isEmpty()) {
                            hasMoreResults = false;
                            break;
                        }

                        // Traitement identique à la version fuzzy
                        for (ShadowPilot shadowPilot : hits) {
                            if (shadowPilot != null) {
                                String[] row = {
                                        nullSafe(shadowPilot.getId()),
                                        nullSafe(shadowPilot.getName()),
                                        nullSafe(shadowPilot.getAddress()),
                                        nullSafe(shadowPilot.getDomain()),
                                        nullSafe(shadowPilot.getPhone()),
                                        nullSafe(shadowPilot.getEmail()),
                                        nullSafe(shadowPilot.getWebsite()),
                                        extractBusinessMetric(shadowPilot.getBusinessMetrics(), "trustscore"),
                                        extractBusinessMetric(shadowPilot.getBusinessMetrics(), "number_of_reviews"),
                                        socialMediaPlatformsAsString(shadowPilot.getSocialMedia()),
                                        shadowPilot.getEnhancedReviews() != null ? String.valueOf(shadowPilot.getEnhancedReviews().size()) : "0",
                                        shadowPilot.getFiveStarReviews() != null ? String.valueOf(shadowPilot.getFiveStarReviews().size()) : "0",
                                        shadowPilot.getOneStarReviews() != null ? String.valueOf(shadowPilot.getOneStarReviews().size()) : "0",
                                        extractDominantSentiment(shadowPilot.getSentimentDistribution()),
                                        starRatingsAsString(shadowPilot.getStarRatings()),
                                        shadowPilot.getSimilarCompanies() != null ? String.valueOf(shadowPilot.getSimilarCompanies().size()) : "0",
                                        businessMetricsAsString(shadowPilot.getBusinessMetrics()),
                                        socialMediaDetailsAsString(shadowPilot.getSocialMedia()),
                                        reviewsSummaryAsString(shadowPilot.getEnhancedReviews())
                                };
                                csvWriter.writeNext(row);
                                totalProcessed++;
                            }
                        }

                        if (hits.size() < BATCH_SIZE || page.isLast()) {
                            hasMoreResults = false;
                        } else {
                            currentPage++;
                        }

                        if (totalProcessed >= 100000) {
                            System.out.println("Limite de 100,000 enregistrements atteinte, arrêt de l'export exact ShadowPilot.");
                            break;
                        }

                    } catch (Exception batchException) {
                        System.err.println("Erreur batch exact ShadowPilot " + (currentPage + 1) + ": " + batchException.getMessage());
                        if (batchException.getMessage().contains("search_phase_execution_exception")) {
                            break;
                        }
                        currentPage++;
                    }
                }

                System.out.println("Export ShadowPilot Exact terminé. Total: " + totalProcessed + " enregistrements.");

                if (totalProcessed == 0) {
                    String[] noResultRow = {"Aucun résultat trouvé (exact)", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""};
                    csvWriter.writeNext(noResultRow);
                }

            } catch (Exception e) {
                System.err.println("Erreur export ShadowPilot Exact: " + e.getMessage());
                String[] errorRow = {"Erreur exact: " + e.getMessage(), "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""};
                csvWriter.writeNext(errorRow);
            }
        }
    }

// ===================== MÉTHODES UTILITAIRES POUR SHADOWPILOT =====================

    /**
     * Extrait une métrique business spécifique
     */
    private String extractBusinessMetric(Map<String, Object> businessMetrics, String metricName) {
        if (businessMetrics == null || businessMetrics.isEmpty()) {
            return "";
        }
        Object value = businessMetrics.get(metricName);
        return value != null ? value.toString() : "";
    }

    /**
     * Convertit les plateformes de réseaux sociaux en chaîne
     */
    private String socialMediaPlatformsAsString(Map<String, Object> socialMedia) {
        if (socialMedia == null || socialMedia.isEmpty()) {
            return "";
        }
        return String.join(", ", socialMedia.keySet());
    }

    /**
     * Extrait le sentiment dominant
     */
    private String extractDominantSentiment(Map<String, Object> sentimentDistribution) {
        if (sentimentDistribution == null || sentimentDistribution.isEmpty()) {
            return "";
        }

        String dominantSentiment = "";
        double maxValue = 0.0;

        for (Map.Entry<String, Object> entry : sentimentDistribution.entrySet()) {
            try {
                double value = Double.parseDouble(entry.getValue().toString());
                if (value > maxValue) {
                    maxValue = value;
                    dominantSentiment = entry.getKey();
                }
            } catch (NumberFormatException e) {
                // Ignorer les valeurs non numériques
            }
        }

        return dominantSentiment;
    }

    /**
     * Convertit les évaluations par étoiles en chaîne
     */
    private String starRatingsAsString(Map<String, Object> starRatings) {
        if (starRatings == null || starRatings.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : starRatings.entrySet()) {
            if (sb.length() > 0) {
                sb.append("; ");
            }
            sb.append(entry.getKey()).append(": ").append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * Convertit toutes les métriques business en chaîne détaillée
     */
    private String businessMetricsAsString(Map<String, Object> businessMetrics) {
        if (businessMetrics == null || businessMetrics.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : businessMetrics.entrySet()) {
            if (sb.length() > 0) {
                sb.append("; ");
            }
            sb.append(entry.getKey()).append(": ").append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * Convertit les détails des réseaux sociaux en chaîne
     */
    private String socialMediaDetailsAsString(Map<String, Object> socialMedia) {
        if (socialMedia == null || socialMedia.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : socialMedia.entrySet()) {
            if (sb.length() > 0) {
                sb.append("; ");
            }
            sb.append(entry.getKey()).append(": ").append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * Crée un résumé des avis
     */
    private String reviewsSummaryAsString(List<Map<String, Object>> reviews) {
        if (reviews == null || reviews.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (Map<String, Object> review : reviews) {
            if (count >= 3) break; // Limiter à 3 avis pour le résumé

            if (sb.length() > 0) {
                sb.append(" | ");
            }

            Object rating = review.get("rating");
            Object content = review.get("content");
            Object author = review.get("author");

            sb.append("★").append(rating != null ? rating : "?");
            if (author != null) {
                sb.append(" (").append(author).append(")");
            }
            if (content != null) {
                String contentStr = content.toString();
                if (contentStr.length() > 50) {
                    contentStr = contentStr.substring(0, 50) + "...";
                }
                sb.append(": ").append(contentStr);
            }
            count++;
        }

        if (reviews.size() > 3) {
            sb.append(" ... (").append(reviews.size() - 3).append(" autres avis)");
        }

        return sb.toString();
    }

    /**
     * Gestion des valeurs nulles (réutilisation de votre méthode existante)
     */
    private String nullSafe(String value) {
        return value != null ? value : "";
    }
}
