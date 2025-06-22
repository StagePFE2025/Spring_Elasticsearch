package com.example.springelasticproject.Services.b2bService.ShadowPilotServices;


import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.json.JsonData;
import com.example.springelasticproject.model.b2bModel.ShadowPilot.ShadowPilot;

import com.example.springelasticproject.repository.ShadowPilotRepository.ShadowPilotRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;


@Service
@Transactional
public class ShadowPilotService {

    @Autowired
    private ShadowPilotRepository repository;
    @Autowired
    private  ElasticsearchOperations elasticsearchOperations;
    // ===== OPÉRATIONS CRUD =====

    /**
     * Sauvegarde ou met à jour un avis d'entreprise
     */
    public ShadowPilot save(ShadowPilot review) {

        return repository.save(review);
    }

    /**
     * Sauvegarde une liste d'avis en lot
     */
    public Iterable<ShadowPilot> saveAll(List<ShadowPilot> reviews) {
        reviews.forEach(review -> {

        });
        return repository.saveAll(reviews);
    }

    /**
     * Recherche par ID
     */
    @Transactional(readOnly = true)
    public Optional<ShadowPilot> findById(String id) {
        return repository.findById(id);
    }

    /**
     * Recherche toutes les entreprises avec pagination
     */
    @Transactional(readOnly = true)
    public Page<ShadowPilot> findAll(int page, int size) {
        try {
            Pageable pageable = PageRequest.of(page, size);
            return repository.findAll(pageable);
        } catch (Exception e) {
            System.err.println("Even no-sort failed: " + e.getMessage());
            return Page.empty(PageRequest.of(page, size));
        }
    }

    /**
     * Supprime un avis par ID
     */
    public void deleteById(String id) {
        repository.deleteById(id);
    }

    /**
     * Compte le nombre total d'entreprises
     */
    @Transactional(readOnly = true)
    public long count() {
        return repository.count();
    }

    // ===== RECHERCHES MÉTIER =====

    /**
     * Recherche par nom d'entreprise

    @Transactional(readOnly = true)
    public List<ShadowPilot> findByName(String name) {
        return repository.findByNameContainingIgnoreCase(name);
    }*/
    @Transactional(readOnly = true)
    public List<ShadowPilot> findByName(String name) {
        if (name == null || name.trim().length() < 2) {
            return Collections.emptyList();
        }

        String cleanName = name.trim();

        try {
            // Essayer d'abord avec la nouvelle méthode qui supporte les espaces
            return repository.findByNameWithSpaceSupport(cleanName);
        } catch (Exception e) {


            // Fallback : diviser en mots et chercher chaque mot
            String[] words = cleanName.split("\\s+");
            if (words.length == 1) {
                try {
                    return repository.findByNameContainingIgnoreCase(words[0]);
                } catch (Exception ex) {
                    return Collections.emptyList();
                }
            } else {
                // Pour les recherches multi-mots, utiliser une approche différente
                return searchByMultipleWords(words);
            }
        }
    }

    private List<ShadowPilot> searchByMultipleWords(String[] words) {
        // Implémentation pour rechercher par mots multiples
        // Vous pouvez combiner les résultats de plusieurs requêtes
        List<ShadowPilot> results = new ArrayList<>();
        for (String word : words) {
            if (word.length() >= 2) {
                try {
                    results.addAll(repository.findByNameContainingIgnoreCase(word));
                } catch (Exception e) {
                    // Ignorer les erreurs pour des mots individuels
                }
            }
        }
        return results.stream().distinct().collect(Collectors.toList());
    }
    /**
     * Recherche par domaine
     */
    @Transactional(readOnly = true)
    public Optional<ShadowPilot> findByDomain(String domain) {
        return repository.findByDomain(domain);
    }

    /**
     * Recherche par localisation
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> findByLocation(String location) {
        return repository.findByAddressContainingIgnoreCase(location);
    }

    /**
     * Recherche les entreprises de haute qualité
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> findHighQualityBusinesses() {
        return repository.findHighQualityBusinesses();
    }

    /**
     * Recherche par critères multiples avec pagination
     */
    @Transactional(readOnly = true)
    public Page<ShadowPilot> searchByCriteria(Double minTrustscore,
                                                         Double minReviews,
                                                         String category,
                                                         int page,
                                                         int size) {
        Pageable pageable = PageRequest.of(page, size, Sort.by("businessMetrics.trustscore").descending());
        return repository.findByMultipleCriteria(minTrustscore, minReviews, category, pageable);
    }

    /**
     * Recherche textuelle libre
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> fullTextSearch(String searchText) {
        return repository.findByFullTextSearch(searchText);
    }

    // ===== ANALYSES ET STATISTIQUES =====

    /**
     * Obtient les statistiques générales
     */
    @Transactional(readOnly = true)
    public Map<String, Object> getGeneralStats() {
        Map<String, Object> stats = new HashMap<>();

        long total = repository.count();
        long highQuality = repository.countByTrustscoreGreaterThanEqual(4.0);
        long withSocialMedia = repository.findWithSocialMedia().size();
        long claimed = repository.findClaimedBusinesses().size();
        long withRecentReviews = repository.findWithRecentReviews().size();

        stats.put("totalBusinesses", total);
        stats.put("highQualityBusinesses", highQuality);
        stats.put("withSocialMedia", withSocialMedia);
        stats.put("claimedBusinesses", claimed);
        stats.put("withRecentReviews", withRecentReviews);
        stats.put("highQualityPercentage", total > 0 ? (double) highQuality / total * 100 : 0);
        stats.put("socialMediaPercentage", total > 0 ? (double) withSocialMedia / total * 100 : 0);

        return stats;
    }

    /**
     * Obtient le top des entreprises par trustscore
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> getTopByTrustscore(int limit) {
        Pageable pageable = PageRequest.of(0, limit, Sort.by("businessMetrics.trustscore").descending());
        return repository.findTopByReviews(pageable).getContent();
    }

    /**
     * Obtient le top des entreprises par nombre d'avis
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> getTopByReviewCount(int limit) {
        Pageable pageable = PageRequest.of(0, limit, Sort.by("businessMetrics.number_of_reviews").descending());
        return repository.findTopByReviews(pageable).getContent();
    }

    /**
     * Recherche d'entreprises similaires
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> findSimilarBusinesses(String businessId) {
        Optional<ShadowPilot> business = repository.findById(businessId);
        if (business.isPresent()) {
            ShadowPilot b = business.get();
            String category = b.getBusinessMetric("categories", String.class);
            String location = extractLocationFromAddress(b.getAddress());

            if (category != null && location != null) {
                return repository.findSimilarBusinesses(category, location, businessId);
            }
        }
        return new ArrayList<>();
    }

    /**
     * Filtre par trustscore minimum
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> findByMinTrustscore(Double minTrustscore) {
        return repository.findByTrustscoreGreaterThanEqual(minTrustscore);
    }

    /**
     * Filtre par nombre minimum d'avis
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> findByMinReviewCount(Double minReviews) {
        return repository.findByNumberOfReviewsGreaterThanEqual(minReviews);
    }

    /**
     * Recherche par catégorie
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> findByCategory(String category) {
        return repository.findByCategory(category);
    }

    /**
     * Recherche les entreprises avec présence sur réseaux sociaux
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> findWithSocialMedia() {
        return repository.findWithSocialMedia();
    }

    /**
     * Recherche par plateforme de réseau social
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> findBySocialMediaPlatform(String platform) {
        return repository.findBySocialMediaPlatform(platform);
    }

    // ===== DONNÉES RÉCENTES =====








    // ===== MÉTHODES UTILITAIRES =====

    /**
     * Met à jour les métriques d'une entreprise
     */
    public ShadowPilot updateBusinessMetrics(String id, Map<String, Object> newMetrics) {
        Optional<ShadowPilot> optional = repository.findById(id);
        if (optional.isPresent()) {
            ShadowPilot business = optional.get();
            Map<String, Object> currentMetrics = business.getBusinessMetrics();
            if (currentMetrics == null) {
                currentMetrics = new HashMap<>();
            }
            currentMetrics.putAll(newMetrics);
            business.setBusinessMetrics(currentMetrics);
            return repository.save(business);
        }
        return null;
    }

    /**
     * Ajoute un avis à une entreprise existante
     */
    public ShadowPilot addReview(String businessId, Map<String, Object> review, String reviewType) {
        Optional<ShadowPilot> optional = repository.findById(businessId);
        if (optional.isPresent()) {
            ShadowPilot business = optional.get();

            switch (reviewType.toLowerCase()) {
                case "enhanced":
                    business.addEnhancedReview(review);
                    break;
                case "fivestar":
                    if (business.getFiveStarReviews().size() < 5) {
                        business.getFiveStarReviews().add(review);
                    }
                    break;
                case "onestar":
                    if (business.getOneStarReviews().size() < 5) {
                        business.getOneStarReviews().add(review);
                    }
                    break;
            }

            return repository.save(business);
        }
        return null;
    }

    /**
     * Extrait la localisation principale de l'adresse
     */
    private String extractLocationFromAddress(String address) {
        if (address == null || address.trim().isEmpty()) {
            return null;
        }

        // Logique simple d'extraction - peut être améliorée
        String[] parts = address.split(",");
        if (parts.length >= 2) {
            return parts[parts.length - 2].trim(); // Avant-dernière partie (ville généralement)
        }
        return address.trim();
    }

    /**
     * Valide la cohérence des données d'une entreprise
     */
    public boolean validateBusinessData(ShadowPilot business) {
        if (business.getName() == null || business.getName().trim().isEmpty()) {
            return false;
        }

        // Valide le trustscore s'il existe
        Double trustscore = business.getTrustscore();
        if (trustscore != null && (trustscore < 0 || trustscore > 5)) {
            return false;
        }

        // Valide le nombre d'avis s'il existe
        Double reviews = business.getNumberOfReviews();
        if (reviews != null && reviews < 0) {
            return false;
        }

        return true;
    }


    public Map<String, Object> searchShadowPilotByAttributesFuzzy(Map<String, String> attributes, Pageable pageable) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // ===================== CHAMPS TEXTE ESSENTIELS (avec analyseur) =====================

        // Nom de l'entreprise
        if (attributes.containsKey("name") && !attributes.get("name").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("name")
                    .query(attributes.get("name"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }
        if (attributes.containsKey("category") && !attributes.get("category").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("category")
                    .query(attributes.get("category"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }
        if (attributes.containsKey("subCategory") && !attributes.get("subCategory").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("subCategory")
                    .query(attributes.get("subCategory"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Adresse
        if (attributes.containsKey("address") && !attributes.get("address").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("address")
                    .query(attributes.get("address"))
                    .operator(Operator.Or) // Plus flexible pour adresse
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Téléphone
        if (attributes.containsKey("phone") && !attributes.get("phone").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("phone")
                    .query(attributes.get("phone"))
                    .operator(Operator.And)
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // ===================== CHAMPS KEYWORD (recherche exacte) =====================

        // Domaine
        if (attributes.containsKey("domain") && !attributes.get("domain").isEmpty()) {
            TermQuery termQuery = new TermQuery.Builder()
                    .field("domain")
                    .value(attributes.get("domain"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
        }

        // Email
        if (attributes.containsKey("email") && !attributes.get("email").isEmpty()) {
            TermQuery termQuery = new TermQuery.Builder()
                    .field("email")
                    .value(attributes.get("email"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
        }

        // Website
        if (attributes.containsKey("website") && !attributes.get("website").isEmpty()) {
            TermQuery termQuery = new TermQuery.Builder()
                    .field("website")
                    .value(attributes.get("website"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
        }

        // ===================== RECHERCHE DANS BUSINESS METRICS =====================

        // Trust Score (avec intervalle)
        if ((attributes.containsKey("trustScoreMin") || attributes.containsKey("trustScoreMax")) &&
                (attributes.get("trustScoreMin") != null && !attributes.get("trustScoreMin").isEmpty() ||
                        attributes.get("trustScoreMax") != null && !attributes.get("trustScoreMax").isEmpty())) {

            try {
                RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field("businessMetrics.trustscore");

                if (attributes.containsKey("trustScoreMin") && !attributes.get("trustScoreMin").isEmpty()) {
                    double minTrustScore = Double.parseDouble(attributes.get("trustScoreMin"));
                    rangeBuilder.gte(JsonData.of(minTrustScore));
                }

                if (attributes.containsKey("trustScoreMax") && !attributes.get("trustScoreMax").isEmpty()) {
                    double maxTrustScore = Double.parseDouble(attributes.get("trustScoreMax"));
                    rangeBuilder.lte(JsonData.of(maxTrustScore));
                }

                boolQueryBuilder.must(new Query.Builder().range(rangeBuilder.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de trustScore invalide dans l'intervalle: " + e.getMessage());
            }
        }
        // Trust Score unique (pour compatibilité arrière)
        else if (attributes.containsKey("trustScore") && !attributes.get("trustScore").isEmpty()) {
            try {
                double targetTrustScore = Double.parseDouble(attributes.get("trustScore"));
                double tolerance = 0.2; // Tolérance de ±0.2

                RangeQuery rangeQuery = new RangeQuery.Builder()
                        .field("businessMetrics.trustscore")
                        .gte(JsonData.of(targetTrustScore - tolerance))
                        .lte(JsonData.of(targetTrustScore + tolerance))
                        .build();

                boolQueryBuilder.must(new Query.Builder().range(rangeQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de trustScore invalide: " + attributes.get("trustScore"));
            }
        }

        // Nombre d'avis (avec intervalle)
        if ((attributes.containsKey("numberOfReviewsMin") || attributes.containsKey("numberOfReviewsMax")) &&
                (attributes.get("numberOfReviewsMin") != null && !attributes.get("numberOfReviewsMin").isEmpty() ||
                        attributes.get("numberOfReviewsMax") != null && !attributes.get("numberOfReviewsMax").isEmpty())) {

            try {
                RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field("businessMetrics.number_of_reviews");

                if (attributes.containsKey("numberOfReviewsMin") && !attributes.get("numberOfReviewsMin").isEmpty()) {
                    int minReviews = Integer.parseInt(attributes.get("numberOfReviewsMin"));
                    rangeBuilder.gte(JsonData.of(minReviews));
                }

                if (attributes.containsKey("numberOfReviewsMax") && !attributes.get("numberOfReviewsMax").isEmpty()) {
                    int maxReviews = Integer.parseInt(attributes.get("numberOfReviewsMax"));
                    rangeBuilder.lte(JsonData.of(maxReviews));
                }

                boolQueryBuilder.must(new Query.Builder().range(rangeBuilder.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de numberOfReviews invalide dans l'intervalle: " + e.getMessage());
            }
        }

        // ===================== RECHERCHE DANS SOCIAL MEDIA =====================

        // Recherche générale dans les réseaux sociaux
        if (attributes.containsKey("socialMedia") && !attributes.get("socialMedia").isEmpty()) {
            String socialQuery = attributes.get("socialMedia");

            BoolQuery.Builder socialBoolQuery = new BoolQuery.Builder();

            // Recherche dans les URLs des réseaux sociaux
            MatchQuery socialUrlQuery = new MatchQuery.Builder()
                    .field("socialMedia.*")
                    .query(socialQuery)
                    .fuzziness("1")
                    .build();

            socialBoolQuery.should(new Query.Builder().match(socialUrlQuery).build());
            boolQueryBuilder.must(new Query.Builder().bool(socialBoolQuery.build()).build());
        }

        // Recherche spécifique par plateforme sociale
        if (attributes.containsKey("facebookUrl") && !attributes.get("facebookUrl").isEmpty()) {
            MatchQuery facebookQuery = new MatchQuery.Builder()
                    .field("socialMedia.facebook")
                    .query(attributes.get("facebookUrl"))
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(facebookQuery).build());
        }

        if (attributes.containsKey("twitterUrl") && !attributes.get("twitterUrl").isEmpty()) {
            MatchQuery twitterQuery = new MatchQuery.Builder()
                    .field("socialMedia.twitter")
                    .query(attributes.get("twitterUrl"))
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(twitterQuery).build());
        }

        if (attributes.containsKey("linkedinUrl") && !attributes.get("linkedinUrl").isEmpty()) {
            MatchQuery linkedinQuery = new MatchQuery.Builder()
                    .field("socialMedia.linkedin")
                    .query(attributes.get("linkedinUrl"))
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(linkedinQuery).build());
        }

        // ===================== RECHERCHE DANS LES AVIS =====================

        // Recherche dans les avis détaillés
        if (attributes.containsKey("reviewContent") && !attributes.get("reviewContent").isEmpty()) {
            BoolQuery.Builder reviewBoolQuery = new BoolQuery.Builder();

            // Recherche dans enhancedReviews
            MatchQuery enhancedReviewQuery = new MatchQuery.Builder()
                    .field("enhancedReviews.content")
                    .query(attributes.get("reviewContent"))
                    .fuzziness("1")
                    .build();

            // Recherche dans fiveStarReviews
            MatchQuery fiveStarQuery = new MatchQuery.Builder()
                    .field("fiveStarReviews.content")
                    .query(attributes.get("reviewContent"))
                    .fuzziness("1")
                    .build();

            // Recherche dans oneStarReviews
            MatchQuery oneStarQuery = new MatchQuery.Builder()
                    .field("oneStarReviews.content")
                    .query(attributes.get("reviewContent"))
                    .fuzziness("1")
                    .build();

            reviewBoolQuery.should(new Query.Builder().match(enhancedReviewQuery).build());
            reviewBoolQuery.should(new Query.Builder().match(fiveStarQuery).build());
            reviewBoolQuery.should(new Query.Builder().match(oneStarQuery).build());

            boolQueryBuilder.must(new Query.Builder().bool(reviewBoolQuery.build()).build());
        }

        // Recherche par rating d'avis
        if (attributes.containsKey("reviewRating") && !attributes.get("reviewRating").isEmpty()) {
            try {
                int rating = Integer.parseInt(attributes.get("reviewRating"));

                BoolQuery.Builder ratingBoolQuery = new BoolQuery.Builder();

                TermQuery enhancedRatingQuery = new TermQuery.Builder()
                        .field("enhancedReviews.rating")
                        .value(rating)
                        .build();

                TermQuery fiveStarRatingQuery = new TermQuery.Builder()
                        .field("fiveStarReviews.rating")
                        .value(rating)
                        .build();

                TermQuery oneStarRatingQuery = new TermQuery.Builder()
                        .field("oneStarReviews.rating")
                        .value(rating)
                        .build();

                ratingBoolQuery.should(new Query.Builder().term(enhancedRatingQuery).build());
                ratingBoolQuery.should(new Query.Builder().term(fiveStarRatingQuery).build());
                ratingBoolQuery.should(new Query.Builder().term(oneStarRatingQuery).build());

                boolQueryBuilder.must(new Query.Builder().bool(ratingBoolQuery.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de reviewRating invalide: " + attributes.get("reviewRating"));
            }
        }

        // ===================== RECHERCHE DANS SENTIMENT DISTRIBUTION =====================

        // Recherche par sentiment prédominant
        if (attributes.containsKey("dominantSentiment") && !attributes.get("dominantSentiment").isEmpty()) {
            MatchQuery sentimentQuery = new MatchQuery.Builder()
                    .field("sentimentDistribution.*")
                    .query(attributes.get("dominantSentiment"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(sentimentQuery).build());
        }

        // ===================== RECHERCHE DANS STAR RATINGS =====================

        // Recherche par pourcentage d'étoiles spécifique
        if (attributes.containsKey("starPercentage") && attributes.containsKey("starLevel") &&
                !attributes.get("starPercentage").isEmpty() && !attributes.get("starLevel").isEmpty()) {
            try {
                double percentage = Double.parseDouble(attributes.get("starPercentage"));
                String starLevel = attributes.get("starLevel"); // ex: "5_stars", "4_stars", etc.

                double tolerance = 5.0; // Tolérance de ±5%

                RangeQuery rangeQuery = new RangeQuery.Builder()
                        .field("starRatings." + starLevel)
                        .gte(JsonData.of(percentage - tolerance))
                        .lte(JsonData.of(percentage + tolerance))
                        .build();

                boolQueryBuilder.must(new Query.Builder().range(rangeQuery).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de starPercentage invalide: " + attributes.get("starPercentage"));
            }
        }

        // ===================== RECHERCHE DANS SIMILAR COMPANIES =====================

        // Recherche par entreprise similaire
        if (attributes.containsKey("similarCompanyName") && !attributes.get("similarCompanyName").isEmpty()) {
            MatchQuery similarQuery = new MatchQuery.Builder()
                    .field("similarCompanies.name")
                    .query(attributes.get("similarCompanyName"))
                    .fuzziness("1")
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(similarQuery).build());
        }

        // Traitement des autres attributs non spécifiés
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Exclure tous les champs déjà traités
            if (!key.equals("name") && !key.equals("address") && !key.equals("phone")
                    && !key.equals("domain") && !key.equals("email") && !key.equals("website")
                    && !key.equals("trustScore") && !key.equals("trustScoreMin") && !key.equals("trustScoreMax")
                    && !key.equals("numberOfReviewsMin") && !key.equals("numberOfReviewsMax")
                    && !key.equals("socialMedia") && !key.equals("facebookUrl") && !key.equals("twitterUrl") && !key.equals("linkedinUrl")
                    && !key.equals("reviewContent") && !key.equals("reviewRating")
                    && !key.equals("dominantSentiment") && !key.equals("starPercentage") && !key.equals("starLevel")
                    && !key.equals("similarCompanyName")
                    && value != null && !value.isEmpty()) {

                MatchQuery matchQuery = new MatchQuery.Builder()
                        .field(key)
                        .query(value)
                        .fuzziness("1")
                        .build();

                boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
            }
        }

        // Création de la requête finale
        BoolQuery builtBoolQuery = boolQueryBuilder.build();

        try {
            // Création de la requête avec pagination fournie par le client
            NativeQuery searchQuery = NativeQuery.builder()
                    .withQuery(q -> q.bool(builtBoolQuery))
                    .withPageable(pageable)
                    .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                    .withTrackTotalHits(true)
                    .build();

            // Exécuter la recherche
            SearchHits<ShadowPilot> searchHits = elasticsearchOperations.search(searchQuery, ShadowPilot.class);

            // Récupérer le nombre total de résultats
            long totalHits = searchHits.getTotalHits();

            // Récupérer les résultats
            List<ShadowPilot> resultsOnPage = searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

            // Log pour débogage
            System.out.println("ShadowPilot Fuzzy - Page " + pageable.getPageNumber() + ": " + resultsOnPage.size() + " résultats");
            System.out.println("Total résultats trouvés: " + totalHits);

            // Créer un Map contenant la page de résultats et le nombre total
            Map<String, Object> result = new HashMap<>();
            result.put("page", new PageImpl<>(resultsOnPage, pageable, totalHits));
            result.put("totalResults", totalHits);

            return result;
        } catch (Exception e) {
            // Log détaillé de l'erreur
            System.err.println("Erreur lors de l'exécution de la requête Elasticsearch ShadowPilot Fuzzy: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }


    public Map<String, Object> searchShadowPilotByAttributesExact(Map<String, String> attributes, Pageable pageable) {
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

        // ===================== CHAMPS TEXTE ESSENTIELS (recherche exacte) =====================

        // Nom de l'entreprise
        if (attributes.containsKey("name") && !attributes.get("name").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("name")
                    .query(attributes.get("name"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }
        if (attributes.containsKey("category") && !attributes.get("category").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("category")
                    .query(attributes.get("category"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }
        if (attributes.containsKey("subCategory") && !attributes.get("subCategory").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("subCategory")
                    .query(attributes.get("subCategory"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Adresse
        if (attributes.containsKey("address") && !attributes.get("address").isEmpty()) {
            MatchQuery matchQuery = new MatchQuery.Builder()
                    .field("address")
                    .query(attributes.get("address"))
                    .operator(Operator.And) // Tous les mots doivent être présents
                    .build();
            boolQueryBuilder.must(new Query.Builder().match(matchQuery).build());
        }

        // Téléphone
        if (attributes.containsKey("phone") && !attributes.get("phone").isEmpty()) {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                    .field("phone")
                    .query(attributes.get("phone"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
        }

        // ===================== CHAMPS KEYWORD (recherche exacte) =====================

        // Domaine
        if (attributes.containsKey("domain") && !attributes.get("domain").isEmpty()) {
            TermQuery termQuery = new TermQuery.Builder()
                    .field("domain")
                    .value(attributes.get("domain"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
        }

        // Email
        if (attributes.containsKey("email") && !attributes.get("email").isEmpty()) {
            TermQuery termQuery = new TermQuery.Builder()
                    .field("email")
                    .value(attributes.get("email"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
        }

        // Website
        if (attributes.containsKey("website") && !attributes.get("website").isEmpty()) {
            TermQuery termQuery = new TermQuery.Builder()
                    .field("website")
                    .value(attributes.get("website"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
        }

        // ===================== RECHERCHE DANS BUSINESS METRICS =====================

        // Trust Score (avec intervalle)
        if ((attributes.containsKey("trustScoreMin") || attributes.containsKey("trustScoreMax")) &&
                (attributes.get("trustScoreMin") != null && !attributes.get("trustScoreMin").isEmpty() ||
                        attributes.get("trustScoreMax") != null && !attributes.get("trustScoreMax").isEmpty())) {

            try {
                RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field("businessMetrics.trustscore");

                if (attributes.containsKey("trustScoreMin") && !attributes.get("trustScoreMin").isEmpty()) {
                    double minTrustScore = Double.parseDouble(attributes.get("trustScoreMin"));
                    rangeBuilder.gte(JsonData.of(minTrustScore));
                }

                if (attributes.containsKey("trustScoreMax") && !attributes.get("trustScoreMax").isEmpty()) {
                    double maxTrustScore = Double.parseDouble(attributes.get("trustScoreMax"));
                    rangeBuilder.lte(JsonData.of(maxTrustScore));
                }

                boolQueryBuilder.must(new Query.Builder().range(rangeBuilder.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de trustScore invalide dans l'intervalle: " + e.getMessage());
            }
        }
        // Trust Score unique (recherche exacte)
        else if (attributes.containsKey("trustScore") && !attributes.get("trustScore").isEmpty()) {
            try {
                double targetTrustScore = Double.parseDouble(attributes.get("trustScore"));

                TermQuery termQuery = new TermQuery.Builder()
                        .field("businessMetrics.trustscore")
                        .value(targetTrustScore)
                        .build();
                boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de trustScore invalide: " + attributes.get("trustScore"));
            }
        }

        // Nombre d'avis (avec intervalle)
        if ((attributes.containsKey("numberOfReviewsMin") || attributes.containsKey("numberOfReviewsMax")) &&
                (attributes.get("numberOfReviewsMin") != null && !attributes.get("numberOfReviewsMin").isEmpty() ||
                        attributes.get("numberOfReviewsMax") != null && !attributes.get("numberOfReviewsMax").isEmpty())) {

            try {
                RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field("businessMetrics.number_of_reviews");

                if (attributes.containsKey("numberOfReviewsMin") && !attributes.get("numberOfReviewsMin").isEmpty()) {
                    int minReviews = Integer.parseInt(attributes.get("numberOfReviewsMin"));
                    rangeBuilder.gte(JsonData.of(minReviews));
                }

                if (attributes.containsKey("numberOfReviewsMax") && !attributes.get("numberOfReviewsMax").isEmpty()) {
                    int maxReviews = Integer.parseInt(attributes.get("numberOfReviewsMax"));
                    rangeBuilder.lte(JsonData.of(maxReviews));
                }

                boolQueryBuilder.must(new Query.Builder().range(rangeBuilder.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de numberOfReviews invalide dans l'intervalle: " + e.getMessage());
            }
        }

        // Nombre d'avis unique (recherche exacte)
        else if (attributes.containsKey("numberOfReviews") && !attributes.get("numberOfReviews").isEmpty()) {
            try {
                int targetReviews = Integer.parseInt(attributes.get("numberOfReviews"));

                TermQuery termQuery = new TermQuery.Builder()
                        .field("businessMetrics.number_of_reviews")
                        .value(targetReviews)
                        .build();
                boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de numberOfReviews invalide: " + attributes.get("numberOfReviews"));
            }
        }

        // ===================== RECHERCHE DANS SOCIAL MEDIA =====================

        // Recherche générale dans les réseaux sociaux (exacte)
        if (attributes.containsKey("socialMedia") && !attributes.get("socialMedia").isEmpty()) {
            MatchPhraseQuery socialQuery = new MatchPhraseQuery.Builder()
                    .field("socialMedia.*")
                    .query(attributes.get("socialMedia"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(socialQuery).build());
        }

        // Recherche spécifique par plateforme sociale (exacte)
        if (attributes.containsKey("facebookUrl") && !attributes.get("facebookUrl").isEmpty()) {
            TermQuery facebookQuery = new TermQuery.Builder()
                    .field("socialMedia.facebook")
                    .value(attributes.get("facebookUrl"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(facebookQuery).build());
        }

        if (attributes.containsKey("twitterUrl") && !attributes.get("twitterUrl").isEmpty()) {
            TermQuery twitterQuery = new TermQuery.Builder()
                    .field("socialMedia.twitter")
                    .value(attributes.get("twitterUrl"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(twitterQuery).build());
        }

        if (attributes.containsKey("linkedinUrl") && !attributes.get("linkedinUrl").isEmpty()) {
            TermQuery linkedinQuery = new TermQuery.Builder()
                    .field("socialMedia.linkedin")
                    .value(attributes.get("linkedinUrl"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(linkedinQuery).build());
        }

        if (attributes.containsKey("instagramUrl") && !attributes.get("instagramUrl").isEmpty()) {
            TermQuery instagramQuery = new TermQuery.Builder()
                    .field("socialMedia.instagram")
                    .value(attributes.get("instagramUrl"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(instagramQuery).build());
        }

        if (attributes.containsKey("youtubeUrl") && !attributes.get("youtubeUrl").isEmpty()) {
            TermQuery youtubeQuery = new TermQuery.Builder()
                    .field("socialMedia.youtube")
                    .value(attributes.get("youtubeUrl"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(youtubeQuery).build());
        }

        // ===================== RECHERCHE DANS LES AVIS =====================

        // Recherche dans les avis détaillés (exacte)
        if (attributes.containsKey("reviewContent") && !attributes.get("reviewContent").isEmpty()) {
            BoolQuery.Builder reviewBoolQuery = new BoolQuery.Builder();

            // Recherche exacte dans enhancedReviews
            MatchPhraseQuery enhancedReviewQuery = new MatchPhraseQuery.Builder()
                    .field("enhancedReviews.content")
                    .query(attributes.get("reviewContent"))
                    .build();

            // Recherche exacte dans fiveStarReviews
            MatchPhraseQuery fiveStarQuery = new MatchPhraseQuery.Builder()
                    .field("fiveStarReviews.content")
                    .query(attributes.get("reviewContent"))
                    .build();

            // Recherche exacte dans oneStarReviews
            MatchPhraseQuery oneStarQuery = new MatchPhraseQuery.Builder()
                    .field("oneStarReviews.content")
                    .query(attributes.get("reviewContent"))
                    .build();

            reviewBoolQuery.should(new Query.Builder().matchPhrase(enhancedReviewQuery).build());
            reviewBoolQuery.should(new Query.Builder().matchPhrase(fiveStarQuery).build());
            reviewBoolQuery.should(new Query.Builder().matchPhrase(oneStarQuery).build());

            boolQueryBuilder.must(new Query.Builder().bool(reviewBoolQuery.build()).build());
        }

        // Recherche par rating d'avis (exacte)
        if (attributes.containsKey("reviewRating") && !attributes.get("reviewRating").isEmpty()) {
            try {
                int rating = Integer.parseInt(attributes.get("reviewRating"));

                BoolQuery.Builder ratingBoolQuery = new BoolQuery.Builder();

                TermQuery enhancedRatingQuery = new TermQuery.Builder()
                        .field("enhancedReviews.rating")
                        .value(rating)
                        .build();

                TermQuery fiveStarRatingQuery = new TermQuery.Builder()
                        .field("fiveStarReviews.rating")
                        .value(rating)
                        .build();

                TermQuery oneStarRatingQuery = new TermQuery.Builder()
                        .field("oneStarReviews.rating")
                        .value(rating)
                        .build();

                ratingBoolQuery.should(new Query.Builder().term(enhancedRatingQuery).build());
                ratingBoolQuery.should(new Query.Builder().term(fiveStarRatingQuery).build());
                ratingBoolQuery.should(new Query.Builder().term(oneStarRatingQuery).build());

                boolQueryBuilder.must(new Query.Builder().bool(ratingBoolQuery.build()).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de reviewRating invalide: " + attributes.get("reviewRating"));
            }
        }

        // Recherche par auteur d'avis
        if (attributes.containsKey("reviewAuthor") && !attributes.get("reviewAuthor").isEmpty()) {
            BoolQuery.Builder authorBoolQuery = new BoolQuery.Builder();

            MatchPhraseQuery enhancedAuthorQuery = new MatchPhraseQuery.Builder()
                    .field("enhancedReviews.author")
                    .query(attributes.get("reviewAuthor"))
                    .build();

            MatchPhraseQuery fiveStarAuthorQuery = new MatchPhraseQuery.Builder()
                    .field("fiveStarReviews.author")
                    .query(attributes.get("reviewAuthor"))
                    .build();

            MatchPhraseQuery oneStarAuthorQuery = new MatchPhraseQuery.Builder()
                    .field("oneStarReviews.author")
                    .query(attributes.get("reviewAuthor"))
                    .build();

            authorBoolQuery.should(new Query.Builder().matchPhrase(enhancedAuthorQuery).build());
            authorBoolQuery.should(new Query.Builder().matchPhrase(fiveStarAuthorQuery).build());
            authorBoolQuery.should(new Query.Builder().matchPhrase(oneStarAuthorQuery).build());

            boolQueryBuilder.must(new Query.Builder().bool(authorBoolQuery.build()).build());
        }

        // ===================== RECHERCHE DANS SENTIMENT DISTRIBUTION =====================

        // Recherche par sentiment prédominant (exacte)
        if (attributes.containsKey("dominantSentiment") && !attributes.get("dominantSentiment").isEmpty()) {
            ExistsQuery existsQuery = new ExistsQuery.Builder()
                    .field("sentimentDistribution." + attributes.get("dominantSentiment"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().exists(existsQuery).build());
        }

        // Recherche par pourcentage de sentiment spécifique
        if (attributes.containsKey("sentimentPercentage") && attributes.containsKey("sentimentType") &&
                !attributes.get("sentimentPercentage").isEmpty() && !attributes.get("sentimentType").isEmpty()) {
            try {
                double percentage = Double.parseDouble(attributes.get("sentimentPercentage"));
                String sentimentType = attributes.get("sentimentType"); // ex: "positive", "negative", "neutral"

                TermQuery termQuery = new TermQuery.Builder()
                        .field("sentimentDistribution." + sentimentType)
                        .value(percentage)
                        .build();
                boolQueryBuilder.must(new Query.Builder().term(termQuery).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de sentimentPercentage invalide: " + attributes.get("sentimentPercentage"));
            }
        }

        // ===================== RECHERCHE DANS STAR RATINGS =====================

        // Recherche par pourcentage d'étoiles spécifique (exacte)
        if (attributes.containsKey("starPercentage") && attributes.containsKey("starLevel") &&
                !attributes.get("starPercentage").isEmpty() && !attributes.get("starLevel").isEmpty()) {
            try {
                double percentage = Double.parseDouble(attributes.get("starPercentage"));
                String starLevel = attributes.get("starLevel"); // ex: "5_stars", "4_stars", etc.

                TermQuery termQuery = new TermQuery.Builder()
                        .field("starRatings." + starLevel)
                        .value(percentage)
                        .build();
                boolQueryBuilder.must(new Query.Builder().term(termQuery).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de starPercentage invalide: " + attributes.get("starPercentage"));
            }
        }

        // Recherche par intervalle de pourcentage d'étoiles
        if (attributes.containsKey("starPercentageMin") && attributes.containsKey("starPercentageMax") &&
                attributes.containsKey("starLevel") && !attributes.get("starLevel").isEmpty() &&
                !attributes.get("starPercentageMin").isEmpty() && !attributes.get("starPercentageMax").isEmpty()) {
            try {
                double minPercentage = Double.parseDouble(attributes.get("starPercentageMin"));
                double maxPercentage = Double.parseDouble(attributes.get("starPercentageMax"));
                String starLevel = attributes.get("starLevel");

                RangeQuery rangeQuery = new RangeQuery.Builder()
                        .field("starRatings." + starLevel)
                        .gte(JsonData.of(minPercentage))
                        .lte(JsonData.of(maxPercentage))
                        .build();
                boolQueryBuilder.must(new Query.Builder().range(rangeQuery).build());

            } catch (NumberFormatException e) {
                System.err.println("Format de starPercentage invalide dans l'intervalle: " + e.getMessage());
            }
        }

        // ===================== RECHERCHE DANS SIMILAR COMPANIES =====================

        // Recherche par entreprise similaire (exacte)
        if (attributes.containsKey("similarCompanyName") && !attributes.get("similarCompanyName").isEmpty()) {
            MatchPhraseQuery similarQuery = new MatchPhraseQuery.Builder()
                    .field("similarCompanies.name")
                    .query(attributes.get("similarCompanyName"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().matchPhrase(similarQuery).build());
        }

        // Recherche par domaine d'entreprise similaire
        if (attributes.containsKey("similarCompanyDomain") && !attributes.get("similarCompanyDomain").isEmpty()) {
            TermQuery similarDomainQuery = new TermQuery.Builder()
                    .field("similarCompanies.domain")
                    .value(attributes.get("similarCompanyDomain"))
                    .build();
            boolQueryBuilder.must(new Query.Builder().term(similarDomainQuery).build());
        }

        // Recherche par score de similarité
        if (attributes.containsKey("similarityScore") && !attributes.get("similarityScore").isEmpty()) {
            try {
                double similarityScore = Double.parseDouble(attributes.get("similarityScore"));

                TermQuery termQuery = new TermQuery.Builder()
                        .field("similarCompanies.similarity_score")
                        .value(similarityScore)
                        .build();
                boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
            } catch (NumberFormatException e) {
                System.err.println("Format de similarityScore invalide: " + attributes.get("similarityScore"));
            }
        }

        // ===================== RECHERCHE PAR MÉTRIQUE SPÉCIFIQUE =====================

        // Recherche par métrique business spécifique
        if (attributes.containsKey("metricName") && attributes.containsKey("metricValue") &&
                !attributes.get("metricName").isEmpty() && !attributes.get("metricValue").isEmpty()) {
            String metricName = attributes.get("metricName");
            String metricValue = attributes.get("metricValue");

            try {
                // Essayer d'abord comme nombre
                double numericValue = Double.parseDouble(metricValue);
                TermQuery termQuery = new TermQuery.Builder()
                        .field("businessMetrics." + metricName)
                        .value(numericValue)
                        .build();
                boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
            } catch (NumberFormatException e) {
                // Si ce n'est pas un nombre, traiter comme texte
                TermQuery termQuery = new TermQuery.Builder()
                        .field("businessMetrics." + metricName)
                        .value(metricValue)
                        .build();
                boolQueryBuilder.must(new Query.Builder().term(termQuery).build());
            }
        }

        // Traitement des autres attributs non spécifiés (exacte)
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Exclure tous les champs déjà traités
            if (!key.equals("name") && !key.equals("address") && !key.equals("phone")
                    && !key.equals("domain") && !key.equals("email") && !key.equals("website")
                    && !key.equals("trustScore") && !key.equals("trustScoreMin") && !key.equals("trustScoreMax")
                    && !key.equals("numberOfReviews") && !key.equals("numberOfReviewsMin") && !key.equals("numberOfReviewsMax")
                    && !key.equals("socialMedia") && !key.equals("facebookUrl") && !key.equals("twitterUrl")
                    && !key.equals("linkedinUrl") && !key.equals("instagramUrl") && !key.equals("youtubeUrl")
                    && !key.equals("reviewContent") && !key.equals("reviewRating") && !key.equals("reviewAuthor")
                    && !key.equals("dominantSentiment") && !key.equals("sentimentPercentage") && !key.equals("sentimentType")
                    && !key.equals("starPercentage") && !key.equals("starLevel")
                    && !key.equals("starPercentageMin") && !key.equals("starPercentageMax")
                    && !key.equals("similarCompanyName") && !key.equals("similarCompanyDomain") && !key.equals("similarityScore")
                    && !key.equals("metricName") && !key.equals("metricValue")
                    && value != null && !value.isEmpty()) {

                // Utiliser match_phrase pour une recherche plus exacte
                MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery.Builder()
                        .field(key)
                        .query(value)
                        .build();
                boolQueryBuilder.must(new Query.Builder().matchPhrase(matchPhraseQuery).build());
            }
        }

        // Création de la requête finale
        BoolQuery builtBoolQuery = boolQueryBuilder.build();

        try {
            // Création de la requête avec pagination fournie par le client
            NativeQuery searchQuery = NativeQuery.builder()
                    .withQuery(q -> q.bool(builtBoolQuery))
                    .withPageable(pageable)
                    .withSort(Sort.by(Sort.Direction.DESC, "_score"))
                    .withTrackTotalHits(true)
                    .build();

            // Pour débogage - Affiche la requête générée
            // System.out.println("DEBUG - Requête ShadowPilot exacte générée: " + searchQuery.toString());

            // Exécuter la recherche
            SearchHits<ShadowPilot> searchHits = elasticsearchOperations.search(searchQuery, ShadowPilot.class);

            // Récupérer le nombre total de résultats
            long totalHits = searchHits.getTotalHits();

            // Récupérer les résultats
            List<ShadowPilot> resultsOnPage = searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

            // Log pour débogage
            System.out.println("ShadowPilot Exact - Page " + pageable.getPageNumber() + ": " + resultsOnPage.size() + " résultats");
            System.out.println("Total résultats trouvés: " + totalHits);

            // Créer un Map contenant la page de résultats et le nombre total
            Map<String, Object> result = new HashMap<>();
            result.put("page", new PageImpl<>(resultsOnPage, pageable, totalHits));
            result.put("totalResults", totalHits);

            return result;
        } catch (Exception e) {
            // Log détaillé de l'erreur
            System.err.println("Erreur lors de l'exécution de la requête Elasticsearch ShadowPilot Exact: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }





    // ===== SUPPRESSION PAR SOUS-CATÉGORIE =====

    /**
     * Supprime toutes les entreprises d'une sous-catégorie donnée
     * @param subCategory La sous-catégorie à supprimer
     * @return Le nombre d'entreprises supprimées
     */
    @Transactional
    public long deleteBySubCategory(String subCategory) {
        if (subCategory == null || subCategory.trim().isEmpty()) {
            throw new IllegalArgumentException("La sous-catégorie ne peut pas être vide");
        }

        try {
            // Compter d'abord pour retourner le nombre
            long count = repository.countBySubCategory(subCategory);

            if (count > 0) {
                // Effectuer la suppression
                repository.deleteBySubCategory(subCategory);
                System.out.println("Suppression de " + count + " entreprises de la sous-catégorie: " + subCategory);
            } else {
                System.out.println("Aucune entreprise trouvée pour la sous-catégorie: " + subCategory);
            }

            return count;
        } catch (Exception e) {
            System.err.println("Erreur lors de la suppression par sous-catégorie '" + subCategory + "': " + e.getMessage());
            throw new RuntimeException("Erreur lors de la suppression par sous-catégorie", e);
        }
    }

    /**
     * Supprime toutes les entreprises d'une sous-catégorie donnée (insensible à la casse)
     * @param subCategory La sous-catégorie à supprimer
     * @return Le nombre d'entreprises supprimées
     */
    @Transactional
    public long deleteBySubCategoryIgnoreCase(String subCategory) {
        if (subCategory == null || subCategory.trim().isEmpty()) {
            throw new IllegalArgumentException("La sous-catégorie ne peut pas être vide");
        }

        try {
            // Compter d'abord pour retourner le nombre
            long count = repository.countBySubCategory(subCategory);

            if (count > 0) {
                // Effectuer la suppression
                repository.deleteBySubCategoryIgnoreCase(subCategory);
                System.out.println("Suppression de " + count + " entreprises de la sous-catégorie (insensible à la casse): " + subCategory);
            } else {
                System.out.println("Aucune entreprise trouvée pour la sous-catégorie: " + subCategory);
            }

            return count;
        } catch (Exception e) {
            System.err.println("Erreur lors de la suppression par sous-catégorie '" + subCategory + "': " + e.getMessage());
            throw new RuntimeException("Erreur lors de la suppression par sous-catégorie", e);
        }
    }

    /**
     * Supprime les entreprises par sous-catégorie avec confirmation et pagination
     * @param subCategory La sous-catégorie à supprimer
     * @param batchSize Taille du lot pour la suppression (optionnel, défaut: 1000)
     * @return Le nombre d'entreprises supprimées
     */
    @Transactional
    public long deleteBySubCategoryWithBatch(String subCategory, Integer batchSize) {
        if (subCategory == null || subCategory.trim().isEmpty()) {
            throw new IllegalArgumentException("La sous-catégorie ne peut pas être vide");
        }

        if (batchSize == null || batchSize <= 0) {
            batchSize = 1000; // Valeur par défaut
        }

        try {
            // Compter le total
            long totalCount = repository.countBySubCategory(subCategory);

            if (totalCount == 0) {
                System.out.println("Aucune entreprise trouvée pour la sous-catégorie: " + subCategory);
                return 0;
            }

            System.out.println("Début de la suppression de " + totalCount + " entreprises de la sous-catégorie: " + subCategory);

            long deletedCount = 0;
            int page = 0;
            boolean hasMore = true;

            while (hasMore) {
                // Récupérer un lot d'entreprises
                Pageable pageable = PageRequest.of(page, batchSize);
                List<ShadowPilot> batch = repository.findBySubCategory(subCategory);

                if (batch.isEmpty()) {
                    hasMore = false;
                } else {
                    // Supprimer le lot
                    List<String> idsToDelete = batch.stream()
                            .map(ShadowPilot::getId) // Assumant que getId() existe
                            .collect(Collectors.toList());

                    repository.deleteAllById(idsToDelete);

                    deletedCount += batch.size();
                    page++;

                    System.out.println("Lot " + page + " supprimé - " + batch.size() + " entreprises (" + deletedCount + "/" + totalCount + ")");

                    // Vérifier s'il reste des éléments
                    if (batch.size() < batchSize) {
                        hasMore = false;
                    }
                }
            }

            System.out.println("Suppression terminée - " + deletedCount + " entreprises supprimées");
            return deletedCount;

        } catch (Exception e) {
            System.err.println("Erreur lors de la suppression par lots de la sous-catégorie '" + subCategory + "': " + e.getMessage());
            throw new RuntimeException("Erreur lors de la suppression par lots", e);
        }
    }

    /**
     * Prévisualise les entreprises qui seraient supprimées (pour vérification)
     * @param subCategory La sous-catégorie à vérifier
     * @param limit Nombre maximum d'exemples à retourner
     * @return Liste des entreprises qui seraient supprimées
     */
    @Transactional(readOnly = true)
    public List<ShadowPilot> previewDeleteBySubCategory(String subCategory, int limit) {
        if (subCategory == null || subCategory.trim().isEmpty()) {
            throw new IllegalArgumentException("La sous-catégorie ne peut pas être vide");
        }

        try {
            Pageable pageable = PageRequest.of(0, limit);
            List<ShadowPilot> preview = repository.findBySubCategory(subCategory);

            // Limiter les résultats si nécessaire
            if (preview.size() > limit) {
                preview = preview.subList(0, limit);
            }

            long totalCount = repository.countBySubCategory(subCategory);
            System.out.println("Prévisualisation: " + totalCount + " entreprises seraient supprimées de la sous-catégorie: " + subCategory);
            System.out.println("Affichage des " + Math.min(limit, preview.size()) + " premiers exemples");

            return preview;

        } catch (Exception e) {
            System.err.println("Erreur lors de la prévisualisation pour la sous-catégorie '" + subCategory + "': " + e.getMessage());
            throw new RuntimeException("Erreur lors de la prévisualisation", e);
        }
    }

    /**
     * Compte le nombre d'entreprises dans une sous-catégorie
     * @param subCategory La sous-catégorie à compter
     * @return Le nombre d'entreprises
     */
    @Transactional(readOnly = true)
    public long countBySubCategory(String subCategory) {
        if (subCategory == null || subCategory.trim().isEmpty()) {
            return 0;
        }

        try {
            return repository.countBySubCategory(subCategory);
        } catch (Exception e) {
            System.err.println("Erreur lors du comptage pour la sous-catégorie '" + subCategory + "': " + e.getMessage());
            return 0;
        }
    }

}