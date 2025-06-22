package com.example.springelasticproject.repository.ShadowPilotRepository;

import com.example.springelasticproject.model.b2bModel.ShadowPilot.ShadowPilot;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository Spring Data Elasticsearch pour FlexibleBusinessReview
 * Fournit des méthodes de recherche avancées pour les données d'entreprises
 */
@Repository
public interface ShadowPilotRepository extends ElasticsearchRepository<ShadowPilot, String> {

    // ===== RECHERCHES PAR CHAMPS DIRECTS =====

    /**
     * Recherche par nom d'entreprise (insensible à la casse)
     */
    List<ShadowPilot> findByNameIgnoreCase(String name);

    /**
     * Recherche par nom contenant une chaîne (insensible à la casse)
     */
    List<ShadowPilot> findByNameContainingIgnoreCase(String name);

    @Query("{\n" +
            "  \"bool\": {\n" +
            "    \"should\": [\n" +
            "      {\n" +
            "        \"match_phrase_prefix\": {\n" +
            "          \"name\": {\n" +
            "            \"query\": \"?0\"\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"match\": {\n" +
            "          \"name\": {\n" +
            "            \"query\": \"?0\",\n" +
            "            \"fuzziness\": \"AUTO\",\n" +
            "            \"operator\": \"and\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}")
    List<ShadowPilot> findByNameWithSpaceSupport(String name);
    /**
     * Recherche par domaine
     */
    Optional<ShadowPilot> findByDomain(String domain);

    /**
     * Recherche par adresse contenant une chaîne
     */
    List<ShadowPilot> findByAddressContainingIgnoreCase(String address);




    // ===== RECHERCHES AVANCÉES AVEC QUERY DSL =====

    /**
     * Recherche par trustscore minimum
     */
    @Query("{\"bool\": {\"must\": [{\"range\": {\"businessMetrics.trustscore\": {\"gte\": ?0}}}]}}")
    List<ShadowPilot> findByTrustscoreGreaterThanEqual(Double minTrustscore);

    /**
     * Recherche par nombre minimum d'avis
     */
    @Query("{\"bool\": {\"must\": [{\"range\": {\"businessMetrics.number_of_reviews\": {\"gte\": ?0}}}]}}")
    List<ShadowPilot> findByNumberOfReviewsGreaterThanEqual(Double minReviews);

    /**
     * Recherche par catégorie d'entreprise
     */
    @Query("{\"bool\": {\"must\": [{\"match\": {\"businessMetrics.categories\": {\"query\": \"?0\", \"operator\": \"and\"}}}]}}")
    List<ShadowPilot> findByCategory(String category);

    /**
     * Recherche d'entreprises avec réseaux sociaux
     */
    @Query("{\"bool\": {\"must\": [{\"term\": {\"socialMedia.has_social_media\": true}}]}}")
    List<ShadowPilot> findWithSocialMedia();

    /**
     * Recherche par plateforme de réseau social spécifique
     */
    @Query("{\"bool\": {\"must\": [{\"exists\": {\"field\": \"socialMedia.?0_url\"}}]}}")
    List<ShadowPilot> findBySocialMediaPlatform(String platform);

    /**
     * Recherche d'entreprises revendiquées
     */
    @Query("{\"bool\": {\"must\": [{\"term\": {\"businessMetrics.is_claimed\": true}}]}}")
    List<ShadowPilot> findClaimedBusinesses();

    /**
     * Recherche par âge minimum de l'entreprise (en jours)
     */
    @Query("{\"bool\": {\"must\": [{\"range\": {\"businessMetrics.business_age_days\": {\"gte\": ?0}}}]}}")
    List<ShadowPilot> findByMinimumAge(Integer minAgeDays);

    /**
     * Recherche par taille d'entreprise
     */
    @Query("{\"bool\": {\"must\": [{\"term\": {\"businessMetrics.business_size_indicator.keyword\": \"?0\"}}]}}")
    List<ShadowPilot> findByBusinessSize(String sizeIndicator);

    // ===== RECHERCHES SUR LES AVIS =====

    /**
     * Recherche d'entreprises avec avis récents (derniers 30 jours)
     */
    @Query("{\"bool\": {\"must\": [{\"range\": {\"businessMetrics.reviews_last_30_days\": {\"gt\": 0}}}]}}")
    List<ShadowPilot> findWithRecentReviews();

    /**
     * Recherche d'entreprises avec un nombre minimum d'avis vérifiés
     */
    @Query("{\"bool\": {\"must\": [{\"range\": {\"businessMetrics.verified_reviews_count\": {\"gte\": ?0}}}]}}")
    List<ShadowPilot> findByMinimumVerifiedReviews(Integer minVerified);

    /**
     * Recherche d'entreprises avec un taux de réponse minimum
     */
    @Query("{\"bool\": {\"must\": [{\"range\": {\"businessMetrics.response_rate\": {\"gte\": ?0}}}]}}")
    List<ShadowPilot> findByMinimumResponseRate(Double minResponseRate);

    // ===== RECHERCHES COMPLEXES =====

    /**
     * Recherche d'entreprises de haute qualité
     * (trustscore >= 4.0, au moins 50 avis, revendiquée)
     */
    @Query("{\"bool\": {\"must\": [" +
            "{\"range\": {\"businessMetrics.trustscore\": {\"gte\": 4.0}}}," +
            "{\"range\": {\"businessMetrics.number_of_reviews\": {\"gte\": 50}}}," +
            "{\"term\": {\"businessMetrics.is_claimed\": true}}" +
            "]}}")
    List<ShadowPilot> findHighQualityBusinesses();

    /**
     * Recherche multicritères avec pagination
     */
    @Query("{\"bool\": {\"must\": [" +
            "{\"range\": {\"businessMetrics.trustscore\": {\"gte\": ?0}}}," +
            "{\"range\": {\"businessMetrics.number_of_reviews\": {\"gte\": ?1}}}," +
            "{\"match\": {\"businessMetrics.categories\": \"?2\"}}" +
            "]}}")
    Page<ShadowPilot> findByMultipleCriteria(Double minTrustscore,
                                                        Double minReviews,
                                                        String category,
                                                        Pageable pageable);

    /**
     * Recherche par texte libre dans les noms et adresses
     */
    @Query("{\"bool\": {\"should\": [" +
            "{\"match\": {\"name\": {\"query\": \"?0\", \"boost\": 2}}}," +
            "{\"match\": {\"address\": \"?0\"}}" +
            "], \"minimum_should_match\": 1}}")
    List<ShadowPilot> findByFullTextSearch(String searchText);

    /**
     * Recherche d'entreprises similaires par catégorie et localisation
     */
    @Query("{\"bool\": {\"must\": [" +
            "{\"match\": {\"businessMetrics.categories\": \"?0\"}}," +
            "{\"match\": {\"address\": \"?1\"}}" +
            "], \"must_not\": [{\"term\": {\"_id\": \"?2\"}}]}}")
    List<ShadowPilot> findSimilarBusinesses(String category, String location, String excludeId);

    // ===== STATISTIQUES ET AGRÉGATIONS =====

    /**
     * Compte le nombre d'entreprises par trustscore minimum
     */
    @Query("{\"bool\": {\"must\": [{\"range\": {\"businessMetrics.trustscore\": {\"gte\": ?0}}}]}}")
    long countByTrustscoreGreaterThanEqual(Double minTrustscore);

    /**
     * Recherche les top entreprises par nombre d'avis
     */
    @Query("{\"match_all\": {}}")
    Page<ShadowPilot> findTopByReviews(Pageable pageable);


    void deleteBySubCategory(String subCategory);

    /**
     * Supprime toutes les entreprises d'une sous-catégorie (insensible à la casse)
     */
    void deleteBySubCategoryIgnoreCase(String subCategory);

    /**
     * Compte les entreprises à supprimer par sous-catégorie (pour vérification)
     */
    long countBySubCategory(String subCategory);

    /**
     * Trouve les entreprises par sous-catégorie (pour vérification avant suppression)
     */
    List<ShadowPilot> findBySubCategory(String subCategory);


}