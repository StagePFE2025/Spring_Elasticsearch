package com.example.springelasticproject.repository;


import com.example.springelasticproject.model.b2bModel.B2B;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface B2BRepository extends ElasticsearchRepository<B2B, String> {

    // Rechercher par nom
    List<B2B> findByNameContainingIgnoreCase(String name);

    // Rechercher par catégorie principale
    List<B2B> findByMainCategory(String mainCategory);

    // Rechercher par ville
    List<B2B> findByCity(String city);

    // Rechercher par code postal
    List<B2B> findByPostalCode(String postalCode);

    // Rechercher les boutiques ayant une note minimale
    List<B2B> findByRatingGreaterThanEqual(Float rating);

    // Rechercher les boutiques ouvertes un jour spécifique
    List<B2B> findByClosedOnNotContaining(String day);

    // Rechercher les boutiques par téléphone
    List<B2B> findByPhoneContaining(String phone);

    // Rechercher les boutiques par description
    List<B2B> findByDescriptionContaining(String text);

    // Requête géospatiale - Trouver les boutiques dans un certain rayon
    @Query("{\"bool\": {\"must\": {\"match_all\": {}}, \"filter\": {\"geo_distance\": {\"distance\": \"?0km\", \"coordinates\": {\"lat\": ?1, \"lon\": ?2}}}}}")
    List<B2B> findByGeoDistance(Double distance, Double latitude, Double longitude);

    // Rechercher par catégorie
    List<B2B> findByCategoriesContaining(String category);

    // Rechercher par statut (ouvert, fermé, etc.)
    List<B2B> findByStatus(String status);

    // Rechercher par plage de notes
    List<B2B> findByRatingBetween(Float minRating, Float maxRating);

    // Rechercher par nombre minimum d'avis
    List<B2B> findByReviewsGreaterThanEqual(Integer minReviews);

    // Recherche multi-champs
    @Query("{\"bool\": {\"should\": [{\"match\": {\"name\": \"?0\"}}, {\"match\": {\"description\": \"?0\"}}, {\"match\": {\"mainCategory\": \"?0\"}}, {\"match\": {\"categories\": \"?0\"}}]}}")
    List<B2B> searchAcrossFields(String searchText);
}