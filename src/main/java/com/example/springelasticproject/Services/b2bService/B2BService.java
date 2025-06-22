package com.example.springelasticproject.Services.b2bService;

import com.example.springelasticproject.model.b2bModel.B2B;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface B2BService {

    // Opérations CRUD de base
    B2B save(B2B B2B);
    Optional<B2B> findById(String id);
    List<B2B> findAll();
    Page<B2B> findAll(Pageable pageable);
    void deleteById(String id);
    public void deleteAll();
    boolean existsById(String id);
    long count();

    // Opérations spécifiques métier
    List<B2B> findByName(String name);
    List<B2B> findByMainCategory(String mainCategory);
    List<B2B> findByCity(String city);
    List<B2B> findByPostalCode(String postalCode);
    List<B2B> findByMinimumRating(Float rating);
    List<B2B> findByRatingRange(Float minRating, Float maxRating);
    List<B2B> findByMinimumReviews(Integer minReviews);
    List<B2B> findOpenOn(String day);
    List<B2B> findByPhone(String phone);
    List<B2B> findByStatus(String status);
    List<B2B> searchByText(String searchText);
    List<B2B> findNearbyShops(Double latitude, Double longitude, Double distanceInKm);
    List<B2B> findByCategory(String category);

    // Opérations sur l'index
    void indexAllShops(List<B2B> shops);
    void reindexAll();
    void deleteIndex();
    void createIndex();
    public Map<String, Object> searchByAttributes04Fuss(Map<String, String> attributes, Pageable pageable);
    public Map<String, Object> searchByAttributes04Exact(Map<String, String> attributes, Pageable pageable);
}
