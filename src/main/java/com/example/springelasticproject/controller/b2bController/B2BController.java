package com.example.springelasticproject.controller.b2bController;

import com.example.springelasticproject.Services.b2bService.B2BScoreServiceFast;
import com.example.springelasticproject.Services.b2bService.B2BService;
import com.example.springelasticproject.Services.b2bService.ExportB2BService;
import com.example.springelasticproject.model.b2bModel.B2B;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/b2b") // Chemin de base pour les requêtes B2B
@CrossOrigin(origins = "*") // Permet les requêtes cross-origin (important pour React)
public class B2BController {

    private final B2BService B2bService;
    private final ExportB2BService exportB2BService;
    private final B2BScoreServiceFast b2bScoreService;

    @Autowired
    public B2BController(B2BService B2bService, ExportB2BService exportB2BService, B2BScoreServiceFast b2bScoreService ) {
        this.B2bService = B2bService;
        this.exportB2BService = exportB2BService;
        this.b2bScoreService = b2bScoreService;

    }

    // Créer une nouvelle boutique de réparation
    @PostMapping
    public ResponseEntity<B2B> createB2B(@RequestBody B2B B2B) {
        B2B savedShop = B2bService.save(B2B);
        return new ResponseEntity<>(savedShop, HttpStatus.CREATED);
    }

    // Obtenir toutes les boutiques (paginées)
    @GetMapping
    public ResponseEntity<Page<B2B>> getAllB2Bs(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false) String sortBy) {
        try {
            PageRequest pageRequest;
            if (sortBy != null && !sortBy.isEmpty()) {
                pageRequest = PageRequest.of(page, size, Sort.by(sortBy));
            } else {
                // Sans tri si aucun champ de tri n'est spécifié
                pageRequest = PageRequest.of(page, size);
            }
            Page<B2B> shops = B2bService.findAll(pageRequest);
            return new ResponseEntity<>(shops, HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // Obtenir une boutique par ID
    @GetMapping("/{id}")
    public ResponseEntity<B2B> getB2BById(@PathVariable String id) {
        Optional<B2B> shop = B2bService.findById(id);
        return shop.map(value -> new ResponseEntity<>(value, HttpStatus.OK))
                .orElseGet(() -> new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    // Mettre à jour une boutique
    @PutMapping("/{id}")
    public ResponseEntity<B2B> updateB2B(@PathVariable String id, @RequestBody B2B B2B) {
        if (!B2bService.existsById(id)) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        B2B.setPlaceId(id);
        B2B updatedShop = B2bService.save(B2B);
        return new ResponseEntity<>(updatedShop, HttpStatus.OK);
    }

    // Supprimer une boutique
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteB2B(@PathVariable String id) {
        if (!B2bService.existsById(id)) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        B2bService.deleteById(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    @DeleteMapping
    public ResponseEntity<Void> deleteAll() {

        B2bService.deleteAll();
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }



    // Rechercher des boutiques par nom
    @GetMapping("/search/name")
    public ResponseEntity<List<B2B>> searchByName(@RequestParam String name) {
        List<B2B> shops = B2bService.findByName(name);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques par catégorie principale
    @GetMapping("/search/category")
    public ResponseEntity<List<B2B>> searchByCategory(@RequestParam String category) {
        List<B2B> shops = B2bService.findByMainCategory(category);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques par ville
    @GetMapping("/search/city")
    public ResponseEntity<List<B2B>> searchByCity(@RequestParam String city) {
        List<B2B> shops = B2bService.findByCity(city);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques par code postal
    @GetMapping("/search/postal-code")
    public ResponseEntity<List<B2B>> searchByPostalCode(@RequestParam String postalCode) {
        List<B2B> shops = B2bService.findByPostalCode(postalCode);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques par note minimale
    @GetMapping("/search/rating")
    public ResponseEntity<List<B2B>> searchByMinimumRating(@RequestParam Float rating) {
        List<B2B> shops = B2bService.findByMinimumRating(rating);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques par plage de notes
    @GetMapping("/search/rating-range")
    public ResponseEntity<List<B2B>> searchByRatingRange(
            @RequestParam Float minRating,
            @RequestParam Float maxRating) {
        List<B2B> shops = B2bService.findByRatingRange(minRating, maxRating);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques par nombre minimum d'avis
    @GetMapping("/search/min-reviews")
    public ResponseEntity<List<B2B>> searchByMinimumReviews(@RequestParam Integer minReviews) {
        List<B2B> shops = B2bService.findByMinimumReviews(minReviews);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques ouvertes un jour spécifique
    @GetMapping("/search/open")
    public ResponseEntity<List<B2B>> searchByOpenDay(@RequestParam String day) {
        List<B2B> shops = B2bService.findOpenOn(day);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques par numéro de téléphone
    @GetMapping("/search/phone")
    public ResponseEntity<List<B2B>> searchByPhone(@RequestParam String phone) {
        List<B2B> shops = B2bService.findByPhone(phone);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques par statut
    @GetMapping("/search/status")
    public ResponseEntity<List<B2B>> searchByStatus(@RequestParam String status) {
        List<B2B> shops = B2bService.findByStatus(status);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Recherche textuelle dans plusieurs champs
    @GetMapping("/search")
    public ResponseEntity<List<B2B>> searchShops(@RequestParam String query) {
        List<B2B> shops = B2bService.searchByText(query);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Rechercher des boutiques à proximité d'une position
    @GetMapping("/search/nearby")
    public ResponseEntity<List<B2B>> searchNearbyShops(
            @RequestParam Double latitude,
            @RequestParam Double longitude,
            @RequestParam(defaultValue = "5.0") Double distance) {
        List<B2B> shops = B2bService.findNearbyShops(latitude, longitude, distance);
        return new ResponseEntity<>(shops, HttpStatus.OK);
    }

    // Endpoints de gestion d'index
    @PostMapping("/reindex")
    public ResponseEntity<Void> reindexAll() {
        B2bService.reindexAll();
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @DeleteMapping("/index")
    public ResponseEntity<Void> deleteIndex() {
        B2bService.deleteIndex();
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PostMapping("/index")
    public ResponseEntity<Void> createIndex() {
        B2bService.createIndex();
        return new ResponseEntity<>(HttpStatus.OK);
    }


    @PostMapping("/searchByA04Fus")
    public ResponseEntity<Map<String, Object>> searchByAttributes04Fus(
            @RequestBody Map<String, String> attributes,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(defaultValue = "_score") String sortBy,
            @RequestParam(defaultValue = "desc") String direction) {
        // Création de l'objet Pageable pour la pagination
        Sort.Direction sortDirection = direction.equalsIgnoreCase("asc") ?
                Sort.Direction.ASC : Sort.Direction.DESC;

        Pageable pageable = PageRequest.of(page, size, Sort.by(sortDirection, sortBy));

        // Appel à la méthode de recherche
        Map<String, Object> searchResults = B2bService.searchByAttributes04Fuss(attributes, pageable);


        return ResponseEntity.ok(searchResults);
    }
    @PostMapping("/searchByA04")
    public ResponseEntity<Map<String, Object>> searchByAttributes04NoFus(
            @RequestBody Map<String, String> attributes,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(defaultValue = "_score") String sortBy,
            @RequestParam(defaultValue = "desc") String direction) {
        // Création de l'objet Pageable pour la pagination
        Sort.Direction sortDirection = direction.equalsIgnoreCase("asc") ?
                Sort.Direction.ASC : Sort.Direction.DESC;

        Pageable pageable = PageRequest.of(page, size, Sort.by(sortDirection, sortBy));

        // Appel à la méthode de recherche
        Map<String, Object> searchResults = B2bService.searchByAttributes04Exact(attributes, pageable);


        return ResponseEntity.ok(searchResults);
    }


    @PostMapping("/export-csvB2B")
    public void exportUsersToCsv(@RequestBody Map<String, String> attributes, HttpServletResponse response) throws IOException {
        exportB2BService.ExportSearchDataB2BExact(attributes ,response); // méthode modifiée pour retourner une liste
    }
    @PostMapping("/export-csvB2B-fus")
    public void exportUsersToCsv_fus(@RequestBody Map<String, String> attributes, HttpServletResponse response) throws IOException {
        exportB2BService.ExportSearchDataB2BFuzzy(attributes ,response); // méthode modifiée pour retourner une liste
    }

    @PostMapping("/updateScores")
    public ResponseEntity<String> updateScores() {
        b2bScoreService.calculateAndStoreScores();
        return ResponseEntity.ok("Scores updated successfully.");
    }

}
