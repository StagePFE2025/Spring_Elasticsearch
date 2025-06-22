package com.example.springelasticproject.controller.b2bController.ShadowpilotController;

import com.example.springelasticproject.Services.b2bService.ShadowPilotServices.ExportShadowpilotService;
import com.example.springelasticproject.Services.b2bService.ShadowPilotServices.ShadowPilotService;

import com.example.springelasticproject.model.b2bModel.ShadowPilot.ShadowPilot;
import com.example.springelasticproject.util.CsvToBusinessReviewTransformer;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/api/businesses")
@CrossOrigin(origins = "*")
public class ShadowPilotController {

    @Autowired
    private ShadowPilotService shadowPilotService;

    @Autowired
    private CsvToBusinessReviewTransformer csvTransformer;
    @Autowired
    private ExportShadowpilotService exportShadowpilotService;

    /**
     * Recherche toutes les entreprises avec pagination
     */
    @GetMapping
    public ResponseEntity<Page<ShadowPilot>> getAllBusinesses(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        Page<ShadowPilot> businesses = shadowPilotService.findAll(page, size);
        return ResponseEntity.ok(businesses);
    }

    /**
     * Recherche par ID
     */
    @GetMapping("/{id}")
    public ResponseEntity<ShadowPilot> getBusinessById(@PathVariable String id) {
        Optional<ShadowPilot> business = shadowPilotService.findById(id);
        return business.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Recherche par nom
     */
    @GetMapping("/search/name")
    public ResponseEntity<List<ShadowPilot>> searchByName(@RequestParam String name) {
        try {
            // Validation des paramètres
            if (name == null || name.trim().length() < 2) {
                return ResponseEntity.badRequest().build();
            }

            List<ShadowPilot> businesses = shadowPilotService.findByName(name);
            return ResponseEntity.ok(businesses);

        } catch (InvalidDataAccessApiUsageException e) {
            // Log l'erreur spécifique Elasticsearch

            return ResponseEntity.ok(Collections.emptyList()); // Retourner liste vide plutôt qu'erreur

        } catch (Exception e) {

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Recherche par localisation
     */
    @GetMapping("/search/location")
    public ResponseEntity<List<ShadowPilot>> searchByLocation(@RequestParam String location) {
        List<ShadowPilot> businesses = shadowPilotService.findByLocation(location);
        return ResponseEntity.ok(businesses);
    }

    /**
     * Recherche par critères multiples
     */
    @GetMapping("/search/criteria")
    public ResponseEntity<Page<ShadowPilot>> searchByCriteria(
            @RequestParam(required = false) Double minTrustscore,
            @RequestParam(required = false) Double minReviews,
            @RequestParam(required = false) String category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Page<ShadowPilot> businesses = shadowPilotService.searchByCriteria(
                minTrustscore, minReviews, category, page, size);
        return ResponseEntity.ok(businesses);
    }

    /**
     * Recherche textuelle libre
     */
    @GetMapping("/search/fulltext")
    public ResponseEntity<List<ShadowPilot>> fullTextSearch(@RequestParam String query) {
        List<ShadowPilot> businesses = shadowPilotService.fullTextSearch(query);
        return ResponseEntity.ok(businesses);
    }

    /**
     * Statistiques générales
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        Map<String, Object> stats = shadowPilotService.getGeneralStats();
        return ResponseEntity.ok(stats);
    }

    /**
     * Top entreprises par trustscore
     */
    @GetMapping("/top/trustscore")
    public ResponseEntity<List<ShadowPilot>> getTopByTrustscore(
            @RequestParam(defaultValue = "10") int limit) {
        List<ShadowPilot> businesses = shadowPilotService.getTopByTrustscore(limit);
        return ResponseEntity.ok(businesses);
    }

    /**
     * Entreprises de haute qualité
     */
    @GetMapping("/high-quality")
    public ResponseEntity<List<ShadowPilot>> getHighQualityBusinesses() {
        List<ShadowPilot> businesses = shadowPilotService.findHighQualityBusinesses();
        return ResponseEntity.ok(businesses);
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
        Map<String, Object> searchResults = shadowPilotService.searchShadowPilotByAttributesFuzzy(attributes, pageable);


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
        Map<String, Object> searchResults = shadowPilotService.searchShadowPilotByAttributesExact(attributes, pageable);


        return ResponseEntity.ok(searchResults);
    }


    @PostMapping("/export-csvB2B")
    public void exportUsersToCsv(@RequestBody Map<String, String> attributes, HttpServletResponse response) throws IOException {
        exportShadowpilotService.ExportSearchDataShadowPilotExact(attributes ,response); // méthode modifiée pour retourner une liste
    }
    @PostMapping("/export-csvB2B-fus")
    public void exportUsersToCsv_fus(@RequestBody Map<String, String> attributes, HttpServletResponse response) throws IOException {
        exportShadowpilotService.ExportSearchDataShadowPilotFuzzy(attributes ,response); // méthode modifiée pour retourner une liste
    }







    @DeleteMapping("/subcategory/{subCategory}")
    public ResponseEntity<Map<String, Object>> deleteBySubCategory(@PathVariable String subCategory) {
        try {
            // Prévisualisation d'abord
            long count = shadowPilotService.countBySubCategory(subCategory);

            if (count == 0) {
                return ResponseEntity.notFound().build();
            }

            // Suppression
            long deleted = shadowPilotService.deleteBySubCategory(subCategory);

            Map<String, Object> response = new HashMap<>();
            response.put("subCategory", subCategory);
            response.put("deletedCount", deleted);
            response.put("message", "Suppression réussie");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Erreur lors de la suppression");
            error.put("message", e.getMessage());
            return ResponseEntity.status(500).body(error);
        }
    }

    /**
     * Prévisualise les entreprises qui seraient supprimées
     */
    @GetMapping("/subcategory/{subCategory}/preview-delete")
    public ResponseEntity<Map<String, Object>> previewDelete(@PathVariable String subCategory,
                                                             @RequestParam(defaultValue = "10") int limit) {
        try {
            List<ShadowPilot> preview = shadowPilotService.previewDeleteBySubCategory(subCategory, limit);
            long totalCount = shadowPilotService.countBySubCategory(subCategory);

            Map<String, Object> response = new HashMap<>();
            response.put("subCategory", subCategory);
            response.put("totalCount", totalCount);
            response.put("previewCount", preview.size());
            response.put("examples", preview);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Erreur lors de la prévisualisation");
            error.put("message", e.getMessage());
            return ResponseEntity.status(500).body(error);
        }
    }


}
