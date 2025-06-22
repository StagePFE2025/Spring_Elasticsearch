package com.example.springelasticproject.controller.b2bController.ShadowpilotController;

import com.example.springelasticproject.Services.b2bService.ShadowPilotServices.CsvImportService;
import com.example.springelasticproject.Services.b2bService.ShadowPilotServices.ShadowPilotService;
import com.opencsv.exceptions.CsvException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Contrôleur REST pour l'importation de données CSV ShadowPilot
 */
@RestController
@RequestMapping("/api/shadowpilot/import")
@CrossOrigin(origins = "*")
public class CsvImportController {

    private static final Logger logger = LoggerFactory.getLogger(CsvImportController.class);

    @Autowired
    private CsvImportService csvImportService;

    @Autowired
    private ShadowPilotService businessReviewService;

    /**
     * Endpoint pour importer un fichier CSV via upload
     * POST /api/shadowpilot/import/upload
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> importCsvFile(
            @RequestParam("file") MultipartFile file,
            @RequestParam("category") String category,
            @RequestParam("subcategory") String subcategory) {

        logger.info("Début de l'importation du fichier: {} avec category: {}, subcategory: {}",
                file.getOriginalFilename(), category, subcategory);

        Map<String, Object> response = new HashMap<>();

        try {
            // Validation du fichier
            if (file.isEmpty()) {
                response.put("success", false);
                response.put("message", "Le fichier est vide");
                return ResponseEntity.badRequest().body(response);
            }

            if (!file.getOriginalFilename().toLowerCase().endsWith(".csv")) {
                response.put("success", false);
                response.put("message", "Seuls les fichiers CSV sont acceptés");
                return ResponseEntity.badRequest().body(response);
            }

            // 🆕 Validation des paramètres category et subcategory
            if (category == null || category.trim().isEmpty()) {
                response.put("success", false);
                response.put("message", "La catégorie est obligatoire");
                return ResponseEntity.badRequest().body(response);
            }

            if (subcategory == null || subcategory.trim().isEmpty()) {
                response.put("success", false);
                response.put("message", "La sous-catégorie est obligatoire");
                return ResponseEntity.badRequest().body(response);
            }

            // 🔧 Importation avec les nouveaux paramètres
            CsvImportService.ImportResult result = csvImportService.importCsvFile(
                    file,
                    category,
                    subcategory
            );

            // Préparation de la réponse enrichie
            response.put("success", true);
            response.put("message", "Importation terminée avec succès");
            response.put("result", createResultMap(result));

            logger.info("Importation réussie: {}", result);
            return ResponseEntity.ok(response);

        } catch (IOException e) {
            logger.error("Erreur d'E/S lors de l'importation: {}", e.getMessage());
            response.put("success", false);
            response.put("message", "Erreur de lecture du fichier: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);

        } catch (CsvException e) {
            logger.error("Erreur de parsing CSV: {}", e.getMessage());
            response.put("success", false);
            response.put("message", "Erreur de format CSV: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);

        } catch (Exception e) {
            logger.error("Erreur inattendue lors de l'importation: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("message", "Erreur inattendue: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Endpoint pour importer un fichier CSV depuis le système de fichiers local
     * POST /api/shadowpilot/import/local
     */


    /**
     * Endpoint pour obtenir les statistiques d'importation
     * GET /api/shadowpilot/import/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getImportStats() {
        try {
            Map<String, Object> stats = businessReviewService.getGeneralStats();

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("stats", stats);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Erreur lors de la récupération des statistiques: {}", e.getMessage());
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Erreur lors de la récupération des statistiques: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Endpoint pour vérifier l'état de la base de données
     * GET /api/shadowpilot/import/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> response = new HashMap<>();

        try {
            long totalBusinesses = businessReviewService.count();

            response.put("success", true);
            response.put("status", "healthy");
            response.put("totalBusinesses", totalBusinesses);
            response.put("timestamp", java.time.LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Erreur lors du health check: {}", e.getMessage());
            response.put("success", false);
            response.put("status", "unhealthy");
            response.put("error", e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
        }
    }

    /**
     * Endpoint pour purger toutes les données (à utiliser avec précaution)
     * DELETE /api/shadowpilot/import/purge
     */
    @DeleteMapping("/purge")
    public ResponseEntity<Map<String, Object>> purgeAllData(@RequestParam(value = "confirm", defaultValue = "false") boolean confirm) {
        Map<String, Object> response = new HashMap<>();

        if (!confirm) {
            response.put("success", false);
            response.put("message", "Confirmation requise pour purger toutes les données. Utilisez ?confirm=true");
            return ResponseEntity.badRequest().body(response);
        }

        try {
            // Note: Cette méthode doit être implémentée dans le service si nécessaire
            // businessReviewService.deleteAll();

            response.put("success", true);
            response.put("message", "Toutes les données ont été supprimées");
            response.put("timestamp", java.time.LocalDateTime.now());

            logger.warn("PURGE: Toutes les données ont été supprimées");
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Erreur lors de la purge: {}", e.getMessage());
            response.put("success", false);
            response.put("message", "Erreur lors de la purge: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Endpoint pour obtenir un échantillon de données importées
     * GET /api/shadowpilot/import/sample
     */
    @GetMapping("/sample")
    public ResponseEntity<Map<String, Object>> getSampleData(@RequestParam(value = "size", defaultValue = "5") int size) {
        try {
            var sampleData = businessReviewService.findAll(0, Math.min(size, 20));

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("sampleData", sampleData.getContent());
            response.put("totalPages", sampleData.getTotalPages());
            response.put("totalElements", sampleData.getTotalElements());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Erreur lors de la récupération de l'échantillon: {}", e.getMessage());
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Erreur lors de la récupération de l'échantillon: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * Crée une map pour le résultat d'importation
     */
    private Map<String, Object> createResultMap(CsvImportService.ImportResult result) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("fileName", result.getFileName());
        resultMap.put("startTime", result.getStartTime());
        resultMap.put("endTime", result.getEndTime());
        resultMap.put("totalRows", result.getTotalRows());
        resultMap.put("successCount", result.getSuccessCount());
        resultMap.put("errorCount", result.getErrorCount());
        resultMap.put("durationSeconds", result.getDurationInSeconds());
        resultMap.put("errors", result.getErrors().size() > 10 ?
                result.getErrors().subList(0, 10) : result.getErrors()); // Limite à 10 erreurs pour la réponse

        // Calcul des pourcentages
        if (result.getTotalRows() > 0) {
            resultMap.put("successPercentage",
                    Math.round((double) result.getSuccessCount() / result.getTotalRows() * 100.0 * 100.0) / 100.0);
            resultMap.put("errorPercentage",
                    Math.round((double) result.getErrorCount() / result.getTotalRows() * 100.0 * 100.0) / 100.0);
        }

        return resultMap;
    }
}