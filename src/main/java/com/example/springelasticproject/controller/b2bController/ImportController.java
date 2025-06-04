package com.example.springelasticproject.controller.b2bController;

import com.example.springelasticproject.util.DataImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/import")
@CrossOrigin(origins = "*") // Permet les requêtes cross-origin (important pour React)
public class ImportController {

    private static final Logger logger = LoggerFactory.getLogger(ImportController.class);
    private final DataImporter dataImporter;

    @Autowired
    public ImportController(DataImporter dataImporter) {
        this.dataImporter = dataImporter;
    }

    /**
     * Endpoint pour importer des données NDJSON depuis un dossier spécifié
     * @param path Chemin vers le dossier contenant les fichiers NDJSON
     * @return Résultat de l'importation
     */
    @PostMapping("/directory")
    public ResponseEntity<Map<String, Object>> importFromDirectory(@RequestParam String path) {
        Map<String, Object> response = new HashMap<>();

        try {
            File directory = new File(path);

            if (!directory.exists() || !directory.isDirectory()) {
                response.put("status", "error");
                response.put("message", "Le chemin spécifié n'est pas un dossier valide: " + path);
                return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
            }

            long startTime = System.currentTimeMillis();
            dataImporter.importDataFromDirectory(path);
            long endTime = System.currentTimeMillis();

            response.put("status", "success");
            response.put("message", "Importation des données terminée");
            response.put("directory", path);
            response.put("executionTimeMs", endTime - startTime);

            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Erreur lors de l'importation des données: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", "Erreur lors de l'importation: " + e.getMessage());
            return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Endpoint pour importer des données NDJSON (en passant le chemin dans le corps de la requête)
     * @param request Map contenant le chemin du dossier
     * @return Résultat de l'importation
     */
    @PostMapping("/import-ndjson")
    public ResponseEntity<Map<String, Object>> importNdjsonWithBody(@RequestBody Map<String, String> request) {
        String directoryPath = request.get("directoryPath");

        if (directoryPath == null || directoryPath.isEmpty()) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Le paramètre 'directoryPath' est requis");
            return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
        }

        return importFromDirectory(directoryPath);
    }

    /**
     * Endpoint pour importer un fichier NDJSON spécifique
     * @param path Chemin vers le fichier NDJSON
     * @return Résultat de l'importation
     */
    @PostMapping("/file")
    public ResponseEntity<Map<String, Object>> importFile(@RequestParam String path) {
        Map<String, Object> response = new HashMap<>();

        try {
            File file = new File(path);

            if (!file.exists() || !file.isFile() || !file.getName().toLowerCase().endsWith(".ndjson")) {
                response.put("status", "error");
                response.put("message", "Le chemin spécifié n'est pas un fichier NDJSON valide: " + path);
                return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
            }

            long startTime = System.currentTimeMillis();
            int importedCount = dataImporter.importDataFromNDJsonFile(path).size();
            long endTime = System.currentTimeMillis();

            response.put("status", "success");
            response.put("message", "Importation du fichier terminée");
            response.put("file", path);
            response.put("importedCount", importedCount);
            response.put("executionTimeMs", endTime - startTime);

            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Erreur lors de l'importation du fichier: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", "Erreur lors de l'importation: " + e.getMessage());
            return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Endpoint pour forcer la réindexation complète de toutes les données
     * @return Résultat de l'opération
     */
    @PostMapping("/reindex")
    public ResponseEntity<Map<String, Object>> forceReindex() {
        Map<String, Object> response = new HashMap<>();

        try {
            long startTime = System.currentTimeMillis();
            dataImporter.forceReimport();
            long endTime = System.currentTimeMillis();

            response.put("status", "success");
            response.put("message", "Réindexation complète des données terminée");
            response.put("executionTimeMs", endTime - startTime);

            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Erreur lors de la réindexation des données: {}", e.getMessage(), e);
            response.put("status", "error");
            response.put("message", "Erreur lors de la réindexation: " + e.getMessage());
            return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}