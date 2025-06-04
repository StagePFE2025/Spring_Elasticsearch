package com.example.springelasticproject.Services.b2bService;

import com.example.springelasticproject.model.b2bModel.B2B;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Iterator;

@Service
public class ImportB2BService {

    private final B2BService b2BService;
    private final ObjectMapper objectMapper;

    @Autowired
    public ImportB2BService(B2BService b2BService, ObjectMapper objectMapper) {
        this.b2BService = b2BService;
        this.objectMapper = objectMapper;
    }

    /**
     * Importe les données NDJSON depuis un dossier
     * @param directoryPath chemin du dossier contenant les fichiers NDJSON
     * @return statistiques d'importation
     */
    public Map<String, Object> importFromDirectory(String directoryPath) {
        Map<String, Object> result = new HashMap<>();
        List<String> processedFiles = new ArrayList<>();
        int totalRecords = 0;
        int successCount = 0;
        int errorCount = 0;

        File directory = new File(directoryPath);
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IllegalArgumentException("Le chemin spécifié n'est pas un dossier valide: " + directoryPath);
        }

        File[] files = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".ndjson"));
        if (files == null || files.length == 0) {
            result.put("message", "Aucun fichier NDJSON trouvé dans le dossier");
            return result;
        }

        for (File file : files) {
            try {
                Map<String, Object> fileStats = processNdjsonFile(file);
                processedFiles.add(file.getName());
                totalRecords += (int) fileStats.get("totalLines");
                successCount += (int) fileStats.get("successCount");
                errorCount += (int) fileStats.get("errorCount");
            } catch (Exception e) {
                errorCount++;
                System.err.println("Erreur lors du traitement du fichier " + file.getName() + ": " + e.getMessage());
            }
        }

        result.put("processedFiles", processedFiles);
        result.put("totalRecords", totalRecords);
        result.put("successCount", successCount);
        result.put("errorCount", errorCount);

        return result;
    }

    /**
     * Traite un seul fichier NDJSON
     */
    private Map<String, Object> processNdjsonFile(File file) throws IOException {
        int totalLines = 0;
        int successCount = 0;
        int errorCount = 0;
        List<B2B> batch = new ArrayList<>();
        final int BATCH_SIZE = 500; // Taille du lot pour l'importation en masse

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                totalLines++;

                try {
                    // Convertir snake_case en camelCase
                    JsonNode jsonNode = objectMapper.readTree(line);
                    JsonNode transformedNode = transformJsonKeys(jsonNode);
                    String transformedJson = objectMapper.writeValueAsString(transformedNode);

                    // Désérialiser le JSON transformé
                    B2B shop = objectMapper.readValue(transformedJson, B2B.class);
                    batch.add(shop);

                    // Insérer par lots pour optimiser les performances
                    if (batch.size() >= BATCH_SIZE) {
                        b2BService.indexAllShops(batch);
                        successCount += batch.size();
                        batch.clear();
                    }
                } catch (Exception e) {
                    errorCount++;
                    System.err.println("Erreur à la ligne " + totalLines + " du fichier " + file.getName() + ": " + e.getMessage());
                }
            }

            // Traiter le reste du lot
            if (!batch.isEmpty()) {
                b2BService.indexAllShops(batch);
                successCount += batch.size();
            }
        }

        Map<String, Object> stats = new HashMap<>();
        stats.put("fileName", file.getName());
        stats.put("totalLines", totalLines);
        stats.put("successCount", successCount);
        stats.put("errorCount", errorCount);

        return stats;
    }

    /**
     * Transforme les clés JSON de snake_case en camelCase
     */
    private JsonNode transformJsonKeys(JsonNode node) {
        if (node.isObject()) {
            ObjectNode objectNode = objectMapper.createObjectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = field.getKey();
                String camelCaseKey = snakeToCamelCase(key);
                JsonNode value = field.getValue();

                // Transformation récursive pour les objets imbriqués et les tableaux
                if (value.isObject() || value.isArray()) {
                    objectNode.set(camelCaseKey, transformJsonKeys(value));
                } else {
                    objectNode.set(camelCaseKey, value);
                }
            }

            return objectNode;
        } else if (node.isArray()) {
            // Traitement pour les tableaux - transformer chaque élément
            List<JsonNode> transformedElements = new ArrayList<>();
            for (JsonNode element : node) {
                transformedElements.add(transformJsonKeys(element));
            }

            return objectMapper.valueToTree(transformedElements);
        }

        return node;
    }

    /**
     * Convertit une chaîne de caractères de snake_case en camelCase
     */
    private String snakeToCamelCase(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        StringBuilder sb = new StringBuilder();
        boolean capitalizeNext = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (c == '_') {
                capitalizeNext = true;
            } else if (capitalizeNext) {
                sb.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }
}