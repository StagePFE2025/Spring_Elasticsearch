package com.example.springelasticproject.util;

import com.example.springelasticproject.Services.b2bService.B2BService;
import com.example.springelasticproject.model.b2bModel.B2B;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class DataImporter implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(DataImporter.class);
    private final B2BService b2BService;
    private final ObjectMapper objectMapper;

    @Value("${data.import.directory:data}")
    private String dataDirectory;

    @Autowired
    public DataImporter(B2BService b2BService, ObjectMapper objectMapper) {
        this.b2BService = b2BService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(String... args) {
        // Exécuté au démarrage de l'application
        if (b2BService.count() == 0) {
            logger.info("Aucune boutique trouvée dans l'index. Démarrage de l'importation...");
            importDataFromDirectory(dataDirectory);
        } else {
            logger.info("Des boutiques existent déjà dans l'index. Import ignoré.");
        }
    }

    /**
     * Importe tous les fichiers NDJSON d'un répertoire
     * @param directoryPath Chemin vers le répertoire contenant les fichiers NDJSON
     */
    public void importDataFromDirectory(String directoryPath) {
        try {
            Path directory = Paths.get(directoryPath);

            // Vérifier si le répertoire existe
            if (!Files.exists(directory)) {
                logger.error("Le répertoire {} n'existe pas", directoryPath);
                return;
            }

            // Créer l'index avant d'importer des données
            b2BService.createIndex();

            // Trouver tous les fichiers NDJSON dans le répertoire
            List<Path> ndjsonFiles;
            try (Stream<Path> paths = Files.walk(directory)) {
                ndjsonFiles = paths
                        .filter(Files::isRegularFile)
                        .filter(path -> path.toString().toLowerCase().endsWith(".ndjson"))
                        .collect(Collectors.toList());
            }

            if (ndjsonFiles.isEmpty()) {
                logger.warn("Aucun fichier NDJSON trouvé dans le répertoire {}", directoryPath);
                return;
            }

            // Importer chaque fichier
            int totalImported = 0;
            for (Path file : ndjsonFiles) {
                logger.info("Importation du fichier: {}", file.getFileName());
                List<B2B> shops = importDataFromNDJsonFile(file.toString());
                if (!shops.isEmpty()) {
                    // Importer les données par lots pour éviter les problèmes de mémoire
                    int batchSize = 10000;
                    List<List<B2B>> batches = splitIntoBatches(shops, batchSize);

                    for (List<B2B> batch : batches) {
                        try {
                            b2BService.indexAllShops(batch);
                            totalImported += batch.size();
                            logger.info("{} boutiques indexées (total: {})", batch.size(), totalImported);
                        } catch (Exception e) {
                            logger.error("Erreur lors de l'indexation d'un lot: {}", e.getMessage());
                            // Traiter la boutique par boutique en cas d'erreur
                            for (B2B shop : batch) {
                                try {
                                    b2BService.save(shop);
                                    totalImported++;
                                } catch (Exception ex) {
                                    logger.error("Erreur lors de l'indexation de la boutique {}: {}", shop.getPlaceId(), ex.getMessage());
                                }
                            }
                        }
                    }

                    logger.info("{} boutiques importées depuis {}", shops.size(), file.getFileName());
                } else {
                    logger.warn("Aucune boutique n'a été importée depuis le fichier {}", file.getFileName());
                }
            }

            logger.info("Importation terminée. {} boutiques importées au total", totalImported);

        } catch (IOException e) {
            logger.error("Erreur lors de la lecture du répertoire {}: {}", directoryPath, e.getMessage());
        }
    }

    /**
     * Divise une liste en lots de taille spécifiée
     */
    private <T> List<List<T>> splitIntoBatches(List<T> items, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += batchSize) {
            batches.add(items.subList(i, Math.min(i + batchSize, items.size())));
        }
        return batches;
    }

    /**
     * Importe des données à partir d'un fichier NDJSON spécifique
     * @param filePath Chemin vers le fichier NDJSON
     * @return Liste des objets B2B importés
     */
    public List<B2B> importDataFromNDJsonFile(String filePath) {
        List<B2B> shops = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int lineCount = 0;

            while ((line = reader.readLine()) != null) {
                lineCount++;
                try {
                    JsonNode jsonNode = objectMapper.readTree(line);

                    // Créer un objet B2B et définir les propriétés depuis jsonNode
                    B2B shop = new B2B();

                    // Propriétés principales
                    shop.setPlaceId(getTextValue(jsonNode, "place_id"));
                    shop.setName(getTextValue(jsonNode, "name"));
                    shop.setDescription(getTextValue(jsonNode, "description"));
                    shop.setIsSpendingOnAds(getBooleanValue(jsonNode, "is_spending_on_ads"));
                    shop.setReviews(getIntValue(jsonNode, "reviews"));
                    shop.setRating(getFloatValue(jsonNode, "rating"));

                    // Adresse et localisation
                    shop.setAddress(getTextValue(jsonNode, "address"));

                    // Traitement spécial pour les coordonnées géographiques
                    if (jsonNode.has("coordinates")) {
                        JsonNode coordNode = jsonNode.get("coordinates");
                        if (coordNode != null) {
                            Double latitude = getDoubleValue(coordNode, "latitude");
                            Double longitude = getDoubleValue(coordNode, "longitude");

                            shop.setLatitude(latitude);
                            shop.setLongitude(longitude);

                            // Format correct pour un GeoPoint dans Elasticsearch
                            if (latitude != null && longitude != null) {
                                String geoPoint = latitude + "," + longitude;
                                Map<String, Double> coordinates = new HashMap<>();
                                coordinates.put("lat", latitude);
                                coordinates.put("lon", longitude);
                                shop.setCoordinates(coordinates);
                            }
                        }
                    }

                    if (jsonNode.has("detailed_address")) {
                        JsonNode addrNode = jsonNode.get("detailed_address");
                        if (addrNode != null) {
                            shop.setWard(getTextValue(addrNode, "ward"));
                            shop.setStreet(getTextValue(addrNode, "street"));
                            shop.setCity(getTextValue(addrNode, "city"));
                            shop.setPostalCode(getTextValue(addrNode, "postal_code"));
                            shop.setState(getTextValue(addrNode, "state"));
                            shop.setCountryCode(getTextValue(addrNode, "country_code"));
                        }
                    }

                    // Propriétés de contact et site web
                    shop.setWebsite(getTextValue(jsonNode, "website"));
                    shop.setPhone(getTextValue(jsonNode, "phone"));
                    shop.setCanClaim(getBooleanValue(jsonNode, "can_claim"));

                    // Propriétaire
                    if (jsonNode.has("owner")) {
                        JsonNode ownerNode = jsonNode.get("owner");
                        if (ownerNode != null) {
                            shop.setOwnerId(getTextValue(ownerNode, "id"));
                            shop.setOwnerName(getTextValue(ownerNode, "name"));
                            shop.setOwnerLink(getTextValue(ownerNode, "link"));
                        }
                    }

                    // Images
                    shop.setFeaturedImage(getTextValue(jsonNode, "featured_image"));

                    // Catégories
                    shop.setMainCategory(getTextValue(jsonNode, "main_category"));
                    if (jsonNode.has("categories") && jsonNode.get("categories").isArray()) {
                        List<String> categories = new ArrayList<>();
                        for (JsonNode cat : jsonNode.get("categories")) {
                            categories.add(cat.asText());
                        }
                        shop.setCategories(categories);
                    }

                    // Horaires
                    shop.setWorkdayTiming(getTextValue(jsonNode, "workday_timing"));
                    shop.setIsTemporarilyClosed(getBooleanValue(jsonNode, "is_temporarily_closed"));
                    shop.setIsPermanentlyClosed(getBooleanValue(jsonNode, "is_permanently_closed"));

                    if (jsonNode.has("closed_on") && jsonNode.get("closed_on").isArray()) {
                        List<String> closedOn = new ArrayList<>();
                        for (JsonNode day : jsonNode.get("closed_on")) {
                            closedOn.add(day.asText());
                        }
                        shop.setClosedOn(closedOn);
                    }

                    // Autres champs
                    shop.setLink(getTextValue(jsonNode, "link"));
                    shop.setStatus(getTextValue(jsonNode, "status"));
                    shop.setPriceRange(getTextValue(jsonNode, "price_range"));
                    shop.setPlusCode(getTextValue(jsonNode, "plus_code"));
                    shop.setTimeZone(getTextValue(jsonNode, "time_zone"));
                    shop.setCid(getTextValue(jsonNode, "cid"));
                    shop.setDataId(getTextValue(jsonNode, "data_id"));
                    shop.setQuery(getTextValue(jsonNode, "query"));

                    // Conversion des objets complexes en Map pour les stocker dans la classe unique
                    if (jsonNode.has("reviews_per_rating")) {
                        Map<String, Integer> reviewsPerRating = new HashMap<>();
                        JsonNode ratingsNode = jsonNode.get("reviews_per_rating");
                        ratingsNode.fields().forEachRemaining(entry -> {
                            reviewsPerRating.put(entry.getKey(), entry.getValue().asInt());
                        });
                        shop.setReviewsPerRating(reviewsPerRating);
                    }

                    // Conversion des competitors en liste de Map
                    if (jsonNode.has("competitors") && jsonNode.get("competitors").isArray()) {
                        List<Map<String, Object>> competitors = new ArrayList<>();
                        for (JsonNode comp : jsonNode.get("competitors")) {
                            Map<String, Object> competitor = new HashMap<>();
                            competitor.put("name", getTextValue(comp, "name"));
                            competitor.put("link", getTextValue(comp, "link"));
                            competitor.put("reviews", getIntValue(comp, "reviews"));
                            competitor.put("rating", getFloatValue(comp, "rating"));
                            competitor.put("mainCategory", getTextValue(comp, "main_category"));
                            competitors.add(competitor);
                        }
                        shop.setCompetitors(competitors);
                    }

                    // Conversion des review_keywords en liste de Map
                    if (jsonNode.has("review_keywords") && jsonNode.get("review_keywords").isArray()) {
                        List<Map<String, Object>> reviewKeywords = new ArrayList<>();
                        for (JsonNode kw : jsonNode.get("review_keywords")) {
                            Map<String, Object> keyword = new HashMap<>();
                            keyword.put("keyword", getTextValue(kw, "keyword"));
                            keyword.put("count", getIntValue(kw, "count"));
                            reviewKeywords.add(keyword);
                        }
                        shop.setReviewKeywords(reviewKeywords);
                    }

                    // Conversion des about en liste de Map
                    if (jsonNode.has("about") && jsonNode.get("about").isArray()) {
                        List<Map<String, Object>> aboutList = new ArrayList<>();
                        for (JsonNode aboutNode : jsonNode.get("about")) {
                            Map<String, Object> about = new HashMap<>();
                            about.put("id", getTextValue(aboutNode, "id"));
                            about.put("name", getTextValue(aboutNode, "name"));

                            if (aboutNode.has("options") && aboutNode.get("options").isArray()) {
                                List<Map<String, Object>> options = new ArrayList<>();
                                for (JsonNode opt : aboutNode.get("options")) {
                                    Map<String, Object> option = new HashMap<>();
                                    option.put("name", getTextValue(opt, "name"));
                                    option.put("enabled", getBooleanValue(opt, "enabled"));
                                    options.add(option);
                                }
                                about.put("options", options);
                            }

                            aboutList.add(about);
                        }
                        shop.setAbout(aboutList);
                    }

                    // Conversion des images en liste de Map
                    if (jsonNode.has("images") && jsonNode.get("images").isArray()) {
                        List<Map<String, String>> imagesList = new ArrayList<>();
                        for (JsonNode imgNode : jsonNode.get("images")) {
                            Map<String, String> image = new HashMap<>();
                            image.put("about", getTextValue(imgNode, "about"));
                            image.put("link", getTextValue(imgNode, "link"));
                            imagesList.add(image);
                        }
                        shop.setImages(imagesList);
                    }

                    // Conversion des hours en liste de Map
                    if (jsonNode.has("hours") && jsonNode.get("hours").isArray()) {
                        List<Map<String, Object>> hoursList = new ArrayList<>();
                        for (JsonNode hourNode : jsonNode.get("hours")) {
                            Map<String, Object> hour = new HashMap<>();
                            hour.put("day", getTextValue(hourNode, "day"));

                            if (hourNode.has("times") && hourNode.get("times").isArray()) {
                                List<String> times = new ArrayList<>();
                                for (JsonNode time : hourNode.get("times")) {
                                    times.add(time.asText());
                                }
                                hour.put("times", times);
                            }

                            hoursList.add(hour);
                        }
                        shop.setHours(hoursList);
                    }

                    // Conversion des featured_reviews en liste de Map
                    if (jsonNode.has("featured_reviews") && jsonNode.get("featured_reviews").isArray()) {
                        List<Map<String, Object>> reviewsList = new ArrayList<>();
                        for (JsonNode reviewNode : jsonNode.get("featured_reviews")) {
                            Map<String, Object> review = new HashMap<>();

                            // Propriétés de base de la review
                            review.put("reviewId", getTextValue(reviewNode, "review_id"));
                            review.put("reviewLink", getTextValue(reviewNode, "review_link"));
                            review.put("name", getTextValue(reviewNode, "name"));
                            review.put("reviewerId", getTextValue(reviewNode, "reviewer_id"));
                            review.put("reviewerProfile", getTextValue(reviewNode, "reviewer_profile"));
                            review.put("rating", getIntValue(reviewNode, "rating"));
                            review.put("reviewText", getTextValue(reviewNode, "review_text"));
                            review.put("publishedAt", getTextValue(reviewNode, "published_at"));
                            review.put("responseFromOwnerText", getTextValue(reviewNode, "response_from_owner_text"));
                            review.put("responseFromOwnerAgo", getTextValue(reviewNode, "response_from_owner_ago"));
                            review.put("isLocalGuide", getBooleanValue(reviewNode, "is_local_guide"));
                            review.put("reviewTranslatedText", getTextValue(reviewNode, "review_translated_text"));
                            review.put("responseFromOwnerTranslatedText", getTextValue(reviewNode, "response_from_owner_translated_text"));

                            // Photos
                            if (reviewNode.has("review_photos") && reviewNode.get("review_photos").isArray()) {
                                List<Map<String, Object>> photosList = new ArrayList<>();
                                for (JsonNode photoNode : reviewNode.get("review_photos")) {
                                    Map<String, Object> photo = new HashMap<>();
                                    photo.put("id", getTextValue(photoNode, "id"));
                                    photo.put("url", getTextValue(photoNode, "url"));
                                    photo.put("caption", getTextValue(photoNode, "caption"));
                                    photo.put("width", getIntValue(photoNode, "width"));
                                    photo.put("height", getIntValue(photoNode, "height"));
                                    photosList.add(photo);
                                }
                                review.put("reviewPhotos", photosList);
                            }

                            reviewsList.add(review);
                        }
                        shop.setFeaturedReviews(reviewsList);
                    }

                    // Conversion des detailed_reviews
                    if (jsonNode.has("detailed_reviews") && jsonNode.get("detailed_reviews").isArray()) {
                        List<Map<String, Object>> detailedReviewsList = new ArrayList<>();
                        for (JsonNode reviewNode : jsonNode.get("detailed_reviews")) {
                            // Similaire à featured_reviews, mais à laisser vide pour simplifier
                            Map<String, Object> review = new HashMap<>();
                            // (même logique que pour featured_reviews)
                            detailedReviewsList.add(review);
                        }
                        shop.setDetailedReviews(detailedReviewsList);
                    }

                    // Ajout à la liste des boutiques
                    shops.add(shop);
                    logger.debug("Ligne {} parsée : {}", lineCount, shop.getName());

                } catch (Exception e) {
                    logger.error("Erreur lors du parsing de la ligne {} dans {}: {}",
                            lineCount, filePath, e.getMessage());
                }
            }

        } catch (IOException e) {
            logger.error("Erreur lors de la lecture du fichier {} : {}", filePath, e.getMessage());
        }

        return shops;
    }

    /**
     * Force la réimportation de toutes les données, même si l'index n'est pas vide
     */
    public void forceReimport() {
        logger.info("Réimportation forcée de toutes les données...");
        b2BService.deleteIndex();
        b2BService.createIndex();
        importDataFromDirectory(dataDirectory);
    }

    // Méthodes utilitaires pour extraire les valeurs du JSON
    private String getTextValue(JsonNode node, String fieldName) {
        if (node.has(fieldName) && !node.get(fieldName).isNull()) {
            return node.get(fieldName).asText();
        }
        return null;
    }

    private Integer getIntValue(JsonNode node, String fieldName) {
        if (node.has(fieldName) && !node.get(fieldName).isNull()) {
            return node.get(fieldName).asInt();
        }
        return null;
    }

    private Float getFloatValue(JsonNode node, String fieldName) {
        if (node.has(fieldName) && !node.get(fieldName).isNull()) {
            return (float) node.get(fieldName).asDouble();
        }
        return null;
    }

    private Double getDoubleValue(JsonNode node, String fieldName) {
        if (node.has(fieldName) && !node.get(fieldName).isNull()) {
            return node.get(fieldName).asDouble();
        }
        return null;
    }

    private Boolean getBooleanValue(JsonNode node, String fieldName) {
        if (node.has(fieldName) && !node.get(fieldName).isNull()) {
            return node.get(fieldName).asBoolean();
        }
        return null;
    }
}