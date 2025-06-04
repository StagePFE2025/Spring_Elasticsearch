package com.example.springelasticproject.Services.b2cService;




import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class StatisticsB2CService {

    @Autowired
    private B2CService b2CService;

    /**
     * Obtient des statistiques pour un attribut spécifique avec des valeurs prédéfinies
     * @param attribute Le nom de l'attribut (ex: "gender", "currentRegion")
     * @param values Liste des valeurs possibles pour cet attribut
     * @return Map avec les valeurs comme clés et les nombres comme valeurs
     */
    public Map<String, Long> getStatisticsByAttribute(String attribute, List<String> values) {
        Map<String, Long> statistics = new LinkedHashMap<>();
        Pageable pageable = PageRequest.of(0, 1, Sort.by(Sort.Direction.DESC, "_score"));

        for (String value : values) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(attribute, value);

            Long count = b2CService.searchByAttributes04NoFusNumb(attributes, pageable);
            statistics.put(value, count);
        }

        return statistics;
    }

    /**
     * Obtient des statistiques pour les départements français les plus populeux
     * @param limit Nombre de départements à inclure
     * @return Map avec les départements comme clés et les nombres comme valeurs
     */
    public Map<String, Long> getDepartmentStatistics(int limit) {
        // Liste des départements français (peut être étendue)
        List<String> departments = Arrays.asList(
                "Paris", "Hauts-de-Seine", "Seine-Saint-Denis", "Val-de-Marne",
                "Seine-et-Marne", "Yvelines", "Essonne", "Val-d'Oise",
                "Rhône", "Bouches-du-Rhône", "Nord", "Gironde", "Haute-Garonne",
                "Alpes-Maritimes", "Loire-Atlantique", "Bas-Rhin", "Hérault",
                "Ille-et-Vilaine", "Marne", "Loire", "Var", "Seine-Maritime",
                "Isère", "Côte-d'Or", "Maine-et-Loire", "Gard", "Vienne",
                "Puy-de-Dôme", "Sarthe", "Vaucluse", "Finistère", "Indre-et-Loire",
                "Somme", "Haute-Vienne", "Savoie", "Pyrénées-Orientales", "Moselle",
                "Doubs", "Loiret", "Calvados", "Haut-Rhin", "Eure"
        );

        // Limite la liste si nécessaire
        if (limit > 0 && limit < departments.size()) {
            departments = departments.subList(0, limit);
        }

        return getStatisticsByAttribute("currentDepartment", departments);
    }

    /**
     * Obtient des statistiques pour les régions françaises
     * @return Map avec les régions comme clés et les nombres comme valeurs
     */
    public Map<String, Long> getRegionStatistics() {
        List<String> regions = Arrays.asList(
                "Île-de-France", "Auvergne-Rhône-Alpes", "Hauts-de-France",
                "Nouvelle-Aquitaine", "Occitanie", "Grand Est", "Provence-Alpes-Côte d'Azur",
                "Pays de la Loire", "Normandie", "Bretagne", "Bourgogne-Franche-Comté",
                "Centre-Val de Loire", "Corse"
        );

        return getStatisticsByAttribute("currentRegion", regions);
    }

    /**
     * Obtient des statistiques pour les statuts relationnels
     * @return Map avec les statuts comme clés et les nombres comme valeurs
     */
    public Map<String, Long> getRelationshipStatusStatistics() {
        List<String> statuses = Arrays.asList(
                "Single", "In a relationship", "Engaged", "Married",
                "Separated"
        );

        return getStatisticsByAttribute("relationshipStatus", statuses);
    }

    /**
     * Obtient des statistiques pour les genres
     * @return Map avec les genres comme clés et les nombres comme valeurs
     */
    public Map<String, Long> getGenderStatistics() {
        List<String> genders = Arrays.asList("Male", "Female");

        return getStatisticsByAttribute("gender", genders);
    }

    /**
     * Obtient des statistiques pour les villes les plus peuplées
     * @param limit Nombre de villes à inclure
     * @return Map avec les villes comme clés et les nombres comme valeurs
     */
    public Map<String, Long> getCityStatistics(int limit) {
        // Liste des grandes villes françaises (peut être étendue)
        List<String> cities = Arrays.asList(
                "Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Montpellier", "Strasbourg", "Bordeaux", "Lille",
                "Rennes", "Toulon", "Reims", "Saint-Étienne", "Le Havre", "Villeurbanne", "Dijon", "Angers", "Grenoble", "Saint-Denis",
                "Nîmes", "Aix-en-Provence", "Clermont-Ferrand", "Le Mans", "Brest", "Tours", "Amiens", "Annecy", "Limoges", "Metz",
                "Perpignan", "Boulogne-Billancourt", "Besançon", "Orléans", "Rouen", "Saint-Denis", "Montreuil", "Caen", "Argenteuil",
                "Saint-Paul", "Mulhouse", "Nancy", "Roubaix", "Tourcoing", "Nanterre", "Vitry-sur-Seine", "Créteil", "Avignon",
                "Asnières-sur-Seine", "Colombes", "Aubervilliers", "Poitiers", "Dunkerque", "Aulnay-sous-Bois", "Saint-Pierre",
                "Versailles", "Le Tampon", "Courbevoie", "Rueil-Malmaison", "Béziers", "La Rochelle", "Pau", "Champigny-sur-Marne",
                "Cherbourg-en-Cotentin", "Mérignac", "Antibes", "Saint-Maur-des-Fossés", "Ajaccio", "Fort-de-France", "Cannes",
                "Saint-Nazaire", "Noisy-le-Grand", "Mamoudzou", "Drancy", "Cergy", "Levallois-Perret", "Issy-les-Moulineaux",
                "Calais", "Colmar", "Pessac", "Vénissieux", "Évry-Courcouronnes", "Clichy", "Quimper", "Ivry-sur-Seine", "Valence",
                "Bourges", "Antony", "Cayenne", "La Seyne-sur-Mer", "Montauban", "Troyes", "Villeneuve-d'Ascq", "Pantin", "Chambéry",
                "Niort", "Le Blanc-Mesnil", "Neuilly-sur-Seine", "Sarcelles", "Fréjus"
        );

        // Limite la liste si nécessaire
        if (limit > 0 && limit < cities.size()) {
            cities = cities.subList(0, limit);
        }

        return getStatisticsByAttribute("currentCity", cities);
    }
}