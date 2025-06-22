package com.example.springelasticproject.model.b2bModel;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.annotations.GeoPointField;

import java.util.List;
import java.util.Map;
//@Document(indexName = "repairshops")
@Document(indexName = "b2b")
public class B2B {

    @Id
    private String placeId;

    @Field(type = FieldType.Text, analyzer = "french")
    private String name;

    @Field(type = FieldType.Text, analyzer = "french")
    private String description;

    @Field(type = FieldType.Boolean)
    private Boolean isSpendingOnAds;

    @Field(type = FieldType.Integer)
    private Integer reviews;

    @Field(type = FieldType.Float)
    private Float rating;

    // Competitors - liste simplifiée
    @Field(type = FieldType.Object)
    private List<Map<String, Object>> competitors;

    @Field(type = FieldType.Text)
    private String website;

    @Field(type = FieldType.Text)
    private String phone;

    @Field(type = FieldType.Boolean)
    private Boolean canClaim;

    // Owner info
    @Field(type = FieldType.Text)
    private String ownerId;

    @Field(type = FieldType.Text)
    private String ownerName;

    @Field(type = FieldType.Text)
    private String ownerLink;

    @Field(type = FieldType.Text)
    private String featuredImage;

    @Field(type = FieldType.Text, analyzer = "french")
    private String mainCategory;

    @Field(type = FieldType.Text, analyzer = "french")
    private List<String> categories;

    @Field(type = FieldType.Text)
    private String workdayTiming;

    @Field(type = FieldType.Boolean)
    private Boolean isTemporarilyClosed;

    @Field(type = FieldType.Boolean)
    private Boolean isPermanentlyClosed;

    @Field(type = FieldType.Text)
    private List<String> closedOn;

    @Field(type = FieldType.Text, analyzer = "french")
    private String address;

    // Review keywords
    @Field(type = FieldType.Object)
    private List<Map<String, Object>> reviewKeywords;

    @Field(type = FieldType.Text)
    private String link;

    @Field(type = FieldType.Text)
    private String status;

    @Field(type = FieldType.Text)
    private String priceRange;

    @Field(type = FieldType.Object)
    private Map<String, Integer> reviewsPerRating;

    @Field(type = FieldType.Text)
    private String featuredQuestion;

    @Field(type = FieldType.Text)
    private String reviewsLink;

    // Coordinates
    @Field(type = FieldType.Double)
    private Double latitude;

    @Field(type = FieldType.Double)
    private Double longitude;

    @GeoPointField
    private Map<String, Double> coordinates;

    @Field(type = FieldType.Text)
    private String plusCode;

    // Address détaillée
    @Field(type = FieldType.Text)
    private String ward;

    @Field(type = FieldType.Text)
    private String street;

    @Field(type = FieldType.Text)
    private String city;

    @Field(type = FieldType.Text)
    private String postalCode;

    @Field(type = FieldType.Text)
    private String state;

    @Field(type = FieldType.Text)
    private String countryCode;

    @Field(type = FieldType.Text)
        private String timeZone;

    @Field(type = FieldType.Text)
    private String cid;

    @Field(type = FieldType.Text)
    private String dataId;

    // About section
    @Field(type = FieldType.Object)
    private List<Map<String, Object>> about;

    // Images
        @Field(type = FieldType.Object)
        private List<Map<String, String>> images;

    // Business hours
    @Field(type = FieldType.Object)
    private List<Map<String, Object>> hours;

    @Field(type = FieldType.Object)
    private Map<String, Object> popularTimes;

    @Field(type = FieldType.Object)
    private List<Map<String, Object>> mostPopularTimes;

    // Reviews
    @Field(type = FieldType.Object)
    private List<Map<String, Object>> featuredReviews;

    @Field(type = FieldType.Object)
    private List<Map<String, Object>> detailedReviews;

    @Field(type = FieldType.Text)
    private String query;

    @Field(type = FieldType.Integer)
    private Integer score;
    @Field(type = FieldType.Text)
    private String scoreCategory;

    // Getters et setters
    public String getPlaceId() {
        return placeId;
    }

    public void setPlaceId(String placeId) {
        this.placeId = placeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getIsSpendingOnAds() {
        return isSpendingOnAds;
    }

    public void setIsSpendingOnAds(Boolean isSpendingOnAds) {
        this.isSpendingOnAds = isSpendingOnAds;
    }

    public Integer getReviews() {
        return reviews;
    }

    public void setReviews(Integer reviews) {
        this.reviews = reviews;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }

    public List<Map<String, Object>> getCompetitors() {
        return competitors;
    }

    public void setCompetitors(List<Map<String, Object>> competitors) {
        this.competitors = competitors;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Boolean getCanClaim() {
        return canClaim;
    }

    public void setCanClaim(Boolean canClaim) {
        this.canClaim = canClaim;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    public String getOwnerLink() {
        return ownerLink;
    }

    public void setOwnerLink(String ownerLink) {
        this.ownerLink = ownerLink;
    }

    public String getFeaturedImage() {
        return featuredImage;
    }

    public void setFeaturedImage(String featuredImage) {
        this.featuredImage = featuredImage;
    }

    public String getMainCategory() {
        return mainCategory;
    }

    public void setMainCategory(String mainCategory) {
        this.mainCategory = mainCategory;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public String getWorkdayTiming() {
        return workdayTiming;
    }

    public void setWorkdayTiming(String workdayTiming) {
        this.workdayTiming = workdayTiming;
    }

    public Boolean getIsTemporarilyClosed() {
        return isTemporarilyClosed;
    }

    public void setIsTemporarilyClosed(Boolean isTemporarilyClosed) {
        this.isTemporarilyClosed = isTemporarilyClosed;
    }

    public Boolean getIsPermanentlyClosed() {
        return isPermanentlyClosed;
    }

    public void setIsPermanentlyClosed(Boolean isPermanentlyClosed) {
        this.isPermanentlyClosed = isPermanentlyClosed;
    }

    public List<String> getClosedOn() {
        return closedOn;
    }

    public void setClosedOn(List<String> closedOn) {
        this.closedOn = closedOn;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public List<Map<String, Object>> getReviewKeywords() {
        return reviewKeywords;
    }

    public void setReviewKeywords(List<Map<String, Object>> reviewKeywords) {
        this.reviewKeywords = reviewKeywords;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getPriceRange() {
        return priceRange;
    }

    public void setPriceRange(String priceRange) {
        this.priceRange = priceRange;
    }

    public Map<String, Integer> getReviewsPerRating() {
        return reviewsPerRating;
    }

    public void setReviewsPerRating(Map<String, Integer> reviewsPerRating) {
        this.reviewsPerRating = reviewsPerRating;
    }

    public String getFeaturedQuestion() {
        return featuredQuestion;
    }

    public void setFeaturedQuestion(String featuredQuestion) {
        this.featuredQuestion = featuredQuestion;
    }

    public String getReviewsLink() {
        return reviewsLink;
    }

    public void setReviewsLink(String reviewsLink) {
        this.reviewsLink = reviewsLink;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Map<String, Double> getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(Map<String, Double> coordinates) {
        this.coordinates = coordinates;
    }

    public String getPlusCode() {
        return plusCode;
    }

    public void setPlusCode(String plusCode) {
        this.plusCode = plusCode;
    }

    public String getWard() {
        return ward;
    }

    public void setWard(String ward) {
        this.ward = ward;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public List<Map<String, Object>> getAbout() {
        return about;
    }

    public void setAbout(List<Map<String, Object>> about) {
        this.about = about;
    }

    public List<Map<String, String>> getImages() {
        return images;
    }

    public void setImages(List<Map<String, String>> images) {
        this.images = images;
    }

    public List<Map<String, Object>> getHours() {
        return hours;
    }

    public void setHours(List<Map<String, Object>> hours) {
        this.hours = hours;
    }

    public Map<String, Object> getPopularTimes() {
        return popularTimes;
    }

    public void setPopularTimes(Map<String, Object> popularTimes) {
        this.popularTimes = popularTimes;
    }

    public List<Map<String, Object>> getMostPopularTimes() {
        return mostPopularTimes;
    }

    public void setMostPopularTimes(List<Map<String, Object>> mostPopularTimes) {
        this.mostPopularTimes = mostPopularTimes;
    }

    public List<Map<String, Object>> getFeaturedReviews() {
        return featuredReviews;
    }

    public void setFeaturedReviews(List<Map<String, Object>> featuredReviews) {
        this.featuredReviews = featuredReviews;
    }

    public List<Map<String, Object>> getDetailedReviews() {
        return detailedReviews;
    }

    public void setDetailedReviews(List<Map<String, Object>> detailedReviews) {
        this.detailedReviews = detailedReviews;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }
    public String getScoreCategory() {
        return scoreCategory;
    }
    public void setScoreCategory(String scoreCategory) {
        this.scoreCategory = scoreCategory;
    }
}