package com.example.springelasticproject.model.b2cModel;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;



@Document(indexName = "users", createIndex = true)
public class B2C {

    @Id
    @Field(type = FieldType.Long)
    private Long idS;

    @Field(type = FieldType.Long)
    private Long userId;

    @Field(type = FieldType.Text)
    private String phoneNumber;

    @Field(type = FieldType.Text)
    private String firstName;

    @Field(type = FieldType.Text)
    private String lastName;

    @Field(type = FieldType.Text)
    private String gender;

    @Field(type = FieldType.Text)
    private String currentCity;

    @Field(type = FieldType.Text)
    private String currentCountry;

    @Field(type = FieldType.Text)
    private String hometownCity;

    @Field(type = FieldType.Text)
    private String hometownCountry;

    @Field(type = FieldType.Text)
    private String relationshipStatus;

    @Field(type = FieldType.Text)
    private String workplace;

    @Field(type = FieldType.Text)
    private String email;

    @Field(type = FieldType.Text)
    private String currentDepartment;
    @Field(type = FieldType.Text)
    private String currentRegion;


    public B2C() {
    }

    public B2C(Long idS, Long userId, String phoneNumber, String firstName, String lastName, String gender, String currentCity, String currentCountry, String hometownCity, String hometownCountry, String relationshipStatus, String workplace, String email, String currentDepartment, String currentRegion) {
        this.idS = idS;
        this.userId = userId;
        this.phoneNumber = phoneNumber;
        this.firstName = firstName;
        this.lastName = lastName;
        this.gender = gender;
        this.currentCity = currentCity;
        this.currentCountry = currentCountry;
        this.hometownCity = hometownCity;
        this.hometownCountry = hometownCountry;
        this.relationshipStatus = relationshipStatus;
        this.workplace = workplace;
        this.email = email;
        this.currentDepartment = currentDepartment;
        this.currentRegion = currentRegion;
    }

    public Long getIdS() {
        return idS;
    }

    public void setIdS(Long idS) {
        this.idS = idS;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getCurrentCity() {
        return currentCity;
    }

    public void setCurrentCity(String currentCity) {
        this.currentCity = currentCity;
    }

    public String getCurrentCountry() {
        return currentCountry;
    }

    public void setCurrentCountry(String currentCountry) {
        this.currentCountry = currentCountry;
    }

    public String getHometownCity() {
        return hometownCity;
    }

    public void setHometownCity(String hometownCity) {
        this.hometownCity = hometownCity;
    }

    public String getHometownCountry() {
        return hometownCountry;
    }

    public void setHometownCountry(String hometownCountry) {
        this.hometownCountry = hometownCountry;
    }

    public String getRelationshipStatus() {
        return relationshipStatus;
    }

    public void setRelationshipStatus(String relationshipStatus) {
        this.relationshipStatus = relationshipStatus;
    }

    public String getWorkplace() {
        return workplace;
    }

    public void setWorkplace(String workplace) {
        this.workplace = workplace;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getCurrentDepartment() {
        return currentDepartment;
    }

    public void setCurrentDepartment(String currentDepartment) {
        this.currentDepartment = currentDepartment;
    }

    public String getCurrentRegion() {
        return currentRegion;
    }

    public void setCurrentRegion(String currentRegion) {
        this.currentRegion = currentRegion;
    }
}
