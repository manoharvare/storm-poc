package com.yogesh.example2.model;

import lombok.*;

@Data
@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class CustomerInfo {
    private String firstName;
    private String middleName;
    private String lastName;
    private String gender;
    private String dob;
    private String mobileNumber;
    private String city;
    private String state;
    private String country;
    private String pinCode;
}
