package com.yogesh.util;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class StormUtil {

    private static List<String> list;
    private static Random random = new Random();

    public static String generateState() {
        list = Arrays.asList(StormDataConstants.state);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateCity() {
        list = Arrays.asList(StormDataConstants.city);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateCountry() {
        list = Arrays.asList(StormDataConstants.country);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateGender() {
        list = Arrays.asList(StormDataConstants.gender);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateFirstName() {
        list = Arrays.asList(StormDataConstants.firstName);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateMiddleName() {
        list = Arrays.asList(StormDataConstants.middleName);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateLastName() {
        list = Arrays.asList(StormDataConstants.lastName);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateDob() {
        list = Arrays.asList(StormDataConstants.dob);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateMobileNumber() {
        list = Arrays.asList(StormDataConstants.mobileNumber);
        return list.get(random.nextInt(list.size()));
    }

    public static String generatePinCode() {
        list = Arrays.asList(StormDataConstants.pinCode);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateUniqueId() {
        return UUID.randomUUID().toString();
    }

    public static String generateIsMarried() {
        list = Arrays.asList(StormDataConstants.married);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateProcessedBy() {
        list = Arrays.asList(StormDataConstants.processedBy);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateProduct() {
        list = Arrays.asList(StormDataConstants.product);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateInstitutionId() {
        list = Arrays.asList(StormDataConstants.institutionId);
        return list.get(random.nextInt(list.size()));
    }

    public static String generateStatus() {
        list = Arrays.asList(StormDataConstants.status);
        return list.get(random.nextInt(list.size()));
    }

    public static int generateAge() {
        int min = 18;
        int max = 60;
        int randomNumber = random.nextInt(max - min) + min;
        if (randomNumber == min) {
            return min + 1;
        } else {
            return randomNumber;
        }
    }

    public static int generateNumber(int min, int max) {
        int randomNumber = random.nextInt(max - min) + min;
        if (randomNumber == min) {
            return min + 1;
        } else {
            return randomNumber;
        }
    }


}
