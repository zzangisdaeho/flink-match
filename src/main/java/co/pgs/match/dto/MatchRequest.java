package co.pgs.match.dto;

import lombok.Data;

import java.util.Arrays;

@Data
public class MatchRequest {

    private Country preferredCountry;

    private Gender preferredGender;

    private long userId;

    public enum Country{
        KOREA, JAPAN, USA, UK, CHINA;

        public static Country fromString(String countryStr) {
            return Arrays.stream(Country.values())
                    .filter(country -> country.name().equalsIgnoreCase(countryStr))
                    .findFirst()
                    .orElse(null); // 없는 경우 null 반환, 또는 orElseThrow()로 예외 처리 가능
        }
    }

    public enum Gender{
        MALE, FEMALE;

        public static Gender fromString(String genderStr) {
            return Arrays.stream(Gender.values())
                    .filter(gender -> gender.name().equalsIgnoreCase(genderStr))
                    .findFirst()
                    .orElse(null); // 없는 경우 null 반환, 또는 orElseThrow()로 예외 처리 가능
        }
    }
}

