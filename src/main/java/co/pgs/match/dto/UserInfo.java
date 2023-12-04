package co.pgs.match.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class UserInfo {

    public UserInfo(String gender, String country) {
        this.gender = MatchRequest.Gender.fromString(gender);
        this.country = MatchRequest.Country.fromString(country);
    }

    private MatchRequest.Gender gender;
    private MatchRequest.Country country;


}
