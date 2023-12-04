package co.pgs.match.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserInfoEnriched {
    private MatchRequest matchRequest;
    private UserInfo userInfo;

}