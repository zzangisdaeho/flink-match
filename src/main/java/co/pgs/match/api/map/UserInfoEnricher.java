package co.pgs.match.api.map;

import co.pgs.match.dto.MatchRequest;
import co.pgs.match.dto.UserInfo;
import co.pgs.match.dto.UserInfoEnriched;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class UserInfoEnricher extends RichMapFunction<MatchRequest, UserInfoEnriched> {

    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/matching";
    private static final String USER = "root";
    private static final String PASSWORD = "1234";

    private transient ValueState<UserInfo> userInfoState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<UserInfo> descriptor =
                new ValueStateDescriptor<>(
                        "userInfo", // 상태 이름
                        TypeInformation.of(new TypeHint<UserInfo>() {})); // 상태 타입 정보
        userInfoState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public UserInfoEnriched map(MatchRequest value) throws Exception {

        UserInfo userInfo = userInfoState.value();

        if (userInfo == null) {
            // DB에서 사용자 정보 조회
            userInfo = queryUserInfoFromDB(value.getUserId());
            // 상태에 사용자 정보 저장
            userInfoState.update(userInfo);
        }

        // ... DB 조회 및 UserInfo 채우기 ...
        return new UserInfoEnriched(value, userInfo);
    }

    private UserInfo queryUserInfoFromDB(long userId) {
        UserInfo userInfo = null;

        try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            String query = "SELECT gender, location FROM user_entity WHERE user_no = ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setLong(1, userId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        String gender = resultSet.getString("gender");
                        String location = resultSet.getString("location");
                        userInfo = new UserInfo(gender, location);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace(); // 실제 애플리케이션에서는 적절한 예외 처리가 필요
        }

        return userInfo;
    }
}
