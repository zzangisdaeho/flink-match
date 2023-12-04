package co.pgs.match.api.flatmap;

import co.pgs.match.dto.MatchRequest;
import co.pgs.match.dto.UserInfo;
import co.pgs.match.dto.UserInfoEnriched;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Set;

public class MatchFunction extends RichFlatMapFunction<UserInfoEnriched, String> {

    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<String, String> connection;
    private transient RedisCommands<String, String> syncCommands;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        // Lettuce Redis 연결 설정
        redisClient = RedisClient.create("redis://localhost:6379");
        connection = redisClient.connect();
        syncCommands = connection.sync();
        objectMapper = new ObjectMapper();
    }

    @Override
    public void flatMap(UserInfoEnriched value, Collector<String> out) throws Exception {
        UserInfo currentUserInfo = value.getUserInfo();
        MatchRequest currentRequest = value.getMatchRequest();

        // Sorted Set에서 전체 대기열의 userId를 조회
        List<String> waitingSet = syncCommands.zrange("waitingSet", 0, -1);

        boolean matched = false;
        for (String userId : waitingSet) {
            String waitingUserJson = syncCommands.hget("userInfoMap", userId);
            UserInfoEnriched waitingUser = objectMapper.readValue(waitingUserJson, UserInfoEnriched.class);

            if(waitingUser.getMatchRequest().getUserId() == value.getMatchRequest().getUserId()) continue;

            if (isMatch(currentRequest, currentUserInfo, waitingUser.getMatchRequest(), waitingUser.getUserInfo())) {
                out.collect("Matched: " + currentRequest.getUserId() + " and " + waitingUser.getMatchRequest().getUserId());
                removeFromWaitingSet(userId);
                removeFromUserInfoMap(userId);
                removeFromWaitingSet(String.valueOf(value.getMatchRequest().getUserId()));
                removeFromUserInfoMap(String.valueOf(value.getMatchRequest().getUserId()));
                matched = true;
                break;
            }
        }

        if (!matched) {
            addToWaitingSetAndUserInfoMap(String.valueOf(currentRequest.getUserId()), value);
        }
    }

    private boolean isMatch(MatchRequest currentRequest, UserInfo currentUserInfo, MatchRequest waitingRequest, UserInfo waitingUserInfo) {
        // 양쪽 사용자의 선호도를 고려한 매칭 로직
        return (currentRequest.getPreferredGender() == null || currentRequest.getPreferredGender() == waitingUserInfo.getGender()) &&
                (currentRequest.getPreferredCountry() == null || currentRequest.getPreferredCountry() == waitingUserInfo.getCountry()) &&
                (waitingRequest.getPreferredGender() == null || waitingRequest.getPreferredGender() == currentUserInfo.getGender()) &&
                (waitingRequest.getPreferredCountry() == null || waitingRequest.getPreferredCountry() == currentUserInfo.getCountry());
    }

    private void addToWaitingSetAndUserInfoMap(String userId, UserInfoEnriched user) throws JsonProcessingException {
        String json = objectMapper.writeValueAsString(user);
        long score = System.currentTimeMillis();
        syncCommands.zadd("waitingSet", score, userId);
        syncCommands.hset("userInfoMap", userId, json);
    }

    private void removeFromWaitingSet(String userId) {
        syncCommands.zrem("waitingSet", userId);
    }

    private void removeFromUserInfoMap(String userId) {
        syncCommands.hdel("userInfoMap", userId);
    }

    @Override
    public void close() throws Exception {
        connection.close();
        redisClient.shutdown();
    }

}