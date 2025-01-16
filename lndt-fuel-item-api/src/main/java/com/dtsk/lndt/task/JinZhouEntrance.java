//package com.dtsk.lndt.task;
//
//import cn.hutool.json.JSONUtil;
//import com.dtsz.frdata.entity.LaboratoryTestEntrance;
//import dtxy.sdxm.interfaceservice.desutil.DesUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.http.HttpEntity;
//import org.springframework.http.HttpHeaders;
//import org.springframework.http.MediaType;
//import org.springframework.http.ResponseEntity;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//import org.springframework.web.client.RestTemplate;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * 燃料三大项: 锦州入厂质量验收综合台账
// */
//@Component
//@Slf4j
//public class JinZhouEntrance {
//    /**
//     * 定时任务,每天凌晨1点执行
//     */
//    @Scheduled(cron = "0 0 1 * * ?", zone = "Asia/Shanghai")
//    public static void myTask() throws SQLException {
//        String url = "jdbc:mysql://10.217.6.103:9030/sfrl?useUnicode=true&characterEncoding=utf8&useTimezone=true&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true";
//        String user = "root";
//        String password = "DT@lnfgs#2024";
//
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//        //今天凌晨时间
//        LocalDateTime todayMidnight = LocalDate.now().atStartOfDay();
//        String todayMidnightStr = todayMidnight.format(formatter);
//
//        //昨天凌晨时间
//        LocalDateTime yesterdayMidnight = LocalDate.now().minusDays(1).atStartOfDay();
//        String yesterdayMidnightStr = yesterdayMidnight.format(formatter);
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.APPLICATION_JSON);
//        RestTemplate restTemplate = new RestTemplate();
//
//        Map paramMap = new HashMap<>();
//        paramMap.put("startTime", yesterdayMidnightStr);
//        paramMap.put("endTime", todayMidnightStr);
//
//        String param = JSONUtil.toJsonStr(paramMap);
//
//        HttpEntity<String> stringHttpEntity = new HttpEntity<>(param, headers);
//        ResponseEntity<String> stringResponseEntity = restTemplate.postForEntity("http://10.66.74.225/DTRLEsc/api/DTRLLaboratory/getLaboratoryTestEntrance", stringHttpEntity, String.class);
//        if (stringResponseEntity.getStatusCode().value() == 200) {
//            String body = stringResponseEntity.getBody();
//            String des = null;
//            try {
//                //数据解密
//                des = DesUtil.des(body);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            des = JSONUtil.parse(des).getByPath("result").toString();
//            log.info(des);
//            List<LaboratoryTestEntrance> results = JSONUtil.toList(des, LaboratoryTestEntrance.class);
//            Connection connection = DriverManager.getConnection(url, user, password);
//            String sql = "INSERT INTO jinzhou_entrance_qa (cm,content) VALUES(?,?)";
//            PreparedStatement preparedStatement = connection.prepareStatement(sql);
//
//            for (LaboratoryTestEntrance laboratoryTestEntrance : results) {
//                preparedStatement.setString(1, yesterdayMidnightStr.substring(0, 10));
//                preparedStatement.setString(2, JSONUtil.toJsonStr(laboratoryTestEntrance));
//                preparedStatement.executeUpdate();
//            }
//            connection.close();
//            preparedStatement.close();
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        myTask();
//    }
//}
