//package com.dtsk.lndt.task;
//
//import cn.hutool.json.JSONUtil;
//import com.dtsz.frdata.entity.LaboratoryTestFurnace;
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
//import java.util.List;
//
///**
// * 燃料三大项: 锦州入炉质量验收综合台账
// */
//@Component
//@Slf4j
//public class JinZhouFurnace {
//    /**
//     * 定时任务,30秒执行
//     */
//    @Scheduled(fixedRate = 60000*5)
//    public static void myTask(){
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.APPLICATION_JSON);
//
//        RestTemplate restTemplate = new RestTemplate();
//        String param = "{\"startTime\":\"2024-11-19 00:00:00\",\"endTime\":\"2024-11-20 00:00:00\"}";
//        HttpEntity<String> stringHttpEntity = new HttpEntity<>(param, headers);
//        ResponseEntity<String> stringResponseEntity = restTemplate.postForEntity("http://10.66.74.225/DTRLEsc/api/DTRLLaboratory/getLaboratoryTestFurnace", stringHttpEntity, String.class);
//        if(stringResponseEntity.getStatusCode().value() == 200){
//            String body = stringResponseEntity.getBody();
//            String des1 = null;
//            try {
//                des1 = DesUtil.des(body);
//                log.info("========================{}", des1);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            des1 = JSONUtil.parse(des1).getByPath("result").toString();
//            List<LaboratoryTestFurnace> list1 = JSONUtil.toList(des1, LaboratoryTestFurnace.class);
//            log.info("-------------------------------{}", JSONUtil.parse(list1).toString());
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        myTask();
//    }
//}
