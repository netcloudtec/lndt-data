package com.dtsk.lndt;

import cn.hutool.json.JSONUtil;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.ByteOrder;
import java.util.*;


public class UDPReceiver {
    public static void main(String[] args) {
            // Kafka 配置参数
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.217.6.104:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            KafkaProducer<String, Object> producer = new KafkaProducer<>(props);
            String bindIP = "10.217.6.104";
            int port = 2006;
            DatagramSocket socket = null;

            InetSocketAddress address = new InetSocketAddress(bindIP, port);

            try {
                socket = new DatagramSocket(address);
                while (true) {
                    byte[] buffer = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    StringBuilder hexString = new StringBuilder();
                    for (byte b : packet.getData()) {
                        hexString.append(String.format("%02X", b));
                    }
                    //获取当前时间
                    LocalDateTime now = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String currentTime = now.format(formatter);
                    String dealString = hexString.toString().substring(20);

                    // 将16进制字符串转换为字节数组
                    byte[] data = hexStringToByteArray(dealString);
                    // 定义解析格式：I (4 bytes), f (4 bytes), I (4 bytes), B (1 byte)
                    int recordLength = 4 + 4 + 4 + 1;

                    // 确保数据长度是记录长度的整数倍
                    if (data.length % recordLength != 0) {
                        throw new IllegalArgumentException("数据长度与记录格式不匹配！");
                    }

                    // 解析所有记录
                    ByteBuffer buffer2 = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN); // 小端字节序
                    for (int i = 0; i < data.length; i += recordLength) {
                        int idValue = buffer2.getInt();
                        float value = buffer2.getFloat();
                        int timestamp = buffer2.getInt();
                        byte byteValue = buffer2.get();
                        int status = Byte.toUnsignedInt(byteValue);
                        // 将时间戳转换为日期时间字符串
                        String formattedDateTime = formatTimestamp(timestamp);
                        Map retMap = new HashMap<>();
                        retMap.put("collect_time", currentTime);
                        retMap.put("id", idValue);
                        retMap.put("point_value", value);
                        retMap.put("real_time", formattedDateTime);
                        retMap.put("status", status);
                        String result = JSONUtil.toJsonStr(retMap);
                        // 发送消息
                        ProducerRecord<String, Object> record = new ProducerRecord<>("hldrl_udp_point_table", result);
                        producer.send(record);
                    }
                }
            } catch (SocketException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 将16进制字符串转换为字节数组
        private static byte[] hexStringToByteArray (String hex){
            int length = hex.length();
            byte[] bytes = new byte[length / 2];
            for (int i = 0; i < length; i += 2) {
                bytes[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                        + Character.digit(hex.charAt(i + 1), 16));
            }
            return bytes;
        }

        // 格式化时间戳为日期时间字符串
        private static String formatTimestamp ( int timestamp){
            Date date = new Date((long) timestamp * 1000); // 假设时间戳为秒单位
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return sdf.format(date);
        }
    }
