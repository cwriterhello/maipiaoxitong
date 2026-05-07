# 业务讲解-API请求定制化防刷数据存储策略

# 介绍


先在gateway网关中进行限制规则的执行验证功能，关于此功能的详细讲解，可跳转到文档

[业务讲解-API接口定制化防刷策略实现](https://www.yuque.com/u22210564/ykdrdh/tv9a5gxrf3pqaz24)



接着要把进行限制的请求记录下来，然后保存起来方便查看，为了尽可能最小的影响程序的性能，决定把保存数据这个步骤使用kafka来进行异步执行，当在gateway产生数据后，放到kafka中，然后由customize服务来进行消费



# kafka的配置


## 生产者配置


在`damai-gateway-service`服务模块下



### 参数配置
```yaml
spring:
  kafka:
  bootstrap-servers: 127.0.0.1:9092
  producer:
    retries: 1
    key-serializer: org.apache.kafka.common.serialization.StringSerializer
    value-serializer: org.apache.kafka.common.serialization.StringSerializer
  topic: save_api_data
```

### Topic配置


```java
@Data
public class KafkaTopic {
    
    @Value("${spring.kafka.topic:default}")
    private String topic;

}
```



### 发送者配置


```java
@ConditionalOnProperty(value = "spring.kafka.bootstrap-servers")
public class ProducerConfig {
    
    @Bean
    public KafkaTopic kafkaTopic(){
        return new KafkaTopic();
    }
    
    @Bean
    public ApiDataMessageSend apiDataMessageSend(KafkaTemplate<String, String> kafkaTemplate, KafkaTopic kafkaTopic){
        return new ApiDataMessageSend(kafkaTemplate, kafkaTopic.getTopic());
    }
}
```



```java
@Slf4j
@AllArgsConstructor
public class ApiDataMessageSend {
    
    private KafkaTemplate<String, String> kafkaTemplate;
    
    private String topic;
    
    public void sendMessage(String message) {
        log.info("sendMessage message : {}", message);
        kafkaTemplate.send(topic,message);
    }
}
```



### gateway中开始生产数据的过程


```java
public void saveApiData(ServerHttpRequest request, String apiUrl, Integer type){
    ApiDataDto apiDataDto = new ApiDataDto();
    //id
    apiDataDto.setId(uidGenerator.getUid());
    //客户端ip
    apiDataDto.setApiAddress(getIpAddress(request));
    //请求的路径
    apiDataDto.setApiUrl(apiUrl);
    //创建的时间
    apiDataDto.setCreateTime(DateUtils.now());
    //按天维度记录请求时间
    apiDataDto.setCallDayTime(DateUtils.nowStr(DateUtils.FORMAT_DATE));
    //按小时维度记录请求时间
    apiDataDto.setCallHourTime(DateUtils.nowStr(DateUtils.FORMAT_HOUR));
    //按分钟维度记录请求时间
    apiDataDto.setCallMinuteTime(DateUtils.nowStr(DateUtils.FORMAT_MINUTE));
    //按秒维度记录请求时间
    apiDataDto.setCallSecondTime(DateUtils.nowStr(DateUtils.FORMAT_SECOND));
    //api规则生效类型 1一般规则 2深度规则
    apiDataDto.setType(type);
    Optional.ofNullable(apiDataMessageSend).ifPresent(send -> send.sendMessage(JSON.toJSONString(apiDataDto)));
}
```



## 消费者配置


在`damai-customize-service`服务模块下



### 参数配置
```yaml
spring:
  kafka:
    bootstrap-servers: 127.0.0.1:9092
    consumer:
      #默认的消费组ID
      group-id: api_data
      #是否自动提交offset
      enable-auto-commit: true
      #提交offset延时
      auto-commit-interval: 2000
      # 当kafka中没有初始offset或offset超出范围时将自动重置offset
      # earliest:重置为分区中最小的offset;
      # latest:重置为分区中最新的offset(消费分区中新产生的数据);
      # none:只要有一个分区不存在已提交的offset,就抛出异常;
      auto-offset-reset: latest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      # 主题
      topic: save_api_data
```



### 进行具体消费
```java
@Slf4j
@AllArgsConstructor
@Component
public class ApiDataMessageConsumer {
    
    @Autowired
    private ApiDataService apiDataService;
    
    @KafkaListener(topics = {"${spring.kafka.topic:save_api_data}"})
    public void consumerOrderMessage(ConsumerRecord<String,String> consumerRecord){
        try {
            Optional.ofNullable(consumerRecord.value()).map(String::valueOf).ifPresent(value -> {
                log.info("consumerOrderMessage message:{}",value);
                ApiData apiData = JSON.parseObject(value, ApiData.class);
                apiDataService.saveApiData(apiData);
            });
        }catch (Exception e) {
            log.error("consumerApiDataMessage error",e);
        }
    }
}
```



```java
@RepeatExecuteLimit(name = RepeatExecuteLimitConstants.CONSUMER_API_DATA_MESSAGE,keys = {"#apiData.id"})
public void saveApiData(ApiData apiData){
    ApiData dbApiData = apiDataMapper.selectById(apiData.getId());
    if (Objects.isNull(dbApiData)) {
        log.info("saveApiData apiData:{}", JSON.toJSONString(apiData));
        apiDataMapper.insert(apiData);
    }
}
```



使用**@RepeatExecuteLimit **** **组件，来实现幂等性，如果幂等组件允许执行业务的话，先查询库中是否已存在记录，如果不存在，则进行添加



关于幂等组件的详细讲解，可跳转到相应的文档

[组件讲解-如何打造高效幂等组件，确保数据一致性](https://www.yuque.com/u22210564/ykdrdh/ro5vey3tmlgzdw0p)



### 表结构
```sql
CREATE TABLE `api_data` (
  `id` bigint(64) NOT NULL COMMENT '主键id',
  `head_version` varchar(32) DEFAULT NULL COMMENT '请求版本',
  `api_address` varchar(32) DEFAULT NULL COMMENT '客户端ip',
  `api_method` varchar(32) DEFAULT NULL COMMENT '请求方法',
  `api_body` varchar(200) DEFAULT NULL COMMENT '请求体',
  `api_params` varchar(100) DEFAULT NULL COMMENT '请求参数',
  `api_url` varchar(100) DEFAULT NULL COMMENT '请求路径',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `status` int(11) DEFAULT '1' COMMENT '状态 1:未删除 0:删除(默认1)',
  `call_day_time` varchar(64) DEFAULT NULL COMMENT '按天维度记录请求时间',
  `call_hour_time` varchar(64) DEFAULT NULL COMMENT '按小时维度记录请求时间',
  `call_minute_time` varchar(64) DEFAULT NULL COMMENT '按分钟维度记录请求时间',
  `call_second_time` varchar(64) DEFAULT NULL COMMENT '按秒维度记录请求时间',
  `type` int(11) DEFAULT NULL COMMENT 'api规则生效类型 1一般规则 2深度规则',
  PRIMARY KEY (`id`),
  KEY `idx_create_time` (`create_time`) USING BTREE,
  KEY `idx_api_address` (`api_address`) USING BTREE,
  KEY `idx_api_url` (`api_url`) USING BTREE,
  KEY `idx_call_day_time` (`call_day_time`) USING BTREE,
  KEY `idx_call_hour_time` (`call_hour_time`) USING BTREE,
  KEY `idx_call_minute_time` (`call_minute_time`) USING BTREE,
  KEY `idx_call_second_time` (`call_second_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='api执行表';
```



#### 保存的记录截图：


> 更新: 2024-06-21 16:35:17  
> 原文: <https://www.yuque.com/u22210564/ykdrdh/yq6fqk92m6vdfil5>