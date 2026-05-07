# 配置讲解-打造专属JSON转换器，解锁数据处理新姿势

# 背景


目前的项目几乎都是采用前后端分离来进行开发，在前端调用后端提供的接口时，不是说写好接口直接把数据返回给前端就完事了，这里面有很多格式的配置，下面会依次介绍要考虑的问题



### long类型丢失精度


在Java中，`long`类型是一个64位的整型，可以存储从-2^63到2^63-1范围内的整数。当这个整数被传输到前端时，通常是通过JSON格式进行传输的。前端大多数情况下是使用JavaScript来处理这些数据。JavaScript中，所有的数字（包括整数和浮点数）都是以64位浮点数格式存储的，根据IEEE 754标准。这种格式的数字最大能精确表示的整数范围是-2^53+1到2^53-1。



当一个超出JavaScript精确表示范围的`long`整数从Java后端传输到前端时，如果直接作为数字类型传输，那么在JavaScript中解析这个数字时就会丢失精度。这是因为在转换过程中，超出JavaScript能精确表示的范围的部分将无法准确表示，从而导致精度丢失。



#### 解决方案


+  将long类型转为String类型返回 

```java
long num = 4553115512345L;
String str = String.valueOf(num);
```

 

+  使用`@JsonFormat`注解，对实体类中的属性进行序列化和反序列化格式化的时候，将格式转为String类型 

```java
public class Test {
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Long orderId;
}
```

 

+  通过Springboot的配置 

```yaml
spring:
	jackson:
		serialization:
			WRITE_NUMBERS_AS_STRINGS: true
```

 



其实真正使用基本都是采用第三种，因为前两种都需要挨个配置很麻烦的



### 日期类型的转换


当后端将Date类型的字段传给前端时，前端接收到的数据格式并不如我们想的那样，而是这个样子



```json
{
    "showTime": "2024-03-21T12:00:00.000+00:00",
}
```



这种格式其实是**ISO 8601字符串**，这是一种广泛使用的日期和时间表示方法，格式如`2020-01-01T12:00:00Z`。很多现代的Web框架和API在传递日期时默认使用ISO 8601格式的字符串，因为它易于解析且避免了时区的混淆。例如，Java中的`java.time`包中的`Instant.toString()`会产生这种格式。



#### 解决方案


+  使用`@JsonFormat`注解 

```java
public class Test {
    @JsonFormat(pattern="yyyy-MM-dd",timezone = "GMT+8")
    private Date createTime;
}
```

 

+  通过Springboot的配置 

```yaml
spring：
	jackson:
		date-format: yyyy-MM-dd HH:mm:ss
		time-zone: GMT+8
```

 



### 多字段报错


默认情况下@RequestBody标注的对象必须包含前端传来的所有字段。



如果没有包含前端传来的字段，就会报错：`Unrecognized field xxx , not marked as ignorable`，这是因为**MappingJacksonHttpMessageConverter**默认要求必须存在相应的字段。如果没有前台传来的某个字段，就会报错



#### 解决方案


+  使用`@JsonIgnoreProperties`注解添加到类上 

```java
@JsonIgnoreProperties(ignoreUnknown = true)
public class Test {
    
}
```

 

+  全局配置 

```java
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {
  @Bean
  public MappingJackson2HttpMessageConverter mappingJackson2HttpMessageConverter(){

      MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
      ObjectMapper objectMapper = new ObjectMapper();
      //添加此配置
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      converter.setObjectMapper(objectMapper);
      return converter;
  }
}
```

 



### 字段空值


当后端返回的数据中，如果实体类的字段没有值的话，默认是返回null，比如



```json
{
    "code": 0,
    "message": null,
    "data": {
        "id": null,
        "title": null,
        "actor": null,
        "ticketCategoryVoList": null,
        "seatVoList": null
    }
}
```



这种null值在和ios客户端对接时，会造成ios的崩溃，当值没有时，需要存在一个默认值，例如：



```json
{
    "code": "0",
    "message": "",
    "data": {
        "id": "",
        "title": "",
        "actor": "",
        "ticketCategoryVoList": [],
        "seatVoList": []
    }
}
```



下面我会详细讲解如何配置实现这种效果



你可能会想，这几种情况除了最后一种，前几种不都提供了全局配置了吗？为什么还要定制配置呢？这是因为这些全局配置都是基于SpringMVC 这种**web框架**的配置，而在Gateway中这种**Reactor的web**是不适用的，在有些业务中，为了不影响性能，避免服务调用产生的网络损耗，会直接把接口写到Gateway中，所以需要做定制化的配置让其支持SpringMVC和Gateway



# 分析


在**spring-boot-starter**的依赖中依赖了**spring-boot-autoconfigure**，而在此依赖中存在了这么个配置



```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(ObjectMapper.class)
public class JacksonAutoConfiguration {

	//省略。。。

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(Jackson2ObjectMapperBuilder.class)
	static class JacksonObjectMapperConfiguration {
		//构建ObjectMapper
		@Bean
		@Primary
		@ConditionalOnMissingBean
		ObjectMapper jacksonObjectMapper(Jackson2ObjectMapperBuilder builder) {
			return builder.createXmlMapper(false).build();
		}

	}

	//省略。。。

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(Jackson2ObjectMapperBuilder.class)
	static class JacksonObjectMapperBuilderConfiguration {

		@Bean
		@Scope("prototype")
		@ConditionalOnMissingBean
		Jackson2ObjectMapperBuilder jacksonObjectMapperBuilder(ApplicationContext applicationContext,
				List<Jackson2ObjectMapperBuilderCustomizer> customizers) {
			Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
			builder.applicationContext(applicationContext);
			//对ObjectMapper进行订制
			customize(builder, customizers);
			return builder;
		}

		private void customize(Jackson2ObjectMapperBuilder builder,
				List<Jackson2ObjectMapperBuilderCustomizer> customizers) {
			//循环Jackson2ObjectMapperBuilderCustomizer类型的集合，依次执行进行定制
			for (Jackson2ObjectMapperBuilderCustomizer customizer : customizers) {
				customizer.customize(builder);
			}
		}

	}
}
```

### 总结
+ 构建`Jackson2ObjectMapperBuilder`对象，在此对象构建中，调用`customize`方法，将`Jackson2ObjectMapperBuilder`对象传入，进行定制化
+ 在`customize`方法中，查找`Jackson2ObjectMapperBuilderCustomizer`类型的集合，然后依次执行进行定制的方法，对`Jackson2ObjectMapperBuilder`进行定制
+ 依靠`Jackson2ObjectMapperBuilder`来创建`ObjectMapper`



### ObjectMapper


+ `ObjectMapper`是很重要的转换类，SpringMVC在接收请求的参数后，都是依靠`ObjectMapper`来进行序列化的，所以一切的根本就是要对`ObjectMapper`进行定制化改造
+ 从上面的分析我们知道了要从`Jackson2ObjectMapperBuilderCustomizer`来入手



### Jackson2ObjectMapperBuilderCustomizer


```java
@FunctionalInterface
public interface Jackson2ObjectMapperBuilderCustomizer {

	/**
	 * Customize the JacksonObjectMapperBuilder.
	 * @param jacksonObjectMapperBuilder the JacksonObjectMapperBuilder to customize
	 */
	void customize(Jackson2ObjectMapperBuilder jacksonObjectMapperBuilder);

}
```



我们需要实现`Jackson2ObjectMapperBuilderCustomizer`接口，然后注入到Spring的容器中就可以了



### 进行定制


```properties
org.springframework.boot.autoconfigure.EnableAutoConfiguration=\
  com.damai.config.DaMaiCommonAutoConfig
```



```java
public class DaMaiCommonAutoConfig {
    
    @Bean
    public Jackson2ObjectMapperBuilderCustomizer JacksonCustom(){
        return new JacksonCustom();
    }
}
```



```java
public class JacksonCustom implements Jackson2ObjectMapperBuilderCustomizer, Ordered {

    /**
     * 默认日期时间格式 
     * */
    private final String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
    
    @Override
    public void customize(Jackson2ObjectMapperBuilder builder) {
        builder.serializationInclusion(Include.ALWAYS);
        
        //允许单引号、允许不带引号的字段名称
        builder.featuresToEnable(Feature.ALLOW_SINGLE_QUOTES);
        builder.featuresToEnable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
        
        
        SimpleModule[] simpleModules = new SimpleModule[9];
        
        //添加自定义的序列化器处理空值
        simpleModules[0] = new SimpleModule().setSerializerModifier(new JsonCustomSerializer());
        //将Date类型日期转换为字符串
        simpleModules[1] = new SimpleModule().addSerializer(Date.class, new JsonSerializer<Date>() {

            @Override
            public void serialize(Date value, JsonGenerator gen, SerializerProvider serializers)
                    throws IOException {
                SimpleDateFormat sdf = new SimpleDateFormat(dateTimeFormat);
                String newValue = sdf.format(value);
                gen.writeString(newValue);
            }

        });
        //将Date类型时间格式转换反序列化
        simpleModules[2] = new SimpleModule().addDeserializer(Date.class, new DateJsonDeserializer());
        //将LocalDateTime进行格式化和序列化
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
        simpleModules[3] = new SimpleModule().addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(dateTimeFormatter));
        simpleModules[4] = new SimpleModule().addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(dateTimeFormatter));
        //将LocalDate进行格式化和序列化
        //默认日期格式
        String dateFormat = "yyyy-MM-dd";
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(dateFormat);
        simpleModules[5] = new SimpleModule().addSerializer(LocalDate.class, new LocalDateSerializer(dateFormatter));
        simpleModules[6] = new SimpleModule().addDeserializer(LocalDate.class, new LocalDateDeserializer(dateFormatter));
        //将LocalTime进行格式化和序列化
        //默认时间格式
        String timeFormat = "HH:mm:ss";
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(timeFormat);
        simpleModules[7] = new SimpleModule().addSerializer(LocalTime.class, new LocalTimeSerializer(timeFormatter));
        simpleModules[8] = new SimpleModule().addDeserializer(LocalTime.class, new LocalTimeDeserializer(timeFormatter));
        
        builder.modules(simpleModules);
        
        //设置时区
        builder.timeZone(TimeZone.getDefault());
        //允许有未知的属性
        builder.featuresToDisable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        //允许包含不带引号的控制字符
        builder.featuresToEnable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature());
        //数字转换为字符串
        builder.featuresToEnable(JsonWriteFeature.WRITE_NUMBERS_AS_STRINGS.mappedFeature());
    }

    @Override
    public int getOrder() {
        return 1;
    }
}
```



### JsonCustomSerializer 自定义JSON序列化


```java
public class JsonCustomSerializer extends BeanSerializerModifier {

	@Override
	public List<BeanPropertyWriter> changeProperties(SerializationConfig config, BeanDescription beanDesc,
			List<BeanPropertyWriter> beanProperties) {
		for (BeanPropertyWriter writer : beanProperties) {
			com.fasterxml.jackson.databind.JsonSerializer<Object> js = judgeType(writer);
			if (js != null) {
				writer.assignNullSerializer(js);
			}
		}
		return beanProperties;
	}

	public com.fasterxml.jackson.databind.JsonSerializer<Object> judgeType(BeanPropertyWriter writer) {
		JavaType javaType = writer.getType();
		Class<?> clazz = javaType.getRawClass();
		//String类型
		if (String.class.isAssignableFrom(clazz)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeString("");
				}
			};
		}
		//数字类型
		if (Number.class.isAssignableFrom(clazz)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeString("");
				}
			};
		}
		//Boolean类型
		if (Boolean.class.isAssignableFrom(clazz)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeBoolean(false);
				}
			};
		}
		//Date类型
		if (java.util.Date.class.isAssignableFrom(clazz)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeString("");
				}
			};
		}
		//DateTime类型
		if (clazz.equals(DateTime.class)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeString("");
				}
			};
		}
		//数组、List、Set类型
		if (clazz.isArray() || clazz.equals(List.class) || clazz.equals(Set.class)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeStartArray();
					gen.writeEndArray();
				}
			};
		}
		return null;
	}
}
```



### 依赖


```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>damai-common</artifactId>
    <version>${revision}</version>
</dependency>
```



> 更新: 2024-03-25 17:41:24  
> 原文: <https://www.yuque.com/u22210564/ykdrdh/cpetgiw4db3vxdql>