# 组件讲解-如何对Elasticsearch进行高效封装

# Elasticsearch 的功能和作用


Elasticsearch 是当今世界上最受欢迎的搜索引擎之一，广泛应用于日志分析、全文搜索、安全情报、业务分析等多个领域。它以其高性能、强大的数据分析能力和易用性而闻名。Elasticsearch 能够处理各种类型的数据，包括文本、数字、地理位置、结构化和非结构化数据。



## 核心功能


+ **全文搜索**: Elasticsearch 提供高级的全文搜索功能，包括多语言处理、自动类型推断、自然语言处理等，使得搜索结果更加准确和相关
+ **实时分析**: 与传统数据库相比，Elasticsearch 能够在数据写入的同时进行实时分析，为用户提供即时的业务洞察
+ **水平可扩展性**: Elasticsearch 可以轻松扩展到多个节点，无需更改应用程序代码，就能处理 PB 级别的数据
+ **高可用性和分布式**: Elasticsearch 的分布式特性使其具有高可用性和容错能力，即使在节点故障的情况下也能保证服务的连续性
+ **多租户能力**: 通过使用 Elasticsearch 的索引和别名功能，可以在单个 Elasticsearch 集群上支持多个租户的数据隔离



## 应用场景


+ **日志和事件数据分析**: Elasticsearch 被广泛用于收集、分析和可视化日志数据，帮助组织发现系统中的问题和优化机会
+ **网站搜索**: 提供网站内容的快速、相关搜索，改善用户体验
+ **安全情报**: 分析网络流量和用户行为，以检测恶意活动和安全威胁
+ **商业智能**: 对客户数据、市场趋势进行实时分析，支持决策制定



## 特点


Elasticsearch 的搜索能力是其最核心的功能之一，提供了从简单的文本匹配到复杂的聚合查询等广泛的搜索功能。它通过灵活的数据索引结构和强大的查询语言（Query DSL）来实现高效、精确的搜索结果。以下是 Elasticsearch 搜索能力的详细介绍：



### 全文搜索


Elasticsearch 强大的全文搜索能力是其最引人注目的特性之一。它使用了倒排索引（Inverted Index）来高效地存储和检索文本数据。用户可以进行模糊查询、词干匹配、同义词处理等，以及利用 Elasticsearch 提供的各种文本分析器来优化搜索结果的相关性



### 复杂查询构建


Elasticsearch 的 Query DSL 提供了丰富的查询构建块，允许用户执行精确匹配、范围查询、布尔查询、嵌套查询等复杂搜索操作。这些查询可以组合使用，构建出能够满足几乎任何搜索需求的复杂查询



### 聚合分析


除了搜索文档，Elasticsearch 还提供了强大的聚合功能，允许用户在搜索结果上进行统计分析。这包括但不限于计数、求和、平均值、最小/最大值等。聚合功能可以帮助用户洞察数据模式，支持复杂的数据分析需求



### 实时搜索


Elasticsearch 能够提供几乎实时的搜索体验。当文档被索引时，它几乎立即就可以被搜索到。这对于需要实时反馈的应用场景（如日志监控、社交媒体分析等）至关重要



# Elasticsearch的type问题


Elasticsearch存储数据是以每个索引index作为维度来存储的，可以理解成关系型数据库中的表，而字段是以文档document存在的，可以理解成表的字段



而在Elasticsearch6.x之前还存在type的概念，介于index和document之间



Elasticsearch6.x之前一个index下可以有多个不同的type，而在7.x开始就type的概念去掉了，采用同一个doc表示type，也就是一个index只能有一个type，这个type还必须是doc，而在8.x中，彻底将type的概念去除了



### 7.x为什么去掉了type？


#### 映射爆炸（Mapping Explosion）


在使用多类型时，如果不同类型之间有大量不同的字段，这会导致映射的数量急剧增加，进而引发映射爆炸问题。映射爆炸不仅会消耗大量的内存资源，还会降低 Elasticsearch 的性能，尤其是在处理大量数据时



#### 字段名冲突


在同一个index的不同type中，如果有相同名称但映射类型不同的字段，会造成字段名冲突。这是因为 Elasticsearch 在内部是将这些字段扁平化处理的，而不同类型的相同名称字段可能会导致数据解析和查询时的混乱



我们可以和关系型数据库来对比，在同一个数据库中，这些不同的表，可以有名称相同但类型不同的字段。而在 Elasticsearch 同一个index的不同type中，如果有不同document的字段名相同，但是类型不同，就会报错



#### 总结


综上所述，Elasticsearch 从 7.x 版本开始废弃类型的主要目的是为了提升系统的性能、避免映射爆炸和字段冲突的问题，以及简化数据模型的设计和管理。这一改变反映了 Elasticsearch 对于提高性能、可维护性和用户体验的持续追求



# 操作


Springboot提供了对Elasticsearch的操作，需要添加以下依赖



```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-elasticsearch</artifactId>
</dependency>
```



但使用中还是有一些问题，在平时开发中还是使用关系型数据库更加普遍，对于数据库、表、字段的概念更为熟悉，也更加习惯对表概念的操作。而在操作Elasticsearch时，提供的api其实是很复杂的，各种操作的对象，如`SearchSourceBuilder` `FieldSortBuilder` `BoolQueryBuilder` 等等，操作上其实算不上简单，所以为了解决这个问题，本人在大麦网中在springboot操作Elasticsearch的基础上，进一步的封装，使用起来贴近于关系型数据库的方式，操作起来更加的容易上手



# damai-elasticsearch-framework


## 介绍


此组件提供了对Elasticsearch常见的操作，操作起来很简单，并且兼容了6.x 、7.x 、8.x的版本



## 使用


### 引入依赖


```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>damai-elasticsearch-framework</artifactId>
    <version>${revision}</version>
</dependency>
```



### 添加地址配置


```yaml
elasticsearch:
  ip: #地址:#端口号
  userName: #账号
  passWord: #密码
  # 是否开启type，默认为false
  esTypeSwitch: false
```



此**type**的作用就是为了兼容6.x、7.x中的type概念，默认是关闭



### 引入api


```java
@Autowired
private BusinessEsHandle businessEsHandle;

public void test(){
    boolean result = businessEsHandle.checkIndex(ProgramDocumentParamName.INDEX_NAME, ProgramDocumentParamName.INDEX_TYPE);
}
```



关于此组件的详细使用，在节目服务下的搜索节目和查询节目的业务中有非常详细的示例，包括了**索引的创建、数据的添加、数据的查询等功能**，本文篇幅有限，都贴出来实在是太长了，小伙伴可直接跳转到 业务讲解 模块下的 节目服务部分 来学习，有详细的介绍



## 讲解


### 参数配置


```java
@Data
@ConfigurationProperties(prefix = BusinessEsProperties.PREFIX)
public class BusinessEsProperties {
    
    public static final String PREFIX = "elasticsearch";
    
    /**
     * 地址
     * */
    private String[] ip;
    
    /**
     * 账户
     * */
    private String userName;
    
    /**
     * 密码
     * */
    private String passWord;
    
    /**
     * 是否开启使用，默认 true
     * */
    private Boolean esSwitch = true;
    
    /**
     * 是否开启type，默认 false
     * */
    private Boolean esTypeSwitch = false;
    
    /**
     * 连接超时时间
     * */
    private Integer connectTimeOut = 40000;
    
    /**
     * socket超市时间
     * */
    private Integer socketTimeOut = 40000;
    
    /**
     * 连接请求超时时间
     * */
    private Integer connectionRequestTimeOut = 40000;
    
    /**
     * 最大连接数
     * */
    private Integer maxConnectNum = 400;
}
```



### 自动装配类


```java
@EnableConfigurationProperties(BusinessEsProperties.class)
@ConditionalOnProperty(value = "elasticsearch.ip")
public class BusinessEsAutoConfig {
	private static final int ADDRESS_LENGTH = 2;
	private static final String HTTP_SCHEME = "http";

	/**
	 * 配置Elasticsearch的客户端
	 * */
	@Bean
	public RestClient businessEsRestClient(BusinessEsProperties businessEsProperties) {
		String defaultValue = "default";
		HttpHost[] hosts = Arrays.stream(businessEsProperties.getIp()).map(this::makeHttpHost).filter(Objects::nonNull)
				.toArray(HttpHost[]::new);
		
		RestClientBuilder builder = RestClient.builder(hosts);
		String userName = businessEsProperties.getUserName();
		String passWord = businessEsProperties.getPassWord();
		if (StringUtil.isNotEmpty(userName) && !defaultValue.equals(userName) && StringUtil.isNotEmpty(passWord) && !defaultValue.equals(passWord)) {
			//开始设置用户名和密码
			CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, passWord));
			builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    .setDefaultIOReactorConfig(
                    IOReactorConfig.custom()
                            // 设置线程数
                            .setIoThreadCount(businessEsProperties.getMaxConnectNum()) 
                            .build()));
		}
		Header[] defaultHeaders = { new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
				new BasicHeader("Role", "Read") };
		// 设置每个请求需要发送的默认headers，这样就不用在每个请求中指定它们。
		builder.setDefaultHeaders(defaultHeaders);
		// 设置相关参数
		builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
				.setConnectTimeout(businessEsProperties.getConnectTimeOut())
				.setSocketTimeout(businessEsProperties.getSocketTimeOut())
				.setConnectionRequestTimeout(businessEsProperties.getConnectionRequestTimeOut()));
        return builder.build();
	}
	/**
	 * Elasticsearch的操作api
	 * */
	@Bean
	public BusinessEsHandle businessEsHandle(@Qualifier("businessEsRestClient")RestClient businessEsRestClient, BusinessEsProperties businessEsProperties){
		return new BusinessEsHandle(businessEsRestClient,businessEsProperties.getEsSwitch(),businessEsProperties.getEsTypeSwitch());
	}

	/**
	 * 获取HttpHost对象
	 *
	 */
	private HttpHost makeHttpHost(String s) {
		assert StringUtil.isNotEmpty(s);
		String[] address = s.split(":");
		if (address.length == ADDRESS_LENGTH) {
			String ip = address[0];
			int port = Integer.parseInt(address[1]);
			return new HttpHost(ip, port, HTTP_SCHEME);
		} else {
			return null;
		}
	}
}
```



### 操作的api


```java
@Slf4j
@AllArgsConstructor
public class BusinessEsHandle {
    
    private final RestClient restClient;
    
    private final Boolean esSwitch;
    
    private final Boolean esTypeSwitch;
    
    /**
     * 创建索引
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param list 参数集合
     */
    public void createIndex(String indexName, String indexType, List<EsDocumentMappingDto> list) throws IOException {
        if (!esSwitch) {
            return;
        }
        if (CollectionUtil.isEmpty(list)) {
            return;
        }
        IndexRequest indexRequest = new IndexRequest();
        XContentBuilder builder = JsonXContent.contentBuilder().startObject().startObject("mappings");
        if (esTypeSwitch) {
            builder = builder.startObject(indexType);
        }
        builder = builder.startObject("properties");
        for (EsDocumentMappingDto esDocumentMappingDto : list) {
            String paramName = esDocumentMappingDto.getParamName();
            String paramType = esDocumentMappingDto.getParamType();
            if ("text".equals(paramType)) {
                Map<String,Map<String,Object>> map1 = new HashMap<>(8);
                Map<String,Object> map2 = new HashMap<>(8);
                map2.put("type","keyword");
                map2.put("ignore_above",256);
                map1.put("keyword",map2);
                builder = builder.startObject(paramName).field("type", "text").field("fields",map1).endObject();
            }else {
                builder = builder.startObject(paramName).field("type", paramType).endObject();
            }
        }
        if (esTypeSwitch) {
            builder.endObject();
        }
        builder = builder.endObject().endObject().startObject("settings").field("number_of_shards", 3)
                .field("number_of_replicas", 1).endObject().endObject();
    
        indexRequest.source(builder);
        String source = indexRequest.source().utf8ToString();
        log.info("create index execute dsl : {}",source);
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        Request request = new Request("PUT","/"+ indexName);
        request.setEntity(entity);
        request.addParameters(Collections.<String, String>emptyMap());
        Response performRequest = restClient.performRequest(request);
    }
    
    /**
     * 检查索引是否存在
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @return boolean
     */
    public boolean checkIndex(String indexName, String indexType)  {
        if (!esSwitch) {
            return false;
        }
        try {
            String path = "";
            if (esTypeSwitch) {
                path = "/" + indexName + "/" + indexType + "/_mapping?include_type_name";
            }else {
                path = "/" + indexName + "/_mapping";
            }
            Request request = new Request("GET", path);
            request.addParameters(Collections.<String, String>emptyMap());
            Response response = restClient.performRequest(request);
            return "OK".equals(response.getStatusLine().getReasonPhrase());
        }catch (Exception e) {
            if (e instanceof ResponseException && ((ResponseException)e).getResponse().getStatusLine().getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
                log.warn("index not exist ! indexName:{}, indexType:{}", indexName,indexType);
            }else {
                log.error("checkIndex error",e);
            }
            return false;
        }
    }
    /**
     * 删除索引
     *
     * @param indexName 索引名字
     * @return boolean
     */
    public boolean deleteIndex(String indexName) {
        if (!esSwitch) {
            return false;
        }
        try {
            Request request = new Request("DELETE", "/" + indexName);
            request.addParameters(Collections.<String, String>emptyMap());
            Response response = restClient.performRequest(request);
            return "OK".equals(response.getStatusLine().getReasonPhrase());
        }catch (Exception e) {
            log.error("deleteIndex error",e);
        }
        return false;
    }
    
    /**
     * 清空索引下所有数据
     *
     * @param indexName 索引名字
     */
    public void deleteData(String indexName) {
        if (!esSwitch) {
            return;
        }
        deleteIndex(indexName);
    }
    
    /**
     * 添加
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param params 参数 key:字段名 value:具体值
     * @return boolean
     */
    public boolean add(String indexName, String indexType,Map<String,Object> params) {
        return add(indexName, indexType, params, null);
    }
    
    /**
     * 添加
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param params 参数 key:字段名 value:具体值
     * @param id 文档id 如果为空，则使用es默认id
     * @return boolean
     */
    public boolean add(String indexName, String indexType,Map<String,Object> params, String id) {
        if (!esSwitch) {
            return false;
        }
        if (CollectionUtil.isEmpty(params)) {
            return false;
        }
        try {
            String jsonString = JSON.toJSONString(params);
            HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
            String endpoint = "";
            if (esTypeSwitch) {
                endpoint = "/" + indexName + "/" + indexType;
            }else {
                endpoint = "/" + indexName + "/_doc";
            }
            if (StringUtil.isNotEmpty(id)) {
                endpoint = endpoint + "/" + id;
            }
            log.info("add dsl : {}",jsonString);
            Request request = new Request("POST",endpoint);
            request.setEntity(entity);
            request.addParameters(Collections.<String, String>emptyMap());
            Response indexResponse = restClient.performRequest(request);
            String reasonPhrase = indexResponse.getStatusLine().getReasonPhrase();
            return "created".equalsIgnoreCase(reasonPhrase) || "ok".equalsIgnoreCase(reasonPhrase);
        }catch (Exception e) {
            log.error("add error",e);
        }
        return false;
    }
    
    /**
     * 查询
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param esDataQueryDtoList 参数
     * @param clazz 返回的类型
     * @return List
     */
    public <T> List<T> query(String indexName, String indexType, List<EsDataQueryDto> esDataQueryDtoList, Class<T> clazz) throws IOException {
        if (!esSwitch) {
            return new ArrayList<>();
        }
        return query(indexName, indexType, null, esDataQueryDtoList, null, null, null, null, null, clazz);
    }
    
    /**
     * 查询
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param esGeoPointDto 经纬度查询参数
     * @param esDataQueryDtoList 参数
     * @param clazz 返回的类型
     * @return List
     */
    public <T> List<T> query(String indexName, String indexType, EsGeoPointDto esGeoPointDto, List<EsDataQueryDto> esDataQueryDtoList, Class<T> clazz) throws IOException {
        if (!esSwitch) {
            return new ArrayList<>();
        }
        return query(indexName, indexType, esGeoPointDto, esDataQueryDtoList, null, null, null, null,null,clazz);
    }
    
    /**
     * 查询
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param esDataQueryDtoList 参数
     * @param sortParam 普通参数排序 不排序则为空 如果进行了排序，会返回es中的排序字段sort，需要用户在返回的实体类中添加sort字段
     * @param sortOrder 升序还是降序，为空则降序
     * @param clazz 返回的类型
     * @return List
     */
    public <T> List<T> query(String indexName, String indexType, List<EsDataQueryDto> esDataQueryDtoList, String sortParam, SortOrder sortOrder, Class<T> clazz) throws IOException {
        if (!esSwitch) {
            return new ArrayList<>();
        }
        return query(indexName, indexType, null, esDataQueryDtoList, sortParam, null, sortOrder, null, null, clazz);
    }
    
    /**
     * 查询
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param esDataQueryDtoList 参数
     * @param geoPointDtoSortParam 经纬度參數排序 不排序则为空 如果进行了排序，会返回es中的排序字段sort，需要用户在返回的实体类中添加sort字段
     * @param sortOrder 升序还是降序，为空则降序
     * @param clazz 返回的类型
     * @return List
     */
    public <T> List<T> query(String indexName, String indexType, List<EsDataQueryDto> esDataQueryDtoList, EsGeoPointSortDto geoPointDtoSortParam, SortOrder sortOrder, Class<T> clazz) throws IOException {
        if (!esSwitch) {
            return new ArrayList<>();
        }
        return query(indexName, indexType, null, esDataQueryDtoList, null, geoPointDtoSortParam, sortOrder,null,null, clazz);
    }
    
    
    
    /**
     * 查询
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param esGeoPointDto 经纬度查询参数
     * @param esDataQueryDtoList 参数
     * @param sortParam 普通參數排序 不排序则为空 如果进行了排序，会返回es中的排序字段sort，需要用户在返回的实体类中添加sort字段
     * @param geoPointDtoSortParam 经纬度參數排序 不排序则为空 如果进行了排序，会返回es中的排序字段sort，需要用户在返回的实体类中添加sort字段
     * @param sortOrder 升序还是降序，为空则降序
     * @param pageSize searchAfterSort搜索的页大小
     * @param searchAfterSort sort值
     * @param clazz 返回的类型
     * @return List
     */
    public <T> List<T> query(String indexName, String indexType, EsGeoPointDto esGeoPointDto, List<EsDataQueryDto> esDataQueryDtoList, String sortParam, EsGeoPointSortDto geoPointDtoSortParam, SortOrder sortOrder, Integer pageSize, Object[] searchAfterSort, Class<T> clazz) throws IOException {
        List<T> list = new ArrayList<>();
        if (!esSwitch) {
            return list;
        }
        SearchSourceBuilder sourceBuilder = getSearchSourceBuilder(esGeoPointDto,esDataQueryDtoList,sortParam,geoPointDtoSortParam,sortOrder);
        executeQuery(indexName,indexType,list,null,clazz,sourceBuilder);
        return list;
    }
    
    
    /**
     * 查询(分页)
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param esDataQueryDtoList 参数
     * @param pageNo 页码
     * @param pageSize 页大小
     * @param clazz 返回的类型
     * @return PageInfo
     */
    public <T> PageInfo<T> queryPage(String indexName, String indexType, List<EsDataQueryDto> esDataQueryDtoList, Integer pageNo, Integer pageSize, Class<T> clazz) throws IOException {
        return queryPage(indexName, indexType, esDataQueryDtoList, null, null, pageNo, pageSize, clazz);
    }
    
    /**
     * 查询(分页)
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param esDataQueryDtoList 参数
     * @param sortParam 排序参数 不排序则为空 如果进行了排序，会返回es中的排序字段sort，需要用户在返回的实体类中添加sort字段
     * @param sortOrder 升序还是降序，为空则降序
     * @param pageNo 页码
     * @param pageSize 页大小
     * @param clazz 返回的类型
     * @return PageInfo
     */
    public <T> PageInfo<T> queryPage(String indexName, String indexType, List<EsDataQueryDto> esDataQueryDtoList, String sortParam, SortOrder sortOrder, Integer pageNo, Integer pageSize, Class<T> clazz) throws IOException {
        return queryPage(indexName, indexType, null, esDataQueryDtoList, sortParam, null,sortOrder, pageNo, pageSize, clazz);
    }
    
    /**
     * 查询(分页)
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param esGeoPointDto 经纬度查询参数
     * @param esDataQueryDtoList 参数
     * @param pageNo 页码
     * @param pageSize 页大小
     * @param clazz 返回的类型
     * @return PageInfo
     */
    public <T> PageInfo<T> queryPage(String indexName, String indexType, EsGeoPointDto esGeoPointDto, List<EsDataQueryDto> esDataQueryDtoList, Integer pageNo, Integer pageSize, Class<T> clazz) throws IOException {
        return queryPage(indexName, indexType, esGeoPointDto, esDataQueryDtoList, null, null,null, pageNo, pageSize, clazz);
    }
    
    /**
     * 查询(分页)
     *
     * @param indexName 索引名字
     * @param indexType 索引类型
     * @param esGeoPointDto 经纬度查询参数
     * @param esDataQueryDtoList 参数
     * @param sortParam 排序参数 不排序则为空 如果进行了排序，会返回es中的排序字段sort，需要用户在返回的实体类中添加sort字段
     * @param sortOrder 升序还是降序，为空则降序
     * @param pageNo 页码
     * @param pageSize 页大小
     * @param clazz 返回的类型
     * @return
     * @throws IOException
     */
    public <T> PageInfo<T> queryPage(String indexName, String indexType, EsGeoPointDto esGeoPointDto, 
                                     List<EsDataQueryDto> esDataQueryDtoList, String sortParam, 
                                     EsGeoPointSortDto geoPointDtoSortParam, SortOrder sortOrder, Integer pageNo, 
                                     Integer pageSize, Class<T> clazz) throws IOException {
        List<T> list = new ArrayList<>();
        PageInfo<T> pageInfo = new PageInfo<>(list);
        pageInfo.setPageNum(pageNo);
        pageInfo.setPageSize(pageSize);
        if (!esSwitch) {
            return pageInfo;
        }
        SearchSourceBuilder sourceBuilder = getSearchSourceBuilder(esGeoPointDto,esDataQueryDtoList,sortParam,geoPointDtoSortParam,sortOrder);
        sourceBuilder.from((pageNo - 1) * pageSize);
        sourceBuilder.size(pageSize);
        executeQuery(indexName,indexType,list,pageInfo,clazz,sourceBuilder);
        return pageInfo;
    }
    
    /**
     * 构建查询条件
     * @param esGeoPointDto 经纬度类型查询数据
     * @param esDataQueryDtoList 普通类型查询数据
     * @param sortParam 排序字段
     * @param geoPointDtoSortParam 经纬度排序
     * @param sortOrder 排序类型
     * */
    private SearchSourceBuilder getSearchSourceBuilder(EsGeoPointDto esGeoPointDto, List<EsDataQueryDto> esDataQueryDtoList, String sortParam, EsGeoPointSortDto geoPointDtoSortParam, SortOrder sortOrder){
        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        if (Objects.isNull(sortOrder)) {
            sortOrder = SortOrder.DESC;
        }
        //排序
        if (StringUtil.isNotEmpty(sortParam)) {
            FieldSortBuilder sort = SortBuilders.fieldSort(sortParam);
            sort.order(sortOrder);
            sourceBuilder.sort(sort);
        }
        //经纬度排序
        if (Objects.nonNull(geoPointDtoSortParam)) {
            GeoDistanceSortBuilder sort = SortBuilders.geoDistanceSort("geoPoint", geoPointDtoSortParam.getLatitude().doubleValue(), geoPointDtoSortParam.getLongitude().doubleValue());
            sort.unit(DistanceUnit.METERS);
            sort.order(sortOrder);
            sourceBuilder.sort(sort);
        }
        //经纬度查询匹配
        if (Objects.nonNull(esGeoPointDto)) {
            QueryBuilder geoQuery = new GeoDistanceQueryBuilder(esGeoPointDto.getParamName()).distance(Long.MAX_VALUE, DistanceUnit.KILOMETERS)
                    .point(esGeoPointDto.getLatitude().doubleValue(), esGeoPointDto.getLongitude().doubleValue()).geoDistance(GeoDistance.PLANE);
            sourceBuilder.query(geoQuery);
        }
        //普通字段的查询匹配
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (EsDataQueryDto esDataQueryDto : esDataQueryDtoList) {
            //字段名
            String paramName = esDataQueryDto.getParamName();
            //字段值
            Object paramValue = esDataQueryDto.getParamValue();
            //日期类型的查询 开始时间
            Date startTime = esDataQueryDto.getStartTime();
            //日期类型的查询 结束时间
            Date endTime = esDataQueryDto.getEndTime();
            //是否使用分词查询
            boolean analyse = esDataQueryDto.isAnalyse();
            
            if (Objects.nonNull(paramValue)) {
                //如果参数的值是集合类型，相当于Mysql的in查询
                if (paramValue instanceof Collection) {
                    //如果分词查询
                    if (analyse) {
                        //构造查询条件
                        BoolQueryBuilder builds = QueryBuilders.boolQuery();
                        Collection<?> collection = (Collection<?>)paramValue;
                        for (Object value : collection) {
                            builds.should(QueryBuilders.matchQuery(paramName, value));
                        }
                        boolQuery.must(builds);
                    }else {
                        //如果是精确查询
                        QueryBuilder builds = QueryBuilders.termsQuery(paramName, (Collection<?>)paramValue);
                        boolQuery.must(builds);
                    }
                }else {
                    //如果参数的值是单个对象，相当于Mysql的=查询
                    QueryBuilder builds;
                    if (analyse) {
                        builds = QueryBuilders.matchQuery(paramName, paramValue);
                    } else {
                        builds = QueryBuilders.termQuery(paramName, paramValue);
                    }
                    boolQuery.must(builds);
                }
            }
            //日期类型的查询
            if (Objects.nonNull(startTime) || Objects.nonNull(endTime)) {
                //构建查询条件
                QueryBuilder builds = QueryBuilders.rangeQuery(paramName)
                        .from(startTime).to(endTime).includeLower(true);
                boolQuery.must(builds);
            }
        }
        sourceBuilder.trackTotalHits(true);
        sourceBuilder.query(boolQuery);
        return sourceBuilder;
    }
    
    /**
     * 执行查询
     * */
    public <T> void executeQuery(String indexName, String indexType,List<T> list,PageInfo<T> pageInfo,Class<T> clazz, 
                                 SearchSourceBuilder sourceBuilder,List<String> highLightFieldNameList) throws IOException {
        String string = sourceBuilder.toString();
        //设置请求头的操作类型，为json
        HttpEntity entity = new NStringEntity(string, ContentType.APPLICATION_JSON);
        StringBuilder endpointStringBuilder = new StringBuilder("/" + indexName);
        //如果开启type，则进行拼接
        if (esTypeSwitch) {
            endpointStringBuilder.append("/").append(indexType).append("/_search");
        }else {
            //没有开启type
            endpointStringBuilder.append("/_search");
        }
        String endpoint = endpointStringBuilder.toString();
        log.info("query execute query dsl : {}",string);
        Request request = new Request("POST",endpoint);
        request.setEntity(entity);
        request.addParameters(Collections.emptyMap());
        //执行请求
        Response response = restClient.performRequest(request);
        String result = EntityUtils.toString(response.getEntity());
        if (StringUtil.isEmpty(result)) {
            return;
        }
        //解析结果
        JSONObject resultJsonObject =  JSONObject.parseObject(result);
        if (Objects.isNull(resultJsonObject)) {
            return;
        }
        JSONObject hits = resultJsonObject.getJSONObject("hits");
        if (Objects.isNull(hits)) {
            return;
        }
        // 总条数
        Long value = null;
        if (esTypeSwitch) {
            value = hits.getLong("total");
        }else {
            JSONObject totalJsonObject = hits.getJSONObject("total");
            if (Objects.nonNull(totalJsonObject)) {
                value = totalJsonObject.getLong("value");
            }
        }
        if (Objects.nonNull(pageInfo) && Objects.nonNull(value)) {
            pageInfo.setTotal(value);
        }
        //解析数据
        JSONArray arrayData = hits.getJSONArray("hits");
        if (Objects.isNull(arrayData) || arrayData.isEmpty()) {
            return;
        }
        for (int i = 0, size = arrayData.size(); i < size; i++) {
            JSONObject data = arrayData.getJSONObject(i);
            if (Objects.isNull(data)) {
                continue;
            }
            //文档id
            String esId = data.getString("_id");
            //数据
            JSONObject jsonObject = data.getJSONObject("_source");
            //排序字段
            JSONArray jsonArray = data.getJSONArray("sort");
            if (Objects.nonNull(jsonArray) && !jsonArray.isEmpty()) {
                Long sort = jsonArray.getLong(0);
                jsonObject.put("sort",sort);
            }
            //高亮显示字段
            JSONObject highlight = data.getJSONObject("highlight");
            if (Objects.nonNull(highlight) && Objects.nonNull(highLightFieldNameList)) {
                for (String highLightFieldName : highLightFieldNameList) {
                    JSONArray highLightFieldValue = highlight.getJSONArray(highLightFieldName);
                    if (Objects.isNull(highLightFieldValue) || highLightFieldValue.isEmpty()) {
                        continue;
                    }
                    jsonObject.put(highLightFieldName,highLightFieldValue.get(0));
                }
            }
            //设置文档id
            if (StringUtil.isNotEmpty(esId)) {
                jsonObject.put("esId",esId);
            }
            list.add(JSONObject.parseObject(jsonObject.toJSONString(),clazz));
        }
    }
    /**
     * 根据文档id删除数据
     * */
    public void deleteByDocumentId(String index,String documentId) {
        if (!esSwitch) {
            return;
        }
        try {
            Request request = new Request("DELETE", "/" + index + "/_doc/" + documentId);
            request.addParameters(Collections.<String, String>emptyMap());
            Response response = restClient.performRequest(request);
            log.info("deleteByDocumentId result : {}",response.getStatusLine().getReasonPhrase());
        }catch (Exception e) {
            log.error("deleteData error",e);
        }
    }
}
```



### 能够解决的问题


本组件提供了多种封装的方法，包括索引的创建和创建、数据的添加、数据的查询、数据的分页查询、构造查询条件、执行查询解析结果、数据的删除



在数据查询和分页查询方法中，可以传入多个字段名和字段值进行查询，但要注意 **只能查询嵌套一层的条件查询**，如果是多条件嵌套了多层，那么就不能使用封装的查询方法了，只能自己来使用 **QueryBuilders**，来构造出查询条件，然后再用封装的**executeQuery**方法，来执行查询和解析结果。在节目服务的搜索功能中，就是这么做的



### 执行的语句


在执行api时，将真正执行dsl语句打印了出来，这里以操作节目为例，将语句贴出来，让小伙伴更加的理解对Elasticsearch操作的特点



#### 创建节目索引的dsl


```json
{
	"mappings": {
		"properties": {
			"id": {
				"type": "long"
			},
			"title": {
				"type": "text",
				"fields": {
					"keyword": {
						"ignore_above": 256,
						"type": "keyword"
					}
				}
			},
			"actor": {
				"type": "text",
				"fields": {
					"keyword": {
						"ignore_above": 256,
						"type": "keyword"
					}
				}
			},
			"place": {
				"type": "text",
				"fields": {
					"keyword": {
						"ignore_above": 256,
						"type": "keyword"
					}
				}
			},
			"itemPicture": {
				"type": "text",
				"fields": {
					"keyword": {
						"ignore_above": 256,
						"type": "keyword"
					}
				}
			},
			"areaId": {
				"type": "long"
			},
			"areaName": {
				"type": "text",
				"fields": {
					"keyword": {
						"ignore_above": 256,
						"type": "keyword"
					}
				}
			},
			"programCategoryId": {
				"type": "long"
			},
			"parentProgramCategoryId": {
				"type": "long"
			},
			"programCategoryName": {
				"type": "text",
				"fields": {
					"keyword": {
						"ignore_above": 256,
						"type": "keyword"
					}
				}
			},
			"showTime": {
				"type": "date"
			},
			"showDayTime": {
				"type": "date"
			},
			"showWeekTime": {
				"type": "text",
				"fields": {
					"keyword": {
						"ignore_above": 256,
						"type": "keyword"
					}
				}
			},
			"minPrice": {
				"type": "integer"
			},
			"maxPrice": {
				"type": "integer"
			}
		}
	},
	"settings": {
		"number_of_shards": 3,
		"number_of_replicas": 1
	}
}
```



#### 添加节目数据的dsl


```json
{
          "showWeekTime":  "周三",
          "parentProgramCategoryId":  1,
          "showDayTime":  1710259200000,
          "showTime":  1710331200000,
          "title":  "韦礼安「一直都在」音乐会",
          "actor":  "韦礼安",
          "areaId":  2,
          "itemPicture":  "img.alicdn.com/bao/uploaded/i4/2251059038/O1CN01ktjGjk2GdSYcofEUT_!!2-item_pic.png_q60.jpg_.webp",
          "minPrice":  288,
          "programCategoryId":  1,
          "id":  2,
          "place":  "JDG英特尔电子竞技中心",
          "maxPrice":  488,
          "programCategoryName":  "演唱会"
}
```



#### 查询节目列表数据的dsl


```json
{
	"from": 0,
	"size": 10,
	"query": {
		"bool": {
			"must": [{
				"term": {
					"areaId": {
						"value": 2,
						"boost": 1.0
					}
				}
			}, {
				"terms": {
					"parentProgramCategoryId": [1],
					"boost": 1.0
				}
			}],
			"adjust_pure_negative": true,
			"boost": 1.0
		}
	},
	"track_total_hits": 2147483647
}
```



#### 查询后的结果


```json
{
  "took": 0,
  "timed_out": false,
  "_shards": {
    "total": 3,
    "successful": 3,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 3,
      "relation": "eq"
    },
    "max_score": 2,
    "hits": [
      {
        "_index": "damai-program",
        "_id": "RUcS_40BBcQ0HdIV2_Ze",
        "_score": 2,
        "_source": {
          "showWeekTime": "周三",
          "parentProgramCategoryId": 1,
          "showDayTime": 1710259200000,
          "showTime": 1710331200000,
          "title": "韦礼安「一直都在」音乐会",
          "actor": "韦礼安",
          "areaId": 2,
          "itemPicture": "img.alicdn.com/bao/uploaded/i4/2251059038/O1CN01ktjGjk2GdSYcofEUT_!!2-item_pic.png_q60.jpg_.webp",
          "minPrice": 288,
          "programCategoryId": 1,
          "id": 2,
          "place": "JDG英特尔电子竞技中心",
          "maxPrice": 488,
          "programCategoryName": "演唱会"
        }
      },
      {
        "_index": "damai-program",
        "_id": "RkcS_40BBcQ0HdIV2_am",
        "_score": 2,
        "_source": {
          "showWeekTime": "周四",
          "parentProgramCategoryId": 1,
          "showDayTime": 1710950400000,
          "showTime": 1711022400000,
          "title": "魏嘉莹&Arrow Band 喜欢我吧 2023初秋巡回",
          "actor": "魏嘉莹",
          "areaId": 2,
          "itemPicture": "img.alicdn.com/bao/uploaded/https://img.alicdn.com/imgextra/i1/2251059038/O1CN01MX41P32GdSYP98YBp_!!2251059038.jpg_q60.jpg_.webp",
          "minPrice": 120,
          "programCategoryId": 1,
          "id": 1,
          "place": "朝阳剧场",
          "maxPrice": 180,
          "programCategoryName": "演唱会"
        }
      },
      {
        "_index": "damai-program",
        "_id": "R0cS_40BBcQ0HdIV2_by",
        "_score": 2,
        "_source": {
          "showWeekTime": "周四",
          "parentProgramCategoryId": 1,
          "showDayTime": 1710345600000,
          "showTime": 1710415800000,
          "title": "冬季恋歌—《请回答1988》韩剧主题曲演唱会",
          "actor": "冬季恋歌",
          "areaId": 2,
          "itemPicture": "img.alicdn.com/bao/uploaded/https://img.alicdn.com/imgextra/i2/2251059038/O1CN01tEJkEK2GdSYJ19cF5_!!2251059038.jpg_q60.jpg_.webp",
          "minPrice": 108,
          "programCategoryId": 1,
          "id": 3,
          "place": "秦乐宫剧院",
          "maxPrice": 338,
          "programCategoryName": "演唱会"
        }
      }
    ]
  }
}
```



> 更新: 2024-07-15 18:08:34  
> 原文: <https://www.yuque.com/u22210564/ykdrdh/ti6u5ovgfuhftlum>