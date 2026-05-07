# 业务讲解-Gateway处理请求的流程

## 简介


本文主要介绍业务是如何对请求进行参数解密、验证签名、验证是否登录、解密后请求体传递给后续服务、生成服务链路id等功能



## 处理请求


**位置：** com.damai.filter.RequestValidationFilter



```java
@Component
@Slf4j
public class RequestValidationFilter implements GlobalFilter, Ordered {

    @Autowired
    private ServerCodecConfigurer serverCodecConfigurer;

    @Autowired
    private ChannelDataService channelDataService;

    @Autowired
    private ApiRestrictService apiRestrictService;

    @Autowired
    private TokenService tokenService;

    @Autowired
    private GatewayProperty gatewayProperty;
    
    @Autowired
    private UidGenerator uidGenerator;
    
    @Autowired
    private RateLimiterProperty rateLimiterProperty;
    
    @Autowired
    private RateLimiter rateLimiter;
    

    @Override
    public Mono<Void> filter(final ServerWebExchange exchange, final GatewayFilterChain chain) {
        if (rateLimiterProperty.getRateSwitch()) {
            try {
                rateLimiter.acquire();
                return doFilter(exchange,chain);
            } catch (InterruptedException e) {
                log.error("interrupted error",e);
                throw new DaMaiFrameException(BaseCode.THREAD_INTERRUPTED);
            } finally {
                rateLimiter.release();
            }
        }else{
            return doFilter(exchange, chain);
        }
    }
    
    public Mono<Void> doFilter(final ServerWebExchange exchange, final GatewayFilterChain chain){
        ServerHttpRequest request = exchange.getRequest();
        //链路id
        String traceId = request.getHeaders().getFirst(TRACE_ID);
        //灰度标识
        String gray = request.getHeaders().getFirst(GRAY_PARAMETER);
        //是否验证参数
        String noVerify = request.getHeaders().getFirst(NO_VERIFY);
        //如果链路id不存在，那么在这里生成
        if (StringUtil.isEmpty(traceId)) {
            traceId = String.valueOf(uidGenerator.getUid());
        }
        //将链路id放到日志的MDC中便于日志输出
        MDC.put(TRACE_ID,traceId);
        Map<String,String> headMap = new HashMap<>(8);
        headMap.put(TRACE_ID,traceId);
        headMap.put(GRAY_PARAMETER,gray);
        if (StringUtil.isNotEmpty(noVerify)) {
            headMap.put(NO_VERIFY,noVerify);
        }
        //将链路id放到ThreadLocal中
        BaseParameterHolder.setParameter(TRACE_ID,traceId);
        //将灰度标识放到ThreadLocal中
        BaseParameterHolder.setParameter(GRAY_PARAMETER,gray);
        //获取请求类型
        MediaType contentType = request.getHeaders().getContentType();
        //application json请求
        if (Objects.nonNull(contentType) && contentType.toString().toLowerCase().contains(MediaType.APPLICATION_JSON_VALUE.toLowerCase())) {
            //如果是json则进行参数验证
            return readBody(exchange,chain,headMap);
        }else {
            //如果不是json请求，则直接执行
            Map<String, String> map = doExecute("", exchange);
            map.remove(REQUEST_BODY);
            map.putAll(headMap);
            request.mutate().headers(httpHeaders -> {
                map.forEach(httpHeaders::add);
            });
            return chain.filter(exchange);
        }
    }
    
    /**
     * 此方法是根据源码进行修改为了能读取请求体并修改，不是重点，可忽略
     * */
    private Mono<Void> readBody(ServerWebExchange exchange, GatewayFilterChain chain, Map<String,String> headMap){
        log.info("current thread readBody : {}",Thread.currentThread().getName());
        RequestTemporaryWrapper requestTemporaryWrapper = new RequestTemporaryWrapper();
        
        ServerRequest serverRequest = ServerRequest.create(exchange, serverCodecConfigurer.getReaders());
        Mono<String> modifiedBody = serverRequest
                .bodyToMono(String.class)
                //execute是执行参数验证的方法
                .flatMap(originalBody -> Mono.just(execute(requestTemporaryWrapper,originalBody,exchange)))
                //这个方法是使用post请求，方式也是json，但请求体为空的情况
                .switchIfEmpty(Mono.defer(() -> Mono.just(execute(requestTemporaryWrapper,"",exchange))));
        
        BodyInserter bodyInserter = BodyInserters.fromPublisher(modifiedBody, String.class);
        HttpHeaders headers = new HttpHeaders();
        headers.putAll(exchange.getRequest().getHeaders());
        headers.remove(HttpHeaders.CONTENT_LENGTH);
        
        CachedBodyOutputMessage outputMessage = new CachedBodyOutputMessage(exchange, headers);
        return bodyInserter
                .insert(outputMessage, new BodyInserterContext())
                .then(Mono.defer(() -> chain.filter(
                        exchange.mutate().request(decorateHead(exchange, headers, outputMessage, requestTemporaryWrapper, headMap)).build()
                )))
                .onErrorResume((Function<Throwable, Mono<Void>>) throwable -> Mono.error(throwable));
    }
    
    public String execute(RequestTemporaryWrapper requestTemporaryWrapper,String requestBody,ServerWebExchange exchange){
        //进行业务验证，并将相关参数放入map
        Map<String, String> map = doExecute(requestBody, exchange);
        //这里的map中的数据在doExecute中放入的，有修改后的请求体和要放在请求头中的数据，先拿出请求体用来返回，然后从map中移除，
        //这样map剩下的数据就都是要放入请求头中的了
        String body = map.get(REQUEST_BODY);
        map.remove(REQUEST_BODY);
        requestTemporaryWrapper.setMap(map);
        return body;
    }
    /**
     * 具体进行参数验证的逻辑
     * */
    private Map<String,String> doExecute(String originalBody,ServerWebExchange exchange){
        log.info("current thread verify: {}",Thread.currentThread().getName());
        ServerHttpRequest request = exchange.getRequest();
        //得到请求体
        String requestBody = originalBody;
        Map<String, String> bodyContent = new HashMap<>(32);
        if (StringUtil.isNotEmpty(originalBody)) {
            //请求体转为map结构
            bodyContent = JSON.parseObject(originalBody, Map.class);
        }
        //基础参数code渠道
        String code = null;
        //用户的token
        String token;
        //用户的userId   
        String userId = null;
        //请求的路径
        String url = request.getPath().value();
        //是否跳过参数验证的标识
        String noVerify = request.getHeaders().getFirst(NO_VERIFY);
        //是否允许跳过参数验证
        boolean allowNormalAccess = gatewayProperty.isAllowNormalAccess();
        if ((!allowNormalAccess) && (VERIFY_VALUE.equals(noVerify))) {
            throw new DaMaiFrameException(BaseCode.ONLY_SIGNATURE_ACCESS_IS_ALLOWED);
        }
        //是否跳过参数验证
        if (checkParameter(originalBody,noVerify) && !skipCheckParameter(url)) {

            String encrypt = request.getHeaders().getFirst(ENCRYPT);
            //应用渠道
            code = bodyContent.get(CODE);
            //token
            token = request.getHeaders().getFirst(TOKEN);
            //验证code参数并获取基础参数
            GetChannelDataVo channelDataVo = channelDataService.getChannelDataByCode(code);
            //如果v2版本就要先对参数进行解密
            if (StringUtil.isNotEmpty(encrypt) && V2.equals(encrypt)) {
                //使用rsa私钥进行解密
                String decrypt = RsaTool.decrypt(bodyContent.get(BUSINESS_BODY),channelDataVo.getDataSecretKey());
                //将解密后的请求体替换加密的请求体
                bodyContent.put(BUSINESS_BODY,decrypt);
            }
            //进行签名验证
            boolean checkFlag = RsaSignTool.verifyRsaSign256(bodyContent, channelDataVo.getSignPublicKey());
            if (!checkFlag) {
                throw new DaMaiFrameException(BaseCode.RSA_SIGN_ERROR);
            }
            //判断是否跳过验证登录的token
            //默认注册和登录接口跳过验证
            boolean skipCheckTokenResult = skipCheckToken(url);
            if (!skipCheckTokenResult && StringUtil.isEmpty(token)) {
                ArgumentError argumentError = new ArgumentError();
                argumentError.setArgumentName(token);
                argumentError.setMessage("token参数为空");
                List<ArgumentError> argumentErrorList = new ArrayList<>();
                argumentErrorList.add(argumentError);
                throw new ArgumentException(BaseCode.ARGUMENT_EMPTY.getCode(),argumentErrorList);
            }
            //获取用户id
            if (!skipCheckTokenResult) {
                UserVo userVo = tokenService.getUser(token,code,channelDataVo.getTokenSecret());
                userId = userVo.getId();
            }
            //如果上一步没有获取到用户id，并且此url在有token情况下还需要解析出userid，
            //那么就再解析一遍token
            if (StringUtil.isEmpty(userId) && checkNeedUserId(url) && StringUtil.isNotEmpty(token)) {
                UserVo userVo = tokenService.getUser(token,code,channelDataVo.getTokenSecret());
                userId = userVo.getId();
            }
            
            requestBody = bodyContent.get(BUSINESS_BODY);
        }
        //根据规则对api接口进行防刷限制
        apiRestrictService.apiRestrict(userId,url,request);
        //将修改后的请求体和要传递的请求头参数放入map
        Map<String,String> map = new HashMap<>(4);
        map.put(REQUEST_BODY,requestBody);
        if (StringUtil.isNotEmpty(code)) {
            map.put(CODE,code);
        }
        if (StringUtil.isNotEmpty(userId)) {
            map.put(USER_ID,userId);
        }
        return map;
    }
    /**
     * 将网关层request请求头中的重要参数传递给后续的微服务中
     * 此方法为Gateway源码部分，可忽略
     */
    private ServerHttpRequestDecorator decorateHead(ServerWebExchange exchange, HttpHeaders headers, CachedBodyOutputMessage outputMessage, RequestTemporaryWrapper requestTemporaryWrapper, Map<String,String> headMap){
        return new ServerHttpRequestDecorator(exchange.getRequest()){
            @Override
            public HttpHeaders getHeaders() {
                log.info("current thread getHeaders: {}",Thread.currentThread().getName());
                long contentLength = headers.getContentLength();
                HttpHeaders newHeaders = new HttpHeaders();
                newHeaders.putAll(headers);
                Map<String, String> map = requestTemporaryWrapper.getMap();
                if (CollectionUtil.isNotEmpty(map)) {
                    newHeaders.setAll(map);
                }
                if (CollectionUtil.isNotEmpty(headMap)) {
                    newHeaders.setAll(headMap);
                }
                if (contentLength > 0){
                    newHeaders.setContentLength(contentLength);
                }else {
                    newHeaders.set(HttpHeaders.TRANSFER_ENCODING,"chunked");
                }
                if (CollectionUtil.isNotEmpty(headMap) && StringUtil.isNotEmpty(headMap.get(TRACE_ID))) {
                    MDC.put(TRACE_ID,headMap.get(TRACE_ID));
                }
                return newHeaders;
            }

            @Override
            public Flux<DataBuffer> getBody() {
                return outputMessage.getBody();
            }
        };
    }
    /**
     * 指定执行顺序
     * */
    @Override
    public int getOrder() {
        return -2;
    }
    /**
     * 验证是否跳过token验证
     * */
    public boolean skipCheckToken(String url){
        for (String skipCheckTokenPath : gatewayProperty.getCheckTokenPaths()) {
            PathMatcher matcher = new AntPathMatcher();
            if (matcher.match(skipCheckTokenPath, url)) {
                return false;
            }
        }
        return true;
    }
    /**
     * 验证是否跳过参数验证
     * */
    public boolean skipCheckParameter(String url){
        for (String skipCheckTokenPath : gatewayProperty.getCheckSkipParmeterPaths()) {
            PathMatcher matcher = new AntPathMatcher();
            if (matcher.match(skipCheckTokenPath, url)) {
                return true;
            }
        }
        return false;
    }
    /**
     * 验证请求头的参数noVerify = true
     * */
    public boolean checkParameter(String originalBody,String noVerify){
        return (!(VERIFY_VALUE.equals(noVerify))) && StringUtil.isNotEmpty(originalBody);
    }
    /**
     * 验证是否需要userId
     * */
    private boolean checkNeedUserId(String url){
        for (String userIdPath : gatewayProperty.getUserIdPaths()) {
            PathMatcher matcher = new AntPathMatcher();
            if (matcher.match(userIdPath, url)) {
                return true;
            }
        }
        return false;
    }
}
```



### 总结


+ 生成链路调用id
+ 如果是json请求则进行参数验证
+ 判断是否跳过参数验证
+ 根据渠道`code`获得秘钥相关参数
+ 如果是v2加密版本则要根据RSA的私钥进行解密
+ 进行签名验证
+ 判断是否需要验证登录
+ 根据规则对api接口进行防刷限制
+ 将修改后的请求体和生成要添加请求头的数据传递给业务服务



### 链路id的生成


链路id的生成其实是使用的分布式id生成器生成的，分布式id是使用的雪花算法，根据MybatisPlus的雪花算法策略进行了改造，并且定制了workId和dataId的配置，解决了在K8s环境下会重复的问题，关于分布式id的介绍可查看相关文档



[组件讲解-分布式ID生成器揭秘，保障数据唯一性的核心组件](https://www.yuque.com/u22210564/ykdrdh/ahgwiywela7xxego)



### 参数是否验证


```java
public static final String VERIFY_VALUE = "true";

public boolean checkParameter(String originalBody,String noVerify){
    return (!(VERIFY_VALUE.equals(noVerify))) && StringUtil.isNotEmpty(originalBody);
}
```



### 根据渠道code获取秘钥


**com.damai.service.ChannelDataService#getChannelDataByCode**



```java
public GetChannelDataVo getChannelDataByCode(String code){
    //验证code是否为空
    checkCode(code);
    //从redis查询
    GetChannelDataVo channelDataVo = getChannelDataByRedis(code);
    if (Objects.isNull(channelDataVo)) {
        //调用基础服务查询
        channelDataVo = getChannelDataByClient(code);
        //放到redis中
        setChannelDataRedis(code,channelDataVo);
    }
    return channelDataVo;
}
```



```java
public void checkCode(String code){
    if (StringUtil.isEmpty(code)) {
        ArgumentError argumentError = new ArgumentError();
        argumentError.setArgumentName(CODE);
        argumentError.setMessage(EXCEPTION_MESSAGE);
        List<ArgumentError> argumentErrorList = new ArrayList<>();
        argumentErrorList.add(argumentError);
        throw new ArgumentException(BaseCode.ARGUMENT_EMPTY.getCode(),argumentErrorList);
    }
}
```



```java
private GetChannelDataVo getChannelDataByRedis(String code){
    return redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.CHANNEL_DATA,code),GetChannelDataVo.class);
}
```



```java
private GetChannelDataVo getChannelDataByClient(String code){
    GetChannelDataByCodeDto getChannelDataByCodeDto = new GetChannelDataByCodeDto();
    getChannelDataByCodeDto.setCode(code);
    ApiResponse<GetChannelDataVo> getChannelDataApiResponse = baseDataClient.getByCode(getChannelDataByCodeDto);
    if (Objects.equals(getChannelDataApiResponse.getCode(), BaseCode.SUCCESS.getCode())) {
        return getChannelDataApiResponse.getData();
    }
    throw new DaMaiFrameException("没有找到ChannelData");
}
```



### 验证登录


用户在登录后会生成token，Gateway在网管中会验证token的状态，关于token的生成过程，可查看用户的登录相关文档



[业务讲解-用户登录和退出流程解析](https://www.yuque.com/u22210564/ykdrdh/bvlg9phc26rwpsie)



接下来分析Gateway是如何进行验证token的



```java
boolean skipCheckTokenResult = skipCheckToken(url);
```



```java
public boolean skipCheckToken(String url){
    for (String skipCheckTokenPath : gatewayProperty.getCheckTokenPaths()) {
        PathMatcher matcher = new AntPathMatcher();
        if (matcher.match(skipCheckTokenPath, url)) {
            return false;
        }
    }
    return true;
}
```



```java
@Data
@Component
public class GatewayProperty {
    /**
     * 需要做频率限制的路径
     */
    @Value("${api.limit.paths:#{null}}")
    private String[] apiRestrictPaths;
    /**
    * 需要token验证的路径
    /
    @Value("${skip.check.token.paths:/**/program/order/create/v1,/**/program/order/create/v2,/**/program/order/create/v3," +
            "/**/ticket/user/add,/**/ticket/user/delete,/**/ticket/user/list,/**/user/authentication,/**/user/logout," +
            "/**/user/update,/**/user/update/email,/**/user/update/mobile,/**/user/update/password,/**/order/cancel," +
            "/**/order/create,/**/order/pay}")
    private String[] checkTokenPaths;
}
```



可以看到是可以通过`skip.check.token.paths`进行配置的，默认是用户注册、用户登录、渠道数据的接口是跳过验证的



如果是需要验证token的请求，那么根据token和密钥进行解析，获得用户



```java
if (!skipCheckTokenResult) {
    UserVo userVo = tokenService.getUser(token,code,channelDataVo.getTokenSecret());
    userId = userVo.getId();
}
```



```java
public UserVo getUser(String token,String code,String tokenSecret){
    UserVo userVo = null;
    //根据token解析获取userId
    String userId = parseToken(token,tokenSecret);
    if (StringUtil.isNotEmpty(userId)) {
        //从缓存中获取登录用户信息
        userVo = redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.USER_LOGIN, code, userId), UserVo.class);
    }
    return Optional.ofNullable(userVo).orElseThrow(() -> new DaMaiFrameException(BaseCode.USER_EMPTY));
}
```



```java
public String parseToken(String token,String tokenSecret){
    String userStr = TokenUtil.parseToken(token,tokenSecret);
    if (StringUtil.isNotEmpty(userStr)) {
        return JSONObject.parseObject(userStr).getString("userId");
    }
    return null;
}
```



### 验证是否需要userId
```java
if (StringUtil.isEmpty(userId) && checkNeedUserId(url) && StringUtil.isNotEmpty(token)) {
    UserVo userVo = tokenService.getUser(token,code,channelDataVo.getTokenSecret());
    userId = userVo.getId();
}
```



在上一步是否需要验证token是为了，有些接口必须要是登录情况下才可以。而此步骤是为了验证有些接口不需要登录的token，但如果传入token的话，就要解析userId。

什么接口需要这种需求？答案就是查看节目详情，在查看节目详情里，有个预热加载购票人信息功能，而此功能是只有在用户登录状态下，才会执行加载购票人信息。这样下单时就不需要再加载购票人了，提高下单响应速度。

在查看节目详情里，用户登录状态下，执行加载购票人信息时需要userId，而userId是Gateway传过来的，所以才需要加上此步骤。



### 根据规则对api接口进行防刷限制


此功能是可以配置来指定接口并根据指定的规则进行请求频率的验证和限制，可以精确到指定分钟单位，例如14:00 - 15:00，如果超过了规则配置的频率那么将请求直接拦截，不再下发到业务服务中，节省了流量，并将发生限制的请求记录下来，关于更详细的介绍可查看相关文档



[业务讲解-API接口定制化防刷策略实现](https://www.yuque.com/u22210564/ykdrdh/tv9a5gxrf3pqaz24)



[业务讲解-API请求定制化防刷数据存储策略](https://www.yuque.com/u22210564/ykdrdh/yq6fqk92m6vdfil5)

## 处理请求返回


**位置：** com.damai.filter.ResponseValidationFilter



```java
private String checkResponseBody(final ServerWebExchange serverWebExchange, final String responseBody) {
    //返回体
    String modifyResponseBody = responseBody;
    ServerHttpRequest request = serverWebExchange.getRequest();
    //是否要进行参数加密的标识
    String noVerify = request.getHeaders().getFirst(NO_VERIFY);
    //参数加密版本
    String encrypt = request.getHeaders().getFirst(ENCRYPT);
    //如果加密标识不为true，并且参数加密版本为v2，返回体也不为空，则将业务参数进行加密
    if ((!VERIFY_VALUE.equals(noVerify)) && "v2".equals(encrypt) && StringUtil.isNotEmpty(responseBody)) {
        ApiResponse<Object> apiResponse = JSON.parseObject(responseBody, ApiResponse.class);
        //业务数据
        Object data = apiResponse.getData();
        if (data != null) {
            //基础渠道编码
            String code = request.getHeaders().getFirst(CODE);
            //验证code
            checkCodeHandler.checkCode(code);
            //得到渠道参数
            GetChannelDataVo channelDataVo = channelDataService.getChannelDataByCode(code);
            //对业务参数进行加密
            String rsaEncrypt = RSATool.encrypt(JSON.toJSONString(data),channelDataVo.getDataPublicKey());
            apiResponse.setData(rsaEncrypt);
            modifyResponseBody = JSON.toJSONString(apiResponse);
        }
    }
    //将修改后的返回体进行返回
    return modifyResponseBody;
}
```



### 总结


+ 拿到参数加密的标识，判断是否要对参数加密
+ 如果参数加密标识不为true或者空，以及参数加密版本为v2，则将业务参数利用rsa的公钥进行加密，然后重新设置到返回体中
+ 将修改后的返回体参数进行返回



> 更新: 2025-01-03 11:34:04  
> 原文: <https://www.yuque.com/u22210564/ykdrdh/psovmckslgg6s2sy>