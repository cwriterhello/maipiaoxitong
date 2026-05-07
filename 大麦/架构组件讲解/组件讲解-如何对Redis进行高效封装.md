# 组件讲解-如何对Redis进行高效封装

# 前言


现在的项目基本都会使用redis，在Springboot中使用提供的`RedisTemplate`或者`StringRedisTemplate`，但使用感觉还是有些问题，比如在使用中还是需要自己来做对象的转换工作，还有对key键的管理，当键逐渐多了后是非常的麻烦，这有一个，那里有一个，当后续修改时候，要一个个去搜索，效率非常的低下，如果想找这个缓存是谁来设计的，还需要去看代码的提交者，更是麻烦。



针对上述的痛点能不能解决下呢？比如，把对象的转换工作也做好，使用是只要指定 **对象类型 **就可以了，以及将 **key** 做好统一的管理，并要求加上 **键的含义、键值的含义、作者 **等信息。



**对于key的管理也要注意，必须要求用户在指定的类中存放，如果不用代码强制约束的话，总会有人偷懒的**。



所以为了解决这些问题，设计出对redis操作的封装组件，使用起来更加的方便和管理



# 介绍
### 依赖


```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>damai-redis-framework</artifactId>
    <version>${revision}</version>
</dependency>
```



### 特点
+ 针对`Springboot`的`StringRedisTemplate`,做了再次封装，使用了`json`格式存放。
+ 对`键值对的操作`、`String`、`Hash`、`Set`、`ZSet`、`List`提供了封装支持。
+ 在使用过程中，存放直接存放对象类型即可，拿取只需指定`class`类型。
+ 对`key`进行了统一的管理约定，用户不能随意在代码中指定`key`值。
+ 在使用中以接口的形式对外提供api操作，目前实现只有`redis`一种。



### 使用
直接使用Spring的注入即可

```java
@Autowired
private RedisCache redisCache;
```



### key的约定说明
提供的api中，key的类型为**RedisKeyBuild**，需要使用提供的方法来转换



+  将key存放在`RedisKeyManage`枚举中 

```java
public enum RedisKeyManage {
    Key("key","键值测试","value为TestCacheDto类型","k"),
    Key2("key:%s","键值占位测试","value为TestCacheDto类型","k"),
}
```

 

+  使用**RedisKeyBuild.createRedisKey**方法传入**RedisKeyManage**类型来构建出**RedisKeyBuild**，此方法支持**String.format**占用符的使用 

```java
public final class RedisKeyBuild {
    /**
     * 实际使用的key
     * */
    private final String relKey;

    private RedisKeyBuild(String relKey) {
        this.relKey = relKey;
    }

    /**
     * 构建真实的key
     * @param redisKeyManage key的枚举
     * @param args 占位符的值
     * */
    public static RedisKeyBuild createRedisKey(RedisKeyManage redisKeyManage, Object... args){
        String redisRelKey = String.format(redisKeyManage.getKey(),args);
        return new RedisKeyBuild(SpringUtil.getPrefixDistinctionName() + "-" + redisRelKey);
    }

    public String getRelKey() {
        return relKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisKeyBuild that = (RedisKeyBuild) o;
        return relKey.equals(that.relKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relKey);
    }
}
```

 



### 特点
+ 封装的api中传入的key的类型为**RedisKeyBuild**，**RedisKeyBuild**的构造方法进行了私有化，用户就不用自己进行构建**RedisKeyBuild**
+ **RedisKeyBuild**提供了公共的静态方法**createRedisKey**，参数为**redisKeyManage**，和具体的值
+ 通过这种约束，用户只能调用**RedisKeyBuild.createRedisKey**，实现了对redis键的统一管理



## 完整使用示例
```java
/**
 * @program: redis-tool
 * @description: 缓存 key管理
 * @author: 星哥
 * @create: 2023-1-6
 **/
public enum RedisKeyEnum {
    Key("key","键值测试","value为TestCacheDto类型","lk"),
    Key2("key:%s","键值占位测试","value为TestCacheDto类型","lk"),
    CHANNEL_DATA("channel_data:%s","channel_data的key","channel_data的value","k"),
}
```



```java
@Autowired
private RedisCache redisCache;

private GetChannelDataVo getChannelDataByRedis(String code){
    return redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.CHANNEL_DATA,code),GetChannelDataVo.class);
}
```



### 提供的api
提供的api非常丰富，可在**com.damai.redis.RedisCache**查看api说明，在**com.damai.redis.RedisCacheImpl**查看具体的是实现，这里就不再贴出代码



> 更新: 2024-07-18 17:14:48  
> 原文: <https://www.yuque.com/u22210564/ykdrdh/wbgcyd7hi0dvrzs6>