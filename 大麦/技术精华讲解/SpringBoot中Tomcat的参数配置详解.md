# SpringBoot中Tomcat的参数配置详解

# tomcat的配置参数
```yaml
server:
  tomcat:
    threads:
      # 最大线程数(默认200)
      max: 200
      # 最小工作空闲线程数(默认10)
      min-spare: 10
    # 最大连接数  (默认8192)
    max-connections: 10000
    # 等待队列长度，默认(100)
    accept-count: 100
```

## server.tomcat.threads.max(最大线程数)
```java
/**
 * Maximum amount of worker threads.
 */
private int max = 200;
```

当每一个请求来到服务中，tomcat都会创建一个线程来对此请求进行处理，此参数就是规定服务能够同时处理多个少请求，默认参数为 200，要根据机器的不同配置来配置不同的数量，因为线程是有成本，包括线程的上下文切换和内存的消耗

## server.tomcat.max-connections(最大连接数)
```java
/**
 * Maximum number of connections that the server accepts and processes at any
 * given time. Once the limit has been reached, the operating system may still
 * accept connections based on the "acceptCount" property.
 */
private int maxConnections = 8192;
```

服务器在<font style="color:rgb(77, 77, 77);">一瞬间</font>接受和处理的最大连接数。(默认为8192)达到限制后，操作系统仍可能接受基于“acceptCount”属性的连接

## server.tomcat.threads.accept-count(等待队列长度)
```java
/**
 * Maximum queue length for incoming connection requests when all possible request
 * processing threads are in use.
 */
private int acceptCount = 100;
```

当所有可能的请求处理线程都在使用时，传入连接请求的最大队列长度。(默认值为100)





HTTP请求数达到tomcat的最大线程数时，还有新的HTTP请求到来，这时tomcat会将该请求放在等待队列中，这个acceptCount就是指能够接受的最大等待数，默认100。如果等待队列也被放满了，这个时候再来新的请求就会被tomcat拒绝（connection refused）。

## 三者之间的关系
+ 当请求到达tomcat时，tomcat会创建线程来处理请求，直到达到 server.tomcat.threads.max(最大线程数) 的规定，就不再创建
+ 当连接数达到server.tomcat.max-connections(最大连接数)后，系统会继续接收连接，在队列中进行排队，
+ 队列的容量由 server.tomcat.threads.accept-count(等待队列长度)决定，当队列满了后，后续的请求都会被拒绝



> 更新: 2024-06-18 12:06:59  
> 原文: <https://www.yuque.com/u22210564/ykdrdh/ggr2z0nygrsggqg8>