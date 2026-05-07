# 技术精华-到底为什么要使用自动装配？而不是@Configuration

# 场景描述


`SpringBoot`自动装配的好处到底优势到底在哪里？直接用@Configuration注解加在配置类上，也一样的能加载Bean，就连复杂的`@Conditional...`这些的注解也都支持。  
但`Springboot`为什么一定要这么费事，在服务启动后，还要通过扫描`spring.factories`文件中的`EnableAutoConfiguration`指定下的配置类，然后去加载这些配置类呢？  
看完本文后，会给你一个清晰的答案



# 解答


1. `@Configuration`要求在自动配置扫描范围下才能生效，默认是`SpringBoot`启动类所在的包以及子包范围，也可以自己指定。然后我们要设计一个组件给其他部门使用，如果每个部门的包命令是不同的话，就没法确定`@Configuration`是不是在扫描范围内，所以就要利用自动装配来加载这个配置类。
2. 有时我们要实现复杂的需求，例如说，我们要在服务启动时排除掉这个配置类，例如：



```plain
org.springframework.boot.autoconfigure.EnableAutoConfiguration=\
  com.example.test.TestConfig2
```



```java
public class TestConfig2 {
    
    public TestConfig2(){
        System.out.println("====TestConfig2====");
    }
    
    @Bean
    public Base base(){
        System.out.println("====base2====");
        return new Base(2);
    }
}
```



```java
@SpringBootApplication(exclude = {TestConfig2.class})
public class ServerCaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServerCaseApplication.class, args);
    }

}
```



这样利用springboot的自动装配是可以实现这个功能。那么直接用@Configuration试试呢？



```java
@Configuration
public class TestConfig2 {
    
    public TestConfig2(){
        System.out.println("====TestConfig2====");
    }
    
    @Bean
    public Base base(){
        System.out.println("====base2====");
        return new Base(2);
    }
}
```



```java
@SpringBootApplication(exclude = {TestConfig2.class})
public class ServerCaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServerCaseApplication.class, args);
    }

}
```



结果：



```plain
java.lang.IllegalStateException: The following classes could not be excluded because they are not auto-configuration classes:
	- com.example.test.TestConfig2
```



可以看到服务启动后是报错的，大概意思是此配置类无法被排除，因为不是一个自动装配的配置类。



有的人可能会觉得谁没事排除配置类啊，但确实有这种情况。比如说之前的服务是在`eureka`上注册，但后来别的部门要将服务注册到`nacos`上，但代码都是用一套的，可以通过配置项如`spring.register = eureka/nacos`来指定是在`eureka`或者`nacos`上注册。如果在`eureka`上注册，就要将`nacos`相关的自动装配全部排除掉。反过来，要在`nacos`上注册，就要把`eureka`的自动装配排除掉。这些框架里面的配置类全都是用自动装配实现的，如果框架只是用`@Configuration`来修饰，那这个功能根本实现不了。



类似于这种功能我做过很多，还有需要指定不同配置类的加载顺序啦，这些都需要自动装配才能实现。



那么为什么用@Configuration修饰就会报错呢？下面来详细的分析一波



# 分析


分析整个链路太过复杂，本文只会在重点部分介绍，详细的SpringBoot分析，可以看到本人的相关系列文章



#### ConfigurationClassPostProcessor


`ConfigurationClassPostProcessor`实现了`BeanDefinitionRegistryPostProcessor`接口的`postProcessBeanDefinitionRegistry`方法，此方法作用就是向容器中加载bean配置。



#### postProcessBeanDefinitionRegistry


```java
public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
	int registryId = System.identityHashCode(registry);
	if (this.registriesPostProcessed.contains(registryId)) {
		throw new IllegalStateException(
				"postProcessBeanDefinitionRegistry already called on this post-processor against " + registry);
	}
	if (this.factoriesPostProcessed.contains(registryId)) {
		throw new IllegalStateException(
				"postProcessBeanFactory already called on this post-processor against " + registry);
	}
	this.registriesPostProcessed.add(registryId);

	processConfigBeanDefinitions(registry);
}
```



`processConfigBeanDefinitions`是重点，此方法非常复杂，我们只分析本文重点



```java
public void processConfigBeanDefinitions(BeanDefinitionRegistry registry) {
	List<BeanDefinitionHolder> configCandidates = new ArrayList<>();
	//循环BeanDefinition的元数据对象
	String[] candidateNames = registry.getBeanDefinitionNames();
	for (String beanName : candidateNames) {
		BeanDefinition beanDef = registry.getBeanDefinition(beanName);
		if (beanDef.getAttribute(ConfigurationClassUtils.CONFIGURATION_CLASS_ATTRIBUTE) != null) {
			if (logger.isDebugEnabled()) {
				logger.debug("Bean definition has already been processed as a configuration class: " + beanDef);
			}
		}
		//如果是@Configuration修饰的类，会加入configCandidates中，当beanName为serverCaseApplication时，if条件为true
		else if (ConfigurationClassUtils.checkConfigurationClassCandidate(beanDef, this.metadataReaderFactory)) {
			configCandidates.add(new BeanDefinitionHolder(beanDef, beanName));
		}
	}

	// configCandidates为空的话，直接返回
	if (configCandidates.isEmpty()) {
		return;
	}
	//省略
	
	// 解析@Configuration修饰的类，也就是serverCaseApplication
	ConfigurationClassParser parser = new ConfigurationClassParser(
			this.metadataReaderFactory, this.problemReporter, this.environment,
			this.resourceLoader, this.componentScanBeanNameGenerator, registry);

	Set<BeanDefinitionHolder> candidates = new LinkedHashSet<>(configCandidates);
	Set<ConfigurationClass> alreadyParsed = new HashSet<>(configCandidates.size());
	do {
		StartupStep processConfig = this.applicationStartup.start("spring.context.config-classes.parse");
		//重点，进入分析
		parser.parse(candidates);
		parser.validate();

		Set<ConfigurationClass> configClasses = new LinkedHashSet<>(parser.getConfigurationClasses());
		configClasses.removeAll(alreadyParsed);

		//省略
	}
	while (!candidates.isEmpty());

	//省略
}
```



可以看到是获取到`serverCaseApplication`的元数据后对其进行解析，`parser.parse(candidates);`中的方法及其复杂，我们直接调到`AutoConfigurationImportSelector#getAutoConfigurationEntry`方法，此方法是关键



#### AutoConfigurationImportSelector#getAutoConfigurationEntry


```java
protected AutoConfigurationEntry getAutoConfigurationEntry(AnnotationMetadata annotationMetadata) {
	if (!isEnabled(annotationMetadata)) {
		return EMPTY_ENTRY;
	}
	AnnotationAttributes attributes = getAttributes(annotationMetadata);
	//重点！！！ configurations就是自动装配扫描的类集合
	List<String> configurations = getCandidateConfigurations(annotationMetadata, attributes);
	configurations = removeDuplicates(configurations);
	//重点！！！ exclusions就是设置要排除的类，案例为com.example.test.TestConfig2
	Set<String> exclusions = getExclusions(annotationMetadata, attributes);
	//重点！！！这里就是要检查设置要排除的类是不是自动装配中的配置类
	checkExcludedClasses(configurations, exclusions);
	configurations.removeAll(exclusions);
	configurations = getConfigurationClassFilter().filter(configurations);
	fireAutoConfigurationImportEvents(configurations, exclusions);
	return new AutoConfigurationEntry(configurations, exclusions);
}
```



#### getCandidateConfigurations


自动装配查找，从META-INF/spring.factories文件中获取



```java
protected List<String> getCandidateConfigurations(AnnotationMetadata metadata, AnnotationAttributes attributes) {
	List<String> configurations = SpringFactoriesLoader.loadFactoryNames(getSpringFactoriesLoaderFactoryClass(),
			getBeanClassLoader());
	Assert.notEmpty(configurations, "No auto configuration classes found in META-INF/spring.factories. If you "
			+ "are using a custom packaging, make sure that file is correct.");
	return configurations;
}
```



#### getExclusions


查找需要进行排除的自动装配的配置类



```java
protected Set<String> getExclusions(AnnotationMetadata metadata, AnnotationAttributes attributes) {
	Set<String> excluded = new LinkedHashSet<>();
	excluded.addAll(asList(attributes, "exclude"));
	excluded.addAll(asList(attributes, "excludeName"));
	excluded.addAll(getExcludeAutoConfigurationsProperty());
	return excluded;
}
```



### checkExcludedClasses


```java
private void checkExcludedClasses(List<String> configurations, Set<String> exclusions) {
	List<String> invalidExcludes = new ArrayList<>(exclusions.size());
	for (String exclusion : exclusions) {
		//configurations.contains(exclusion)就是判断要排除的配置类是不是在自动装配的配置类集合中
		//案例中的com.example.test.TestConfig2没有在此configurations中，所以此if条件的结果为true
		if (ClassUtils.isPresent(exclusion, getClass().getClassLoader()) && !configurations.contains(exclusion)) {
			invalidExcludes.add(exclusion);
		}
	}
	//这步if为true
	if (!invalidExcludes.isEmpty()) {
		handleInvalidExcludes(invalidExcludes);
	}
}
```



#### handleInvalidExcludes


```java
protected void handleInvalidExcludes(List<String> invalidExcludes) {
	StringBuilder message = new StringBuilder();
	for (String exclude : invalidExcludes) {
		message.append("\t- ").append(exclude).append(String.format("%n"));
	}
	throw new IllegalStateException(String.format(
			"The following classes could not be excluded because they are not auto-configuration classes:%n%s",
			message));
}
```



可以看到这里抛出了异常，就是我们最开始案例中的错误信息



> 更新: 2023-12-26 13:50:31  
> 原文: <https://www.yuque.com/u22210564/ykdrdh/nbz9x32povuvwezr>