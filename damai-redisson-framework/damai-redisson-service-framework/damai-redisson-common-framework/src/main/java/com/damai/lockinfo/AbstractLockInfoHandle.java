package com.damai.lockinfo;


import com.damai.core.SpringUtil;
import com.damai.parser.ExtParameterNameDiscoverer;
import com.damai.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ObjectUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static com.damai.core.Constants.SEPARATOR;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: 锁信息抽象
 * @author: 阿星不是程序员
 **/
@Slf4j
public abstract class AbstractLockInfoHandle implements LockInfoHandle {
    
    private static final String LOCK_DISTRIBUTE_ID_NAME_PREFIX = "LOCK_DISTRIBUTE_ID";

    private final ParameterNameDiscoverer nameDiscoverer = new ExtParameterNameDiscoverer();

    private final ExpressionParser parser = new SpelExpressionParser();
    
    /**
     * 锁信息前缀
     * @return 具体前缀
     * */
    protected abstract String getLockPrefixName();
    @Override
    public String getLockName(JoinPoint joinPoint,String name,String[] keys){
        return SpringUtil.getPrefixDistinctionName() + "-" + getLockPrefixName() + SEPARATOR + name + getRelKey(joinPoint, keys);
    }
    @Override
    public String simpleGetLockName(String name,String[] keys){
        List<String> definitionKeyList = new ArrayList<>();
        for (String key : keys) {
            if (StringUtil.isNotEmpty(key)) {
                definitionKeyList.add(key);
            }
        }
        return SpringUtil.getPrefixDistinctionName() + "-" + 
                LOCK_DISTRIBUTE_ID_NAME_PREFIX + SEPARATOR + name + SEPARATOR + String.join(SEPARATOR, definitionKeyList);
    }

    /**
     * 获取自定义键
     * */
    private String getRelKey(JoinPoint joinPoint, String[] keys){
        Method method = getMethod(joinPoint);
        List<String> definitionKeys = getSpElKey(keys, method, joinPoint.getArgs());
        return SEPARATOR + String.join(SEPARATOR, definitionKeys);
    }

    /**
     * 这段代码的执行流程是：
     * 先获取MethodSignature中的method
     * 检查这个方法是否来自接口
     * 如果是接口方法，则尝试获取目标类中对应的实现方法
     * 如果获取失败，仍然使用原来的接口方法
     * 这种方式确保了在各种情况下都能获取到合适的方法对象，同时避免了不必要的性能开销和异常处理。
     * */
    private Method getMethod(JoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        if (method.getDeclaringClass().isInterface()) {
            try {
                method = joinPoint.getTarget().getClass().getDeclaredMethod(signature.getName(),
                        method.getParameterTypes());
            } catch (Exception e) {
                log.error("get method error ",e);
            }
        }
        return method;
    }

    /**
     * 获取自定义键
     * @param definitionKeys 参数名
     * @param method 方法
     * @param parameterValues 参数值
     * @return 切点的方法
     * */
    private List<String> getSpElKey(String[] definitionKeys, Method method, Object[] parameterValues) {
        List<String> definitionKeyList = new ArrayList<>();
        for (String definitionKey : definitionKeys) {
            if (!ObjectUtils.isEmpty(definitionKey)) {
                //spEl的构建
                EvaluationContext context = new MethodBasedEvaluationContext(null, method, parameterValues, nameDiscoverer);
                //解析参数名来替换成真正的参数值
                Object objKey = parser.parseExpression(definitionKey).getValue(context);
                definitionKeyList.add(ObjectUtils.nullSafeToString(objKey));
            }
        }
        //将解析出的参数值拼装一起后返回
        return definitionKeyList;
    }

}
