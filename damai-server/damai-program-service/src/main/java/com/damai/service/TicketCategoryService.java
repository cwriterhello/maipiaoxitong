package com.damai.service;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import com.baidu.fsg.uid.UidGenerator;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.damai.core.RedisKeyManage;
import com.damai.dto.TicketCategoryAddDto;
import com.damai.dto.TicketCategoryDto;
import com.damai.dto.TicketCategoryListByProgramDto;
import com.damai.entity.TicketCategory;
import com.damai.mapper.TicketCategoryMapper;
import com.damai.redis.RedisCache;
import com.damai.redis.RedisKeyBuild;
import com.damai.service.cache.local.LocalCacheTicketCategory;
import com.damai.servicelock.LockType;
import com.damai.servicelock.annotion.ServiceLock;
import com.damai.util.DateUtils;
import com.damai.util.ServiceLockTool;
import com.damai.vo.TicketCategoryDetailVo;
import com.damai.vo.TicketCategoryVo;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.damai.core.DistributedLockConstants.GET_REMAIN_NUMBER_LOCK;
import static com.damai.core.DistributedLockConstants.GET_TICKET_CATEGORY_LOCK;
import static com.damai.core.DistributedLockConstants.REMAIN_NUMBER_LOCK;
import static com.damai.core.DistributedLockConstants.TICKET_CATEGORY_LOCK;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: 票档 service
 * @author: 阿星不是程序员
 **/
@Slf4j
@Service
public class TicketCategoryService extends ServiceImpl<TicketCategoryMapper, TicketCategory> {
    
    @Autowired
    private UidGenerator uidGenerator;
    
    @Autowired
    private RedisCache redisCache;
    
    @Autowired
    private TicketCategoryMapper ticketCategoryMapper;
    
    @Autowired
    private ServiceLockTool serviceLockTool;
    
    @Autowired
    private LocalCacheTicketCategory localCacheTicketCategory;
    
    /**
     * 添加票档信息
     * <p>该方法用于向系统中添加一个新的票档。主要流程包括:
     * 1. 复制票档添加DTO到实体对象
     * 2. 生成唯一ID
     * 3. 插入数据库</p>
     *
     * @param ticketCategoryAddDto 票档添加参数对象，包含节目ID、介绍、价格、总数量和剩余数量等信息
     * @return Long 票档ID
     * @throws Exception 当数据库插入操作出现异常时会抛出异常
     */
    @Transactional(rollbackFor = Exception.class)
    public Long add(TicketCategoryAddDto ticketCategoryAddDto) {
        TicketCategory ticketCategory = new TicketCategory();
        BeanUtil.copyProperties(ticketCategoryAddDto,ticketCategory);
        ticketCategory.setId(uidGenerator.getUid());
        ticketCategoryMapper.insert(ticketCategory);
        return ticketCategory.getId();
    }
    
    /**
     * 通过多级缓存获取节目票档列表
     * <p>该方法通过本地缓存获取节目票档列表，如果本地缓存未命中，则会调用selectTicketCategoryListByProgramId方法。
     * 使用了本地缓存(LocalCacheTicketCategory)来提高查询性能。</p>
     *
     * @param programId 节目ID
     * @param showTime 演出时间
     * @return List<TicketCategoryVo> 票档列表
     */
    public List<TicketCategoryVo> selectTicketCategoryListByProgramIdMultipleCache(Long programId, Date showTime){
        return localCacheTicketCategory.getCache(programId,key -> selectTicketCategoryListByProgramId(programId, 
                DateUtils.countBetweenSecond(DateUtils.now(),showTime), TimeUnit.SECONDS));
    }
    
    /**
     * 根据节目ID获取票档列表
     * <p>该方法通过分布式锁和Redis缓存获取节目票档列表。主要流程包括:
     * 1. 先从Redis缓存中查询票档列表
     * 2. 如果缓存未命中，则获取分布式锁
     * 3. 再次尝试从Redis缓存中查询(防止并发情况下重复查询数据库)
     * 4. 如果仍然未命中，则查询数据库并存入Redis缓存</p>
     *
     * @param programId 节目ID
     * @param expireTime 过期时间
     * @param timeUnit 时间单位
     * @return List<TicketCategoryVo> 票档列表
     */
    @ServiceLock(lockType= LockType.Read,name = TICKET_CATEGORY_LOCK,keys = {"#programId"})
    public List<TicketCategoryVo> selectTicketCategoryListByProgramId(Long programId,Long expireTime,TimeUnit timeUnit){
        List<TicketCategoryVo> ticketCategoryVoList = 
                redisCache.getValueIsList(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_CATEGORY_LIST, 
                        programId), TicketCategoryVo.class);
        if (CollectionUtil.isNotEmpty(ticketCategoryVoList)) {
            return ticketCategoryVoList;
        }
        RLock lock = serviceLockTool.getLock(LockType.Reentrant, GET_TICKET_CATEGORY_LOCK, 
                new String[]{String.valueOf(programId)});
        lock.lock();
        try {
            return redisCache.getValueIsList(
                    RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_CATEGORY_LIST, programId),
                    TicketCategoryVo.class,
                    () -> {
                        LambdaQueryWrapper<TicketCategory> ticketCategoryLambdaQueryWrapper =
                                Wrappers.lambdaQuery(TicketCategory.class).eq(TicketCategory::getProgramId, programId);
                        List<TicketCategory> ticketCategoryList =
                                ticketCategoryMapper.selectList(ticketCategoryLambdaQueryWrapper);
                        return ticketCategoryList.stream().map(ticketCategory -> {

                            TicketCategoryVo ticketCategoryVo = new TicketCategoryVo();
                            BeanUtil.copyProperties(ticketCategory, ticketCategoryVo);
                            return ticketCategoryVo;
                        }).collect(Collectors.toList());
                    }, expireTime, timeUnit);
        }finally {
            lock.unlock();
        }
    }
    
    /**
     * 获取Redis中的余票数量信息
     * <p>该方法通过分布式锁和Redis缓存获取指定节目和票档的余票数量信息。主要流程包括:
     * 1. 先从Redis缓存中查询余票数量信息
     * 2. 如果缓存未命中，则获取分布式锁
     * 3. 再次尝试从Redis缓存中查询(防止并发情况下重复查询数据库)
     * 4. 如果仍然未命中，则查询数据库并存入Redis缓存</p>
     *
     * @param programId 节目ID
     * @param ticketCategoryId 票档ID
     * @return Map<String, Long> 余票数量映射，key为票档ID，value为余票数量
     */
    @ServiceLock(lockType= LockType.Read,name = REMAIN_NUMBER_LOCK,keys = {"#programId","#ticketCategoryId"})
    public Map<String, Long> getRedisRemainNumberResolution(Long programId,Long ticketCategoryId){
        Map<String, Long> ticketCategoryRemainNumber =
                redisCache.getAllMapForHash(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION,
                        programId,ticketCategoryId), Long.class);
        
        if (CollectionUtil.isNotEmpty(ticketCategoryRemainNumber)) {
            return ticketCategoryRemainNumber;
        }
        RLock lock = serviceLockTool.getLock(LockType.Reentrant, GET_REMAIN_NUMBER_LOCK,
                new String[]{String.valueOf(programId),String.valueOf(ticketCategoryId)});
        lock.lock();
        try {
            ticketCategoryRemainNumber =
                    redisCache.getAllMapForHash(RedisKeyBuild.createRedisKey(
                            RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION, programId,ticketCategoryId), Long.class);
            if (CollectionUtil.isNotEmpty(ticketCategoryRemainNumber)) {
                return ticketCategoryRemainNumber;
            }
            LambdaQueryWrapper<TicketCategory> ticketCategoryLambdaQueryWrapper = Wrappers.lambdaQuery(TicketCategory.class)
                    .eq(TicketCategory::getProgramId, programId).eq(TicketCategory::getId,ticketCategoryId);
            List<TicketCategory> ticketCategoryList = ticketCategoryMapper.selectList(ticketCategoryLambdaQueryWrapper);
            Map<String, Long> map = ticketCategoryList.stream().collect(Collectors.toMap(t -> String.valueOf(t.getId()),
                    TicketCategory::getRemainNumber, (v1, v2) -> v2));
            redisCache.putHash(RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION,
                    programId,ticketCategoryId),map);
            return map;
        }finally {
            lock.unlock();
        }
    }
    
    /**
     * 获取票档详情
     * <p>该方法根据票档ID查询票档的详细信息。</p>
     *
     * @param ticketCategoryDto 票档参数对象，包含票档ID
     * @return TicketCategoryDetailVo 票档详情对象
     */
    public TicketCategoryDetailVo detail(TicketCategoryDto ticketCategoryDto) {
        TicketCategory ticketCategory = ticketCategoryMapper.selectById(ticketCategoryDto.getId());
        TicketCategoryDetailVo ticketCategoryDetailVo = new TicketCategoryDetailVo();
        BeanUtil.copyProperties(ticketCategory,ticketCategoryDetailVo);
        return ticketCategoryDetailVo;
    }
    
    /**
     * 根据节目ID查询票档列表
     * <p>该方法根据节目ID查询所有相关的票档信息列表。</p>
     *
     * @param ticketCategoryListByProgramDto 节目票档列表参数对象，包含节目ID
     * @return List<TicketCategoryDetailVo> 票档详情列表
     */
    public List<TicketCategoryDetailVo> selectListByProgram(TicketCategoryListByProgramDto ticketCategoryListByProgramDto) {
        List<TicketCategory> ticketCategorieList = ticketCategoryMapper.selectList(Wrappers.lambdaQuery(TicketCategory.class)
                .eq(TicketCategory::getProgramId, ticketCategoryListByProgramDto.getProgramId()));
        return ticketCategorieList.stream().map(ticketCategory -> {
            TicketCategoryDetailVo ticketCategoryDetailVo = new TicketCategoryDetailVo();
            BeanUtil.copyProperties(ticketCategory,ticketCategoryDetailVo);
            return ticketCategoryDetailVo;
        }).collect(Collectors.toList());
    }
}
