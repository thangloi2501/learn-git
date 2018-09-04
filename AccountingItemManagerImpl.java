package com.tcbs.portfolio.feature.accounting.manager;

import com.google.common.collect.Iterables;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQuery;
import com.tcbs.portfolio.common.Constants;
import com.tcbs.portfolio.common.PagingUtils;
import com.tcbs.portfolio.exception.PortfolioExceptions;
import com.tcbs.portfolio.feature.accounting.entity.AccountingItemEntity;
import com.tcbs.portfolio.feature.accounting.entity.QAccountingItemEntity;
import com.tcbs.portfolio.feature.accounting.manager.base.AccountingItemProvider;
import com.tcbs.portfolio.feature.accounting.manager.base.AccountingItemProviderFactory;
import com.tcbs.portfolio.feature.accounting.model.*;
import com.tcbs.portfolio.feature.accounting.repository.AccountingItemRepository;
import com.tcbs.portfolio.feature.accounting.repository.QueryAspectAccountingItemRepository;
import com.tcbs.portfolio.feature.accounting.task.AccountingTaskRunner;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Quartet;
import org.javatuples.Triplet;
import org.modelmapper.ModelMapper;
import org.modelmapper.TypeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import javax.persistence.EntityManager;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.tcbs.portfolio.feature.accounting.entity.QAccountingItemEntity.accountingItemEntity;

@Component
public class AccountingItemManagerImpl implements AccountingItemManager, AccountingItemProvider, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(AccountingItemManagerImpl.class);

    @Autowired
    @Qualifier("accountingItemTaskRunner")
    AccountingTaskRunner accountingTaskRunner;

    @Autowired
    AccountingItemRepository accountingItemRepository;

    @Autowired
    QueryAspectAccountingItemRepository queryAspectAccountingItemRepository;

    @Autowired
    AccountingItemProviderFactory accountingItemProviderFactory;

    @Autowired
    PortfolioExceptions portfolioExceptions;

    @Autowired
    ModelMapper modelMapper;

    @Autowired
    EntityManager entityManager;

    private QAccountingItemEntity qAccountingItemEntity = accountingItemEntity;

    @Override
    public void performAccountingItem(AccountingItem accountingItem) {
        accountingTaskRunner.run(accountingItem);
    }

    @Override
    public Optional<AccountingItem> getByNameAndAccountAndProductAndSource(AccountingItem accountingItem, Date toDateTime) {
        BooleanExpression accountingExp = qAccountingItemEntity.accountId.eq(accountingItem.getAccountItem().getTcbsId())
                .and(qAccountingItemEntity.accountSource.eq(accountingItem.getAccountItem().getSource()));

        BooleanExpression productExp = accountingItem.getProductItem().getProductCode() != null ?
                qAccountingItemEntity.productItemEntity.productCode.eq(accountingItem.getProductItem().getProductCode()) :
                qAccountingItemEntity.productItemEntity.productCode.isNull();

        BooleanExpression applyTimeExp = buildApplyTimeBeforeOrEqualExp(toDateTime);
        Predicate predicate = isValidItemExp().and(accountingExp).and(productExp).and(applyTimeExp);

        JPAQuery<AccountingItemEntity> query = new JPAQuery<>(entityManager);
        AccountingItemEntity rs = query.from(qAccountingItemEntity)
                .where(predicate)
                .orderBy(qAccountingItemEntity.applyTime.desc())
                .orderBy(qAccountingItemEntity.updatedDate.desc())
                .fetchFirst();

        TypeMap<AccountingItemEntity, AccountingItem> typeMap = modelMapper.getTypeMap(AccountingItemEntity.class, AccountingItem.class);
        return rs != null ? Optional.ofNullable(typeMap.map(rs)) : Optional.empty();
    }

    @Override
    public void afterPropertiesSet() {
        accountingItemProviderFactory.addProvider(Constants.AI_TYPE.NORMAL, this);
    }

    @Override
    public List<AccountingItem> getAllAccountingItemByAccountId(String accountId, Date toDateTime) {
        logger.trace("start query");
        long now1 = System.currentTimeMillis();
        List<QueryAspectAccountingItemAttr> entities = queryAspectAccountingItemRepository.getByAccountIdAndBeforeTime(
                accountId, toDateTime
        );
        logger.trace("end query, spend {} ms", System.currentTimeMillis() - now1);
        logger.trace("entities size {}", Iterables.size(entities));

        List<AccountingItem> accountingItemsResult = groupToItem(entities);
        logger.trace("accountingItemsResult size {}", accountingItemsResult.size());
        return accountingItemsResult;
    }

    @Override
    public List<AccountingItem> getAllAccountingItemByProductCode(String productCode, Date toDateTime) {
        logger.trace("start query");
        long now1 = System.currentTimeMillis();
        List<QueryAspectAccountingItemAttr> entities = queryAspectAccountingItemRepository.getByProductCodeAndBeforeTime(
                productCode, toDateTime
        );
        logger.trace("end query, spend {} ms", System.currentTimeMillis() - now1);
        logger.trace("entities size {}", Iterables.size(entities));

        List<AccountingItem> accountingItemsResult = groupToItem(entities);
        logger.trace("accountingItemsResult size {}", accountingItemsResult.size());
        return accountingItemsResult;
    }

    @Override
    public List<AccountingItem> getAllAccountingItemByBondCode(String bondCode, Date toDateTime) {
        logger.trace("start query");
        long now1 = System.currentTimeMillis();
        List<QueryAspectAccountingItemAttr> entities = queryAspectAccountingItemRepository.getByUnderlyingAndBeforeTime(
                bondCode, toDateTime
        );
        logger.trace("end query, spend {} ms", System.currentTimeMillis() - now1);
        logger.trace("entities size {}", Iterables.size(entities));

        List<AccountingItem> accountingItemsResult = groupToItem(entities);
        logger.trace("accountingItemsResult size {}", accountingItemsResult.size());
        return accountingItemsResult;
    }

    // TODO: 8/2/18 should change to proper solution
    @Override
    public Page<AccountingItem> getAllAccountingItemByBondCode(String bondCode, Pageable pageable) {
        AccountingPageableRequest accountingPageableRequest = (AccountingPageableRequest) pageable;

        List<AccountingItem> accountingItemsResult = getAllAccountingItemByBondCode(
                bondCode,
                accountingPageableRequest.getToDateTime());

        return PagingUtils.getPage(accountingItemsResult, pageable);
    }

    // TODO: 7/24/18 this API is till in used?
    @Override
    public List<AccountingItem> getAllAccountingItemByCondition(AccountingItemParam accountingItemParam, Date toDateTime) {

        String bondCode = accountingItemParam.getBondCode();
        String productCode = accountingItemParam.getProductCode();
        String accountId = accountingItemParam.getAccountId();

        BooleanExpression productItemExp = StringUtils.isEmpty(bondCode) ?
                qAccountingItemEntity.productItemEntity.underlyingCode.isNotNull() : qAccountingItemEntity.productItemEntity.underlyingCode.eq(bondCode)
                .and(StringUtils.isEmpty(productCode) ?
                        qAccountingItemEntity.productItemEntity.productCode.isNotNull() : qAccountingItemEntity.productItemEntity.productCode.eq(productCode));

        BooleanExpression accountingItemExp = StringUtils.isEmpty(accountId) ?
                qAccountingItemEntity.accountId.isNotNull() : qAccountingItemEntity.accountId.eq(accountId);

        Predicate accountingItemQuery = isValidItemExp().and(accountingItemExp).and(productItemExp);
        Iterable<AccountingItemEntity> accountingItemEntities = accountingItemRepository.findAll(accountingItemQuery);
        TypeMap<AccountingItemEntity, AccountingItem> typeMap = modelMapper.getTypeMap(AccountingItemEntity.class, AccountingItem.class);

        List<AccountingItem> accountingItems = StreamSupport.stream(accountingItemEntities.spliterator(), false)
                .map(typeMap::map).collect(Collectors.toList());

        // <ProductCode, ProductUnderlyingCode, Source>
        Map<Triplet<String, String, String>, List<AccountingItem>> groups = accountingItems.stream().collect(Collectors.groupingBy(ai ->
                Triplet.with(ai.getProductItem().getProductCode(),
                        ai.getProductItem().getUnderlyingCode(),
                        ai.getSource())
        ));
        groups.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isEmpty());
        List<AccountingItem> accountingItemsResult = groups.values().stream().map(ais -> getLatestByApplyTime(ais).get()).collect(Collectors.toList());
        return accountingItemsResult;
    }

    @Override
    public void importAccountingItem(AccountingItem accountingItem) {
        logger.debug("================= {}", accountingItem);

        TypeMap<AccountingItem, AccountingItemEntity> toEntityTypeMap = modelMapper.getTypeMap(AccountingItem.class, AccountingItemEntity.class);
        AccountingItemEntity entity = toEntityTypeMap.map(accountingItem);
        accountingItemRepository.saveAndFlush(entity);
    }

    // AccountingItemProvider implementations

    @Override
    public AccountingItem save(AccountingItem accountingItem) {

        logger.info("save accounting item {}", accountingItem);

        TypeMap<AccountingItem, AccountingItemEntity> toEntityTypeMap = modelMapper.getTypeMap(
                AccountingItem.class, AccountingItemEntity.class);
        AccountingItemEntity entity = toEntityTypeMap.map(accountingItem);

        logger.info("save accounting item entity {}", entity);

        long now1 = System.currentTimeMillis();
        AccountingItemEntity savedEntity = accountingItemRepository.saveAndFlush(entity);
        logger.trace("time: {}", System.currentTimeMillis() - now1);
        if (savedEntity != null) {
            TypeMap<AccountingItemEntity, AccountingItem> fromEntityTypeMap = modelMapper.getTypeMap(
                    AccountingItemEntity.class, AccountingItem.class);
            return fromEntityTypeMap.map(savedEntity);
        } else throw portfolioExceptions.dbErrorException.get();
    }

    @Override
    public AccountingItem update(AccountingItem accountingItem) {
        logger.info("update accounting item {}", accountingItem);

        TypeMap<AccountingItem, AccountingItemEntity> toEntityTypeMap = modelMapper.getTypeMap(
                AccountingItem.class, AccountingItemEntity.class);
        AccountingItemEntity entity = toEntityTypeMap.map(accountingItem);

        AccountingItemEntity savedEntity = accountingItemRepository.saveAndFlush(entity);
        if (savedEntity != null) {
            TypeMap<AccountingItemEntity, AccountingItem> fromEntityTypeMap = modelMapper.getTypeMap(
                    AccountingItemEntity.class, AccountingItem.class);
            return fromEntityTypeMap.map(savedEntity);
        } else throw portfolioExceptions.dbErrorException.get();
    }

    @Override
    public Optional<AccountingItem> getLatestByNameAndAccountAndProductAndSource(AccountingItem accountingItem) {
        BooleanExpression accountingExp = qAccountingItemEntity.accountId.eq(accountingItem.getAccountItem().getTcbsId())
                .and(qAccountingItemEntity.accountSource.eq(accountingItem.getAccountItem().getSource()));

        BooleanExpression productExp = accountingItem.getProductItem().getProductCode() != null ?
                qAccountingItemEntity.productItemEntity.productCode.eq(accountingItem.getProductItem().getProductCode()) :
                qAccountingItemEntity.productItemEntity.productCode.isNull();

        Predicate predicate = isValidItemExp().and(accountingExp).and(productExp);

        JPAQuery<AccountingItemEntity> query = new JPAQuery<>(entityManager);
        long now1 = System.currentTimeMillis();
        AccountingItemEntity rs = query.from(qAccountingItemEntity)
                .where(predicate)
                .orderBy(qAccountingItemEntity.applyTime.desc())
                .orderBy(qAccountingItemEntity.updatedDate.desc())
                .fetchFirst();
        logger.trace("time: {}", System.currentTimeMillis() - now1);

        TypeMap<AccountingItemEntity, AccountingItem> typeMap = modelMapper.getTypeMap(AccountingItemEntity.class, AccountingItem.class);
        return rs != null ? Optional.ofNullable(typeMap.map(rs)) : Optional.empty();
    }

    // Optional<T> getByNameAndAccountAndProductAndSource(AccountingItem accountingItem, Date toDateTime)
    // is already implemented above

    @Override
    public List<AccountingItem> getAllExistingNewerByNameAndAccountAndProductAndSource(AccountingItem accountingItem, Date fromDateTime) {
        BooleanExpression accountingExp = qAccountingItemEntity.accountId.eq(accountingItem.getAccountItem().getTcbsId())
                .and(qAccountingItemEntity.accountSource.eq(accountingItem.getAccountItem().getSource()));

        BooleanExpression productExp = accountingItem.getProductItem().getProductCode() != null ?
                qAccountingItemEntity.productItemEntity.productCode.eq(accountingItem.getProductItem().getProductCode()) :
                qAccountingItemEntity.productItemEntity.productCode.isNull();

        BooleanExpression applyTimeExp = buildApplyTimeAfterExp(fromDateTime);

        Predicate predicate = isValidItemExp().and(accountingExp).and(productExp).and(applyTimeExp);

        JPAQuery<AccountingItemEntity> query = new JPAQuery<>(entityManager);
        List<AccountingItemEntity> accountingItemEntities = query.from(qAccountingItemEntity)
                .where(predicate)
                .fetch();
        logger.trace("entities {}", accountingItemEntities);

        TypeMap<AccountingItemEntity, AccountingItem> typeMap = modelMapper.getTypeMap(AccountingItemEntity.class, AccountingItem.class);
        return accountingItemEntities.stream().map(typeMap::map).collect(Collectors.toList());
    }

    // private methods
    private Optional<AccountingItem> getLatestByApplyTime(List<AccountingItem> accountingItems) {
        return accountingItems.stream().max((AccountingItem ai1, AccountingItem ai2) -> {
            Optional<Date> applyTimeOpt1 = Optional.ofNullable(ai1.getApplyTime());
            Optional<Date> applyTimeOpt2 = Optional.ofNullable(ai2.getApplyTime());
            Date applyTime1 = applyTimeOpt1.orElse(ai1.getUpdatedDate());
            Date applyTime2 = applyTimeOpt2.orElse(ai2.getUpdatedDate());
            int applyTimeCmp = applyTime1.compareTo(applyTime2);
            return applyTimeCmp != 0 ? applyTimeCmp : ai1.getUpdatedDate().compareTo(ai2.getUpdatedDate());
        });
    }

    private Optional<AccountingItemEntity> getLatestEntityByApplyTime(List<AccountingItemEntity> accountingItems) {
        return accountingItems.stream().max((AccountingItemEntity aiEntity1, AccountingItemEntity aiEntity2) -> {
            Optional<Date> applyTimeOpt1 = Optional.ofNullable(aiEntity1.getApplyTime());
            Optional<Date> applyTimeOpt2 = Optional.ofNullable(aiEntity2.getApplyTime());
            Date applyTime1 = applyTimeOpt1.orElse(aiEntity1.getUpdatedDate());
            Date applyTime2 = applyTimeOpt2.orElse(aiEntity2.getUpdatedDate());
            int applyTimeCmp = applyTime1.compareTo(applyTime2);
            return applyTimeCmp != 0 ? applyTimeCmp : aiEntity1.getUpdatedDate().compareTo(aiEntity2.getUpdatedDate());
        });
    }

    private BooleanExpression buildApplyTimeBeforeOrEqualExp(Date conditionTime) {
        return qAccountingItemEntity.applyTime.before(conditionTime)
                .or(qAccountingItemEntity.applyTime.eq(conditionTime));
    }

    private BooleanExpression buildApplyTimeAfterExp(Date conditionTime) {
        return qAccountingItemEntity.applyTime.after(conditionTime);
    }

    private BooleanExpression isValidItemExp() {
        return qAccountingItemEntity.status.isNull()
                .or(qAccountingItemEntity.status.eq(Constants.AI_STATUS.VALID.name()));
    }

    private List<AccountingItem> groupAndFindClosestItemsByApplyTime(List<AccountingItemEntity> accountingItemEntities) {
        TypeMap<AccountingItemEntity, AccountingItem> typeMap = modelMapper.getTypeMap(AccountingItemEntity.class, AccountingItem.class);
        // <account id, source, product code, underlying code>
        return StreamSupport.stream(accountingItemEntities.spliterator(), false)
                .collect(Collectors.groupingBy(ai ->
                        Quartet.with(
                                ai.getAccountId(),
                                ai.getAccountSource(),
                                ai.getProductItemEntity().getProductCode(),
                                ai.getProductItemEntity().getUnderlyingCode())
                )).values().parallelStream().map(ais -> getLatestEntityByApplyTime(ais).get()).map(typeMap::map).collect(Collectors.toList());
    }

    private List<AccountingItem> groupToItem(List<QueryAspectAccountingItemAttr> accountingAttrEntities) {
        TypeMap<QueryAspectAccountingItemAttr, AccountingItemAttr> typeMap = modelMapper.getTypeMap(
                QueryAspectAccountingItemAttr.class, AccountingItemAttr.class);
        return accountingAttrEntities.stream().collect(Collectors.groupingBy(attr ->
                attr.getAccountingItemId()
        )).values().parallelStream().map(attrs -> {
            QueryAspectAccountingItemAttr behalfOfGroup = attrs.get(0);
            AccountingItem accountingItem = modelMapper.map(behalfOfGroup, AccountingItem.class);
            accountingItem.setAccountingItemAttr(attrs.stream().map(typeMap::map).collect(Collectors.toList()));
            return accountingItem;
        }).collect(Collectors.toList());
    }
}
