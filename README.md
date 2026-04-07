# Valkey QueryDSL

项目名称：`Valkey QueryDSL`

`Valkey QueryDSL` 是一个面向 Java 17 与 Spring Boot 的 Valkey Search 类型安全查询框架，目标是用编译期生成的查询对象替代手写 `FT.SEARCH` 字符串。

它通过：

- 注解定义实体与索引
- APT 生成 `XXXQuery` 与索引元数据
- Repository 提供 `list / page / one / count`
- Spring Boot Starter 自动完成扫描、连接与索引初始化

把 Valkey Search 的底层字符串查询，收口成更稳定的 Java API。

## 项目缘起

这个项目不是从“大而全框架设计”出发，而是来自一个非常具体的开发体验问题。

平时会更关注那些对开源社区更友好、治理更透明、生态更健康的基础设施项目。接触 Valkey 之后，很快就会被它 “Open Source, Forever” 的定位吸引，因为这类项目更适合长期学习、二次开发和实际落地。

但在真正写项目时，也会碰到一个很现实的问题：Valkey Search 的底层能力没有问题，原生接口也足够强，但直接面向 Java 业务开发时并不算顺手。尤其在下面这些场景里，使用成本会明显上升：

- 查询条件需要手工拼接，容易写错
- 索引定义和实体模型容易分离
- 集合字段、嵌套对象、分页和排序的调用不够统一
- Spring Boot 接入时需要重复写连接、扫描和建索引逻辑

所以这个项目的目标很明确：不是重新发明一个数据库客户端，而是给 Valkey Search 补上一层更适合 Java / Spring Boot 业务项目的 QueryDSL 和 Repository 抽象，让“实体定义、查询构造、索引元数据、运行时执行”串成一套完整工作流。

## 解决的问题

这个项目主要解决三类问题：

1. Valkey Search 查询表达式偏底层，业务代码直接拼字符串容易错。
2. 真实业务实体通常包含分页、排序、集合字段和嵌套对象，不适合只靠临时命令处理。
3. 索引定义、Java 实体和查询条件长期容易漂移，需要编译期约束把这些绑定起来。

## 核心能力

基础能力：

- 编译期生成 `XXXQuery`
- 支持 `StorageType.JSON` 和 `StorageType.HASH`
- 支持 `list / page / one / count`
- 支持链式查询 `ValkeyQueryChain`
- 支持集合字段和嵌套对象
- 支持 Spring Boot 自动建索引

高阶能力：

- 基础聚合链 `ValkeyAggregateChain`
- Facet 快捷链 `ValkeyFacetChain`
- 多字段独立 Facet 结果 `FacetResults`
- 聚合结果映射 `AggregateResult.mapRows(...)`
- `AggregateExpressions` 轻量表达式 DSL
- 向量检索

## 模块结构

- `valkey-query-annotations`
  注解定义
- `valkey-query-core`
  查询模型、分页对象、Repository 接口、链式查询
- `valkey-query-processor`
  APT 处理器
- `valkey-query-glide-adapter`
  Glide 适配层与 Repository 基类
- `valkey-query-spring-boot-starter`
  Spring Boot 自动配置
- `valkey-query-test-example`
  示例工程与真实测试

## 一个最小示例

```java
@ValkeyDocument(
    indexName = "idx:sku",
    prefixes = {"sku:"},
    storageType = StorageType.JSON
)
public class Sku {

    @ValkeyId
    private String id;

    @ValkeySearchable(weight = 2.5d, noStem = true)
    private String title;

    @ValkeyNumeric(sortable = true)
    private Integer price;
}
```

编译后会生成 `SkuQuery`，可以直接这样查询：

```java
SkuQuery q = new SkuQuery();

Page<Sku> page = skuRepository.queryChain()
    .where(q.title.contains("iphone"))
    .orderByAsc("price")
    .page(0, 20);
```

## 文档导航

### 简介

- [技术架构](docs/技术架构.md)

### 快速开始

- [快速开始](docs/快速开始.md)
- [最小示例](docs/最小示例.md)
- [依赖接入](docs/依赖接入.md)
- [依赖说明](docs/依赖说明.md)

### 基础功能

- [基础功能](docs/基础功能.md)
- [使用指南](docs/使用指南.md)

### 核心功能

- [注解参考](docs/注解参考.md)
- [配置与治理](docs/配置与治理.md)
- [拓扑支持矩阵](docs/拓扑支持矩阵.md)

### 高阶能力

- [高阶能力](docs/高阶能力.md)

### 接入减负

- `@EnableValkeyQuery("...")` 支持 `value` 别名
- `@EnableValkeyQuery` 无参时默认回退到启动类所在包
- `saveBatch(...)` 是 `saveAll(...)` 的别名
- 结构化字段推荐显式使用 `@ValkeyTag / @ValkeyNumeric`

### 工程化

- [常见问题排障](docs/常见问题排障.md)
- [真实环境测试手册](docs/真实环境测试手册.md)
- [测试分层说明](docs/测试分层说明.md)
- [发布指南](RELEASE.md)

## 示例工程

- `valkey-query-test-example`

## 本地构建

```bash
mvn clean install
```

## 版本与坐标

- Maven Central 已发布稳定版：`io.github.lmemory123:*:1.0.0`
- Maven Central 已发布候选版：`io.github.lmemory123:*:1.1.0-RC5`
- 当前仓库源码快照：`com.momao:*:1.1.0-SNAPSHOT`

如果你是第一次接入：

- 想直接在业务项目里引用稳定版，按 [快速开始](/Users/momao/dm/java/demo/valkey-demo/docs/快速开始.md) 里的 Maven Central 坐标接入
- 想验证当前工作区的最新代码，先在本仓库执行 `mvn clean install`，再在业务项目里引用 `com.momao:*:1.1.0-SNAPSHOT`

## 自动化发版

仓库已经提供本地发版脚本和 GitHub Release 自动化：

- 本地发布到 Maven Central：[`scripts/release-publish.sh`](/Users/momao/dm/java/demo/valkey-demo/scripts/release-publish.sh)
- 发布后创建并推送 tag：[`scripts/release-tag.sh`](/Users/momao/dm/java/demo/valkey-demo/scripts/release-tag.sh)
- 切回下一个开发快照：[`scripts/release-start-next.sh`](/Users/momao/dm/java/demo/valkey-demo/scripts/release-start-next.sh)
- GitHub tag 自动生成 Release：[`release.yml`](/Users/momao/dm/java/demo/valkey-demo/.github/workflows/release.yml)

完整流程见 [发布指南](/Users/momao/dm/java/demo/valkey-demo/RELEASE.md)。

补充说明：

- 普通 `push main` 只会触发 CI 测试，不会自动打包发布
- 只有手工执行 [`scripts/release-publish.sh`](/Users/momao/dm/java/demo/valkey-demo/scripts/release-publish.sh) 才会发布到 Maven Central
- 只有推送 `v*` tag 才会触发 GitHub Release 自动生成

## 生态背景

如果你关心 Valkey 生态成熟度，可以继续看这些来源：

- [valkey-io/valkey](https://github.com/valkey-io/valkey)
- [valkey-io/valkey-glide](https://github.com/valkey-io/valkey-glide)
- [Valkey GitHub Organization](https://github.com/valkey-io)
- [Docker Hub: valkey/valkey](https://hub.docker.com/r/valkey/valkey)
- [Valkey 官网](https://valkey.io/)
- [Valkey Blog: Celebrating Valkey's First Year and Looking Ahead](https://valkey.io/blog/celebrating-valkeys-first-year-and-looking-ahead/)

## 开源协议

本项目采用 [MIT License](LICENSE)。
