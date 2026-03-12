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

- 编译期生成 `XXXQuery`
- 支持 `StorageType.JSON` 和 `StorageType.HASH`
- 支持 `list`、`page`、`one`、`count`
- 支持链式查询 `ValkeyQueryChain`
- 支持集合字段和嵌套对象
- 支持 Spring Boot 自动建索引
- 支持 Glide 运行时适配

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

## 生态背景

Valkey 本身已经不是一个边缘实验项目，而是一个增长很快、生态越来越完整的开源基础设施项目。截止 2026 年 3 月 12 日，可以参考这些公开指标：

- Valkey 官方 GitHub 仓库约 `25.1k` stars
- `valkey-glide` 官方仓库约 `705` stars
- Valkey GitHub 组织约 `1.4k` followers
- 官方 Docker Hub 镜像 `valkey/valkey` 已达到 `50M+` pulls
- 官方网站明确写明项目由 Linux Foundation 支持，并强调 “Open Source, Forever”

从官方博客披露的信息看，Valkey 在成立一年左右时，企业参与方数量已经从 `22` 增长到 `47`，背后有 AWS、Google Cloud、Oracle、Alibaba、Huawei、Tencent、Canonical、Percona 等贡献者持续投入。

说明：

- 官方公开页面没有给出统一、可稳定引用的“每月下载量”口径
- 这里优先采用 GitHub Stars、Docker Hub pulls 和官方生态说明作为参考指标

参考来源：

- [valkey-io/valkey](https://github.com/valkey-io/valkey)
- [valkey-io/valkey-glide](https://github.com/valkey-io/valkey-glide)
- [Valkey GitHub Organization](https://github.com/valkey-io)
- [Docker Hub: valkey/valkey](https://hub.docker.com/r/valkey/valkey)
- [Valkey 官网](https://valkey.io/)
- [Valkey Blog: Celebrating Valkey's First Year and Looking Ahead](https://valkey.io/blog/celebrating-valkeys-first-year-and-looking-ahead/)

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

    @ValkeyIndexed(sortable = true)
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

- [快速开始](docs/快速开始.md)
- [使用指南](docs/使用指南.md)
- [依赖说明](docs/依赖说明.md)
- [技术架构](docs/技术架构.md)

## 示例工程

- `valkey-query-test-example`

## 本地构建

```bash
mvn clean install
```

## 开源协议

本项目采用 [MIT License](LICENSE)。
