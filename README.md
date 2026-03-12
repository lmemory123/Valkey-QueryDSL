# Valkey QueryDSL

项目名称：`Valkey QueryDSL`

这是一个面向 Java 17 与 Spring Boot 的 Valkey Search 类型安全查询框架。

它通过：

- 注解定义实体与索引
- APT 生成 `XXXQuery` 与索引元数据
- Repository 提供 `list / page / one / count`
- Spring Boot Starter 自动完成扫描、连接与索引初始化

把 Valkey Search 的底层字符串查询，收口成更稳定的 Java API。

## 文档入口

- [文档首页](docs/README.md)
- [项目介绍](docs/项目介绍.md)
- [快速开始](docs/快速开始.md)
- [使用指南](docs/使用指南.md)

## 示例工程

- `valkey-query-test-example`

## 本地构建

```bash
mvn clean install
```
