# 发布指南

本文档用于说明 `Valkey QueryDSL` 如何手动发布到 Maven Central，以及 `RC` / 正式版发布时需要注意的事项。

## 当前发布结论

当前仓库已经具备 Maven Central 发布能力，`1.0.0` 已成功发布，`1.1.0-RC1` 也已成功发布。

当前正式发布使用的关键前提如下：

- `groupId`: `io.github.lmemory123`
- Central Portal 命名空间已开通
- `~/.m2/settings.xml` 已配置 `central` server token
- 本机已配置 GPG 密钥并可用于 Maven 签名
- 根 `pom.xml` 已配置 `release` profile
- 仓库日常开发坐标仍保持为 `com.momao:*:*-SNAPSHOT`
- 正式发布时由脚本临时切换到 `io.github.lmemory123`
- GitHub Actions 的真实环境验证依赖 `ghcr.io/lmemory123/valkey-bundle:9.1.0`

## 发布产物

会发布到 Central 的模块：

- `io.github.lmemory123:valkey-query-annotations`
- `io.github.lmemory123:valkey-query-core`
- `io.github.lmemory123:valkey-query-processor`
- `io.github.lmemory123:valkey-query-glide-adapter`
- `io.github.lmemory123:valkey-query-spring-boot-starter`

不会发布到 Central 的模块：

- `valkey-query-test-example`

原因：

- 该模块只用于示例和集成测试
- 已设置 `maven.deploy.skip=true`
- 已设置 `skipPublishing=true`

## 发布前提

### 1. Sonatype Central Token

`~/.m2/settings.xml` 中需要存在：

```xml
<server>
  <id>central</id>
  <username>YOUR_PORTAL_TOKEN_USERNAME</username>
  <password>YOUR_PORTAL_TOKEN_PASSWORD</password>
</server>
```

### 2. GPG 密钥

仓库使用 `maven-gpg-plugin` 进行签名。

推荐生成方式：

```bash
gpg --full-generate-key
```

推荐参数：

- key type: `RSA and RSA`
- key size: `4096`
- expiration: `0`

查看密钥：

```bash
gpg --list-secret-keys --keyid-format LONG
```

发布时需要导出口令环境变量：

```bash
export MAVEN_GPG_PASSPHRASE='你的 GPG 口令'
```

如需指定签名 key：

```bash
-Dgpg.keyname=YOUR_KEY_ID
```

### 3. Maven 版本

当前仓库发布时，**推荐使用 Maven 3.9.x**。

说明：

- 当前本机默认 Maven 4.0.0-rc-5 在本项目里无法稳定触发 `central-publishing-maven-plugin` 的自动 `deploy` 生命周期注入
- 实际成功发布 `1.0.0` 时使用的是 Maven `3.9.11`

推荐检查：

```bash
mvn -version
```

如果本机默认不是 Maven 3.9.x，可以显式使用指定版本：

```bash
/tmp/apache-maven-3.9.11/bin/mvn -version
```

## 手动发布流程

### 1. 确认工作区状态

发布前建议：

- 当前分支无未确认改动
- 版本号已确认
- `CHANGELOG.md` 已更新
- `README.md` 中依赖版本示例已同步

### 2. 先跑一次本地校验

```bash
mvn -B -ntp clean test
```

如果你要和正式发布环境完全对齐，建议再跑一遍：

```bash
/tmp/apache-maven-3.9.11/bin/mvn -B -ntp clean test
```

### 3. 改成待发布版本

例如把当前：

- `1.1.0-SNAPSHOT`

改成：

- `1.1.0`

或：

- `1.1.0-RC1`

注意：

- 所有子模块版本都要保持一致
- Central 是不可变仓库，**同一版本号不能重复发布**

### 4. 设置 GPG 口令环境变量

```bash
export MAVEN_GPG_PASSPHRASE='你的 GPG 口令'
```

### 5. 执行正式发布

推荐命令：

```bash
./scripts/release-publish.sh 1.0.1 YOUR_KEY_ID
```

例如：

```bash
export MAVEN_GPG_PASSPHRASE='你的 GPG 口令'
./scripts/release-publish.sh 1.0.1 3842D1364CE6DA1A
```

说明：

- 会先把项目版本切到待发布版本
- 会临时把项目坐标从 `com.momao` 切到 `io.github.lmemory123`
- 会先执行 `./scripts/run-real-tests.sh all`
- `release` profile 会自动附带 `sources.jar`
- `release` profile 会自动附带 `javadoc.jar`
- `release` profile 会自动进行 GPG 签名
- `central-publishing-maven-plugin` 会自动上传 bundle 到 Sonatype Central

默认使用系统 `mvn`（按 `PATH` 解析）。

如果你需要显式指定 Maven（路径或命令名）：

```bash
./scripts/release-publish.sh 1.0.1 3842D1364CE6DA1A /tmp/apache-maven-3.9.11/bin/mvn
```

### 6. 等待 Portal 发布完成

当前配置是：

- `autoPublish=true`
- `waitUntil=published`

所以命令会一直等到状态变成 `PUBLISHED`。

成功日志通常类似：

```text
Uploaded bundle successfully, deploymentId: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
Waiting until Deployment xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx is published
Deployment xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx was successfully published
```

### 7. 打 Git Tag

例如：

```bash
./scripts/release-tag.sh 1.0.1 --push
```

这会创建并推送 `v1.0.1`，随后 GitHub Actions 的 [release.yml](/Users/momao/dm/java/demo/valkey-demo/.github/workflows/release.yml) 会自动生成 GitHub Release。

### 8. 切回下一个开发版本

例如正式发完 `1.0.1` 后，切回：

- `1.0.2-SNAPSHOT`

这是推荐做法，避免后续开发继续停留在 release 版本号上。

推荐命令：

```bash
./scripts/release-start-next.sh 1.0.2-SNAPSHOT
```

说明：

- 会把版本切回下一个 `SNAPSHOT`
- 会把项目坐标恢复为日常开发使用的 `com.momao`

## RC 版本与正式版怎么发

### RC 版本

推荐命名：

- `1.1.0-RC1`
- `1.1.0-RC2`

适用场景：

- 大版本前的外部试用
- 需要给别人提前验证 API / Starter / 索引治理行为
- 想让别人能通过 Maven Central 直接接入测试

注意事项：

- `RC` 版本也是正式进入 Central 的不可变版本
- `1.1.0-RC1` 一旦发布，不能覆盖
- 如果有 bug，只能发 `1.1.0-RC2`
- 不要把 `RC` 当成可以反复覆盖的临时版本

推荐流程：

1. 从 `x.y.z-SNAPSHOT` 切到 `x.y.z-RC1`
2. 发布到 Central
3. 收反馈
4. 如需修复，发布 `RC2 / RC3`
5. 最终确认后，发正式版 `x.y.z`

### 正式版

推荐命名：

- `1.0.0`
- `1.0.1`
- `1.1.0`

适用场景：

- 对外正式宣发
- README 示例依赖版本更新
- GitHub Release / CHANGELOG 对外同步

注意事项：

- 正式版不能覆盖
- 发布前必须确认版本号、文档、变更记录和依赖示例全部一致
- 正式版发布后，下一步要尽快切回新的 `SNAPSHOT`

## SNAPSHOT、RC、正式版的建议策略

建议采用下面这套简单规则：

- 日常开发：`x.y.z-SNAPSHOT`
- 对外试发布：`x.y.z-RC1`
- RC 修复：`x.y.z-RC2`
- 正式发布：`x.y.z`
- 正式版发布后继续开发：`x.y.(z+1)-SNAPSHOT`

示例：

- 当前开发中：`1.1.0-SNAPSHOT`
- 第一轮候选版：`1.1.0-RC1`
- 修复后再发：`1.1.0-RC2`
- 最终正式版：`1.1.0`
- 继续开发：`1.1.1-SNAPSHOT`

## 发布时必须注意的坑

### 1. Central 不可覆盖

最重要的一条：

- 版本一旦发布，不能重新上传覆盖

所以：

- 不要在版本号还没确认时就急着发布
- 不要把调试版直接发成正式版

### 2. Maven 4 兼容性

当前本项目的实际发布经验表明：

- Maven 4.0.0-rc-5 下，`central-publishing-maven-plugin` 在本项目里没有稳定执行自动发布链
- Maven 3.9.11 可以正常完成上传和发布

所以当前版本发布建议：

- **只用 Maven 3.9.x 发版**

### 3. 搜索页存在延迟

即使 Portal 已经显示 `PUBLISHED`：

- `repo1.maven.org` 往往先可访问
- `search.maven.org` 和 Maven Central 搜索页可能会延迟几分钟到几十分钟

所以发布验证建议先看：

- `https://repo1.maven.org/maven2/...`

### 4. Example 模块不应该发布

`valkey-query-test-example` 只是测试工程，不应作为正式依赖暴露给外部用户。

当前已经做了排除，但发版前仍建议确认：

- `maven.deploy.skip=true`
- `skipPublishing=true`

### 5. 版本示例要同步

每次正式版或 RC 发版后，都建议同步：

- `README.md`
- `docs/快速开始.md`
- `CHANGELOG.md`

避免文档里还写着旧版本号。

## 一套推荐命令

### 发布 RC

```bash
export MAVEN_GPG_PASSPHRASE='你的 GPG 口令'
/tmp/apache-maven-3.9.11/bin/mvn -B -ntp clean test
/tmp/apache-maven-3.9.11/bin/mvn -B -ntp -Prelease -Dgpg.keyname=YOUR_KEY_ID clean deploy
git tag v1.1.0-RC1
git push origin v1.1.0-RC1
```

### 发布正式版

```bash
export MAVEN_GPG_PASSPHRASE='你的 GPG 口令'
/tmp/apache-maven-3.9.11/bin/mvn -B -ntp clean test
/tmp/apache-maven-3.9.11/bin/mvn -B -ntp -Prelease -Dgpg.keyname=YOUR_KEY_ID clean deploy
git tag v1.1.0
git push origin v1.1.0
```

### 发完后切回开发版

```bash
# 把 pom 版本改成下一个 SNAPSHOT
git add .
git commit -m "chore: start next development iteration"
git push origin main
```

## 当前项目的实际发布记录

`1.0.0` 的实际发布结果：

- 使用 Maven：`3.9.11`
- Deployment ID：`c7eca613-bba6-4906-9f79-9d9229f7af9e`
- 最终状态：`PUBLISHED`

这说明当前这套 `release` profile 和 Central 发布链路已经可复用。

## 发布辅助脚本

仓库已经提供了 3 个辅助脚本：

- `scripts/set-project-version.py`
- `scripts/release-publish.sh`
- `scripts/release-start-next.sh`

### 只改版本号

```bash
python3 ./scripts/set-project-version.py 1.1.0-RC1
```

这个脚本只会更新：

- 根 `pom.xml` 的项目版本
- 各子模块 `parent.version`

不会修改：

- `README.md`
- `CHANGELOG.md`
- 其它文档示例

### 一键发布 RC / 正式版

```bash
export MAVEN_GPG_PASSPHRASE='你的 GPG 口令'
./scripts/release-publish.sh 1.1.0-RC1 3842D1364CE6DA1A
```

或：

```bash
export MAVEN_GPG_PASSPHRASE='你的 GPG 口令'
./scripts/release-publish.sh 1.1.0 3842D1364CE6DA1A
```

默认使用系统 `mvn`（按 `PATH` 解析）。

如果你想手动指定 Maven（路径或命令名）：

```bash
./scripts/release-publish.sh 1.1.0 3842D1364CE6DA1A /path/to/mvn
```

### 发版后切回开发版本

```bash
./scripts/release-start-next.sh 1.1.1-SNAPSHOT
```

然后再手工提交：

```bash
git add pom.xml */pom.xml
git commit -m "chore: start 1.1.1-SNAPSHOT"
```
