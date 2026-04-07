# 发布指南

这份文档只回答 3 件事：

- 先发到哪里
- 每一步先做什么后做什么
- 这次已经踩过哪些坑，以后不要再踩

一句话结论：

- **先发 Maven Central**
- **Central 成功后再打 Git tag**
- **Git tag 推上去后，GitHub Release 会自动生成**
- **平时 push 到 `main` 只跑 CI 测试，不会自动打包发布**

不要反过来做。  
如果先打 tag、先出 GitHub Release，而 Central 最后失败，外部看到的版本状态就会错乱。

## 当前发布基线

当前仓库的发布基线如下：

- 开发态坐标：`com.momao:*:1.1.0-SNAPSHOT`
- 发布态坐标：`io.github.lmemory123:*:<release-version>`
- 稳定版：`1.0.0`
- 当前候选版：`1.1.0-RC5`
- GitHub Actions 真实验证镜像：`ghcr.io/lmemory123/valkey-bundle:9.1.0`
- 推荐 Maven：`3.9.x`
- 当前可用发布 key：`8ACEA513E5F728CC`

## 会发布哪些模块

会发布到 Maven Central：

- `io.github.lmemory123:valkey-query-annotations`
- `io.github.lmemory123:valkey-query-core`
- `io.github.lmemory123:valkey-query-processor`
- `io.github.lmemory123:valkey-query-glide-adapter`
- `io.github.lmemory123:valkey-query-spring-boot-starter`

不会发布到 Maven Central：

- `valkey-query-test-example`

## 发布前提

### 1. Central Token

`~/.m2/settings.xml` 里必须有：

```xml
<server>
  <id>central</id>
  <username>YOUR_PORTAL_TOKEN_USERNAME</username>
  <password>YOUR_PORTAL_TOKEN_PASSWORD</password>
</server>
```

### 2. GPG 密钥

查看可用 key：

```bash
gpg --list-secret-keys --keyid-format LONG
```

发布前建议用这条先验签：

```bash
MAVEN_GPG_PASSPHRASE='你的口令' /Users/momao/dm/path/java/apache-maven-3.9.14/bin/mvn -B -ntp -Prelease -Dgpg.keyname=8ACEA513E5F728CC -DskipTests verify
```

只要这里 `BUILD SUCCESS`，说明：

- GPG key 可用
- 口令是对的
- `release` profile 能正常签名

### 3. Maven 版本

当前项目正式发布时，**只推荐 Maven 3.9.x**。

原因：

- Maven 4 可以做本地 `verify`
- 但这个项目之前真实发布到 Central，稳定成功的是 Maven 3.9.x
- Maven 4 在 `central-publishing-maven-plugin` 这条链上不够稳

推荐直接用：

```bash
/Users/momao/dm/path/java/apache-maven-3.9.14/bin/mvn -version
```

### 4. 真实环境验证

发布前必须跑真实 Valkey 测试：

```bash
./scripts/run-real-tests.sh all
```

不接受只跑 mock 或纯模型测试就发版。

## 标准发布顺序

先把边界说清楚：

- `push main`：只触发 [ci.yml](/Users/momao/dm/java/demo/valkey-demo/.github/workflows/ci.yml)，验证编译和真实环境测试
- `push vX.Y.Z / vX.Y.Z-RC* tag`：才触发 [release.yml](/Users/momao/dm/java/demo/valkey-demo/.github/workflows/release.yml)，自动创建 GitHub Release
- 发布到 Maven Central：只会在你手工执行 [`scripts/release-publish.sh`](/Users/momao/dm/java/demo/valkey-demo/scripts/release-publish.sh) 时发生

也就是说：

- 平时提交到 `main` 不会自动发版
- 不会自动上传 Maven Central
- 不会因为普通提交就生成 GitHub Release

### 第 1 步：确认当前是开发态

检查根 `pom.xml`：

```bash
grep -n "<version>" pom.xml | head -n 1
```

正常应该是：

```xml
<version>1.1.0-SNAPSHOT</version>
```

### 第 2 步：发布到 Maven Central

这是第一优先级，必须先做。

示例，发布 `1.1.0-RC5`：

```bash
cd /Users/momao/dm/java/demo/valkey-demo
MAVEN_GPG_PASSPHRASE='你的口令' ./scripts/release-publish.sh 1.1.0-RC5 8ACEA513E5F728CC /Users/momao/dm/path/java/apache-maven-3.9.14/bin/mvn
```

这个脚本会自动做这些事：

- 把版本切到 `1.1.0-RC5`
- 把 `groupId` 从 `com.momao` 临时切到 `io.github.lmemory123`
- 先跑 `./scripts/run-real-tests.sh all`
- 用 `release` profile 打包、签名、上传
- 等待 Sonatype Central 最终状态变成 `PUBLISHED`
- 结束后把源码切回 `1.1.0-SNAPSHOT`

发布成功的标志是：

```text
[release] published version 1.1.0-RC5
```

### 第 3 步：再打 Git tag

只有 **Central 成功之后** 才打 tag。

```bash
git tag -a v1.1.0-RC5 -m "Release 1.1.0-RC5"
git push origin v1.1.0-RC5
```

### 第 4 步：GitHub Release 自动生成

仓库已经配置了：

- [release.yml](/Users/momao/dm/java/demo/valkey-demo/.github/workflows/release.yml)

所以：

- 推送 `v1.1.0-RC5`
- GitHub 会自动创建 Release

也就是说，不需要手工去网页点 Release。

### 第 5 步：确认源码已经回到开发态

发布脚本结束后，再检查一次：

```bash
grep -n "<version>" pom.xml | head -n 1
```

应该仍然是：

```xml
<version>1.1.0-SNAPSHOT</version>
```

如果已经是 `SNAPSHOT`，而且 `git status` 没有已跟踪改动，就说明不需要再额外 commit。

## 一份最短可执行命令

以 `1.1.0-RC5` 为例：

```bash
cd /Users/momao/dm/java/demo/valkey-demo
MAVEN_GPG_PASSPHRASE='你的口令' ./scripts/release-publish.sh 1.1.0-RC5 8ACEA513E5F728CC /Users/momao/dm/path/java/apache-maven-3.9.14/bin/mvn
git tag -a v1.1.0-RC5 -m "Release 1.1.0-RC5"
git push origin v1.1.0-RC5
```

正式版也是同一套顺序，只是版本号改成：

```bash
1.1.0
```

## RC 和正式版怎么发

推荐规则：

- 日常开发：`x.y.z-SNAPSHOT`
- 候选版：`x.y.z-RC1`
- 候选修复：`x.y.z-RC2`
- 再修复：`x.y.z-RC3`
- 正式版：`x.y.z`

例如：

- 当前开发：`1.1.0-SNAPSHOT`
- 第一轮候选：`1.1.0-RC1`
- 第二轮候选：`1.1.0-RC2`
- 候选版可按实际连续递增，例如：`1.1.0-RC3 / RC4 / RC5`
- 正式发布：`1.1.0`

原则只有一条：

- **Central 版本不可覆盖**

所以 RC 发错了，不能重传，只能继续发：

- `RC2`
- `RC3`
- `RC4`

## 这次真实踩过的坑

### 1. 不要先打 tag

这次就遇到过：

- GitHub tag 和 Release 已经有了
- 但 Central 上对应 RC 其实没有真正发布成功

后果就是：

- 外部看到 Release 以为版本已经可用
- 但 Maven Central 拉不到

所以顺序必须固定：

1. Central 成功
2. 再打 tag
3. 再自动生成 GitHub Release

### 2. Maven 4 不要拿来正式发

这次实践里：

- Maven 4 能做本地 `verify`
- 但正式 `publish` 仍然不建议用 Maven 4

结论：

- 正式发布固定走 Maven 3.9.x

### 3. `MAVEN_GPG_PASSPHRASE` 最稳是内联写法

不要假设另一个终端 shell 环境会继承你的变量。  
最稳的写法就是：

```bash
MAVEN_GPG_PASSPHRASE='你的口令' ./scripts/release-publish.sh ...
```

这样最不容易因为会话隔离出问题。

### 4. Central 对 POM 元数据要求比本地严格

这次 `1.1.0-RC2` 失败的真实原因不是签名，也不是 token，而是：

- 发布模块缺少显式 `name`

Sonatype Central 直接拒绝发布。

所以现在已经固定要求：

- 每个发布模块都要有自己的 `<name>`

### 5. 本地有环境，不代表 CI fresh 容器也成立

这次连续踩出了几类问题：

- standalone bulk 测试 fresh 容器里没有索引
- cluster CI 组网方式不稳
- annotation processor 在 CI 的 reactor 解析方式和本地不同
- 自定义 bundle 镜像最开始只有 `arm64`，CI 的 `linux/amd64` 拉不到

结论：

- 所有运行时功能，以真实 Valkey 验证结果为准
- 发布前必须过 `./scripts/run-real-tests.sh all`

### 6. GitHub Actions 镜像必须用当前项目基线

不要把 CI 镜像随便改成别的公开 tag。  
当前项目必须使用：

```text
ghcr.io/lmemory123/valkey-bundle:9.1.0
```

而且它必须是：

- `linux/amd64`
- `linux/arm64`

双架构 manifest。

## 常见问答

### 是先发 Maven 还是先发 GitHub？

先 Maven Central，后 GitHub tag / Release。

### `release-publish.sh` 成功后为什么不一定还要 commit？

因为这个脚本结束后，源码通常已经被切回 `SNAPSHOT`。  
如果 `git status` 里没有已跟踪改动，就不需要再额外提交。

### 那些未跟踪文件要不要管？

发版主线里不用管。  
像下面这些一般都是本地分析垃圾文件，不要随手 `git add .`：

- `*.xml`
- `index.html`
- `script.js`
- `styles.css`
- `qodana.yaml`

### 如何确认真的发布成功？

先看脚本输出，再看仓库页面：

1. 终端里看到：
   - `[release] published version ...`
2. 再去 Maven Central 仓库目录看版本是否存在
3. 最后再推 tag，等 GitHub Release 自动生成

## 当前实际发布记录

### `1.0.0`

- 已成功发布到 Maven Central

### `1.1.0-RC1`

- GitHub tag / Release 存在
- 但未成功进入 Maven Central

### `1.1.0-RC2`

- Sonatype deployment 失败
- 原因是发布模块缺少显式 `name`

### `1.1.0-RC5`

- 已成功发布到 Maven Central
- tag：`v1.1.0-RC5`
- GitHub Release：自动生成
