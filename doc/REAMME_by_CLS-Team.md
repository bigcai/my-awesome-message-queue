- 文档存放在该文件夹doc下面。

> 要解决的问题：
> 1. 高效的消息存储问题
> 2. 严格顺序消费消息
> 3. 尽最大努力不丢消息，或者能够做到消息可恢复
> 4. 高效能够找到下一条消息或者是否有上一条依赖消息来源



1.一个队列 177 位，即23个Byte， 100个topic和100个queue，即最大有4600个Byte,即4.5KB

2.一个queueData 56位， 即7个Byte


所有的queueindex都放在一个page

一个队列有多个queueIndex

一个queueIndex有多个queueData,queueIndex里面的queueData都在同一个页

一个queueData表示一个message