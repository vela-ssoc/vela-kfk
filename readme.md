# kafka
> 封装的kafka的生产着和消费者的接口对象

## 内置方法
- [vela.kfk.producer(cfg)](#生产者)&emsp; 获取一个kafka生产对象
- [vela.kfk.consumer(cfg)](#消费者)&emsp; 获取一个kafka消费对象


## 生产者
> p = vela.kfk.producer(cfg) <br />
> 满足lua.writer接口  所有output位置都可以满足

配置信息:
- name
- balancer    &emsp; least,crc32,hash,rr
- ack         &emsp;是否开启确认,默认关闭:one , all
- compression &emsp;是否开启压缩
- async &emsp;默认为true
- topic &emsp; 可以是string或者[]string
- addr &emsp; 可以是string或者[]string

内置函数
- [p.start()](#) &emsp;启动
- [p.limit(n ,time)](#) &emsp;限速
- [p.push(value)](#) &emsp;发送数据
- [p.clone(topic)](#) &emsp;生成一个新的topic对象,不用新建链接


```lua
    local p = vela.kfk.producer{
        name = "client",
        topic = "producer-topic",
        addr  = {"192.168.1.2:9092" , "192.168.1.3:9092"}
    }
    p.limit(1000 , 10) --10s 1000条数据
    p.start()
    p.push("aaaaaaaaaaaaaaaaaaaaaaaaaaaa")    
    
    local p2 = p.clone("topic2")
    p2.push("bbbbbbbbbbbbbbbbbbbb2")
```

## 消费者
> c = vela.kfk.consumer(cfg)

配置信息:
- name
- thread
- addr &emsp; 可以是string或者[]string
- group_id &emsp; group 默认:vela
- topic &emsp; 可以是string或者[]string

内置函数:
- [c.start()](#)
- [c.select({switch})](#)
- [c.pipe()](#)
- [c.to(output)](#)
- [c.limit(n , time)](#)

```lua
    local switch = vela.switch()
    switch._{
        ["name = w"] = print,
        ["name = zhangsan"] = print
    }

    local cli = vela.elastic.cli{} --output

    local c = vela.kfk.consumer{
        name   = "aaa",
        thread = 10,
        group_id = "kkk",
        topic  = {"topic1" , "topic2"},
        addr   = {"192.168.1.1:9092" , "192.168.1.2:9092"},
    }

    s.limit(1000)
    s.select(switch)
    s.pipe(print)
    
    s.to(cli)
    s.start()
    
    
```