# simplequeue

## Start Server

```sh
go run main.go
2023/07/19 23:16:58 -----------Start queueServer On 8080---------
2023/07/19 23:16:58 --------Starting Topic Message Pump---------
2023/07/19 23:17:04 created new channel:  msg-chan1
2023/07/19 23:17:04 add client to queue server:  {1 0x140000b4500   0x1400019a2d0 0x140001964e0}
found client:  1
found client Channel:  msg-chan1
found client Channel Topic:  hello-topic
--------Starting Client Message Pump--------
2023/07/19 23:17:04 cli channel: 0x140001964e0
2023/07/19 23:17:04 cli channel msg length: 0
2023/07/19 23:17:08 [QueueServer] Incoming req: pub:<topic:"hello-topic" msg:"Hello Hello Hello - How are you" >
2023/07/19 23:17:08 [QueueServer] get topic: &{mu:{w:{state:0 sema:0} writerSem:0 readerSem:0 readerCount:{_:{} v:0} readerWait:{_:{} v:0}} qs:0x140000b4500 Name:hello-topic ChannelMap:map[msg-chan1:0x1400019a2d0] MemoryMsgChan:0x14000196420}
2023/07/19 23:17:08 [QueueServer] new message: id:"cirvst382vs422ad2l00" body:"Hello Hello Hello - How are you" timestamp:1689779828
2023/07/19 23:17:08 [QueueServer] Success Put Msg: id:"cirvst382vs422ad2l00" body:"Hello Hello Hello - How are you" timestamp:1689779828 , to: hello-topic
2023/07/19 23:17:08 put msg to channels:  id:"cirvst382vs422ad2l00" body:"Hello Hello Hello - How are you" timestamp:1689779828
2023/07/19 23:17:08 length of channel:  1
2023/07/19 23:17:08 put msg to channel:  id:"cirvst382vs422ad2l00" body:"Hello Hello Hello - How are you" timestamp:1689779828  &{{{0 0} 0 0 {{} 0} {{} 0}} hello-topic msg-chan1 0x14000196480 0x140000b4500 map[1:0x140001a6280]}
2023/07/19 23:17:08 Send Msg:  id:"cirvst382vs422ad2l00" body:"Hello Hello Hello - How are you" timestamp:1689779828
```

## Start Consumer

```sh
go run main.go
2023/07/19 23:17:04 Sub Event to queue server:  localhost:8080
2023/07/19 23:17:04 [Consumer] SubEvent ClientID: 1
2023/07/19 23:17:08 received msg:  {cirvst382vs422ad2l00 Hello Hello Hello - How are you 1689779828 0 {} [] 0}
```

## Start Producer

```sh
go run producer.go
2023/07/19 23:17:08 Pub message to queue server:  localhost:8080
2023/07/19 23:17:08 [Producer] PubMessage:
```
