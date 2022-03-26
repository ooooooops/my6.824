一些规则：

- map阶段需要把中间key划分到`nReduce` 个哈希桶，其中`nReduce` 是在`main/mrmaster.go`中传递给`MakeMaster()`的参数。
- 第X个reduce的产出文件命名成`mr-out-X`。
- 一个`mr-out-X`文件应该包含对应的Recude函数输出的一行格式为`"%v %v"`的kv pair。如果不是这个格式，测试脚本会报错。
- `main/mrmaster.go` 需要`mr/master.go`实现`Done()`方法，MapReduce任务完成的时候，这个方法返回true，然后`mrmaster.go`退出。
- MapReduce任务完成后，worker应该能正常退出。一种简单的实现方式是使用call()的返回值：如果worker与master通信失败，它就可以认为master任务完成已经退出了，它自己也就可以正常退出。

提示

- 可以先从修改`mr/worker.go`的`Worker()`函数着手编写代码，这个函数的功能是向master发送RPC获取一个任务。然后编写master发送response相关的代码，response中需要带上文件名。然后再继续完善worker中的代码以实现读文件调用用户Map函数的功能，类似`mrsequential.go`中的功能。

- 用户的Map和Reduce函数在运行时使用go插件包(.so文件)的形式加载。

- 任何对mr/目录的修改都需要使用命令`go build -buildmode=plugin ../mrapps/wc.go`重新构建插件包。

- 这个实验需要worker之间使用同一个文件系统。所有worker跑在同一台机器上时满足这个要求，但是当worker运行在许多不同的机器上时，需要一个类似GFS的全局文件系统。

- 对中间文件命名的一个约定俗成的规则是`mr-X-Y`，其中X是map任务编号，Y是reduce任务编号。

- Map worker需要一种往中间文件写入kv pair的方式以便后面的Reduce worker能正确读取。可以使用go自带的`encoding/json`包。向文件中写入kv pair：

  ```go
    enc := json.NewEncoder(file)
    for _, kv := ... {
      err := enc.Encode(&kv)
  ```

  相应地从文件中读出kv pair：

  ```go
    dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      if err := dec.Decode(&kv); err != nil {
        break
      }
      kva = append(kva, kv)
    }
  ```

- Map worker可以使用ihash(key)函数来选出给定key对应的Reduce worker。

- 你可以从mrsequential.go中借鉴一些代码，包括读取Map操作的输入文件，对中间临时的kv pair排序，存储Reduce的输出到文件。

- master会有并发，因此需要锁住共享数据。（什么数据是共享的？）

- 使用go的竞态检测器：`go build -race`和`go run -race`。`test-mr.sh`中有注释说明如何开启竞态检测器。

- Worker有时候可能需要等待，比如reduce任务需要等到最后一个map任务完成后才能启动。一种方式是Worker可以采用`time.Sleep()`的方式周期性地向master请求任务。另外一种方式是master在相关的RPC处理过程中循环等待，可以使用`time.Sleep()`或`sync.Cond`。go在每个RPC对应的线程中处理RPC请求（意思是每个RPC都会开一个线程），因此各个RPC处理中的循环等待并不会相互影响。

- master不能区分worker是挂了，还是因为某些原因挂住了，或者是因为执行代码实在是太慢了。你最好让master等一段时间，（一段时间后如果还没有等到worker的响应就）放弃这个worker的任务重新把任务分发到其他worker。在这个实验中，我们让master等待10分钟；10分钟后我们就假设worker挂了。

- 你可以使用`mrapps/crash.go`来测试崩溃恢复。

- 为了保证worker崩溃后写了部分的文件不对外可见，MapReduce论文提到了一个技巧：写文件时使用临时文件名，写完后重命名。你可以使用`ioutil.TempFile`来创建临时文件以及`os.Rename`重命名文件。

- `test-mr.sh`会在子目录`mr-tmp`运行所有处理过程，因此如果运行过程中发生了错误，你可以到这个目录中看看中间文件和最终的产出文件。