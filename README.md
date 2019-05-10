# auphi_designer

傲飞Kettle 设计器 3.x 是在 Kettle 5.0 基础上，定制开发的一款设计器.
傲飞 Kettle 设计器可以独立运行，也可以结合傲飞数据整合平台使用。
设计器设计的 ETL 作业或转换可以发布到傲飞数据整合平台，由傲飞数据整合平台调度、监控、管理 ETL作业。

代码地址：
https://github.com/jianjunchu/etl_designer_30


傲飞 Kettle 设计器的版本说明：
   Version 3.4.3   发布时间：2019-04-25  下载： https://pan.baidu.com/s/1JhFuQ8OuvAMvV8iWKMsewQ
   
      1. 增加强行停止功能，通过杀掉步骤线程的方式，强行停止正在运行的转换或作业。
   
      2. 增加系统级别的日期格式变量 KETTLE_DEFAULT_DATE_FORMAT_MASK，默认日期格式: yyyy-MM-dd HH:mm:ss
      
   Version 3.4.2  发布时间：2019-01-25
      1. 字段选择步骤里增加函数和表达式功能。
      2. 增加函数接口，支持用户自定义开发函数插件。
      3. 增加DB2 批量加载步骤。
      4. 日志表增加记录变量功能。
      5. 文本文件输出步骤，增加按日期完成落地文件的分区功能。
      6. bug 修订。
      
   历史版本功能：
      该设计器版本是基于原 Kettle 5.0 版本定制开发，除 Kettle 5.0 已有功能外，增加了 Kettle 6.x, 7.x, 8.x 的big data 架构和相应的插件。
   另外定制开发了 Oracle CDC 增量抽取插件，Oracle 触发器增量抽取插件， 等插件。 
      
   
其他版本傲飞Kettle 设计器说明：
1. 根据新版本 Kettle 8.x 改造的傲飞Kettle 设计器请参考：https://github.com/jianjunchu/pentaho-kettle
2. 傲飞Kettle 云设计器Demo： http://cloud.doetl.com


北京傲飞商智软件有限公司
技术支持： 
      QQ群：493062264
      邮箱：support@pentahochina.com
   
   
