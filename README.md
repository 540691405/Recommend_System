# Recommend_System
第一个实时itemcf

ItemCF文件夹里：
ItemCF.py是将《推荐系统实践》里的ItemCF算法实现，改入口后可提交

TencentRec.py是参考了腾讯的论文“TencentRec: Real-time Stream Recommendation in Practice”的实时推荐算法,
是可以实时更新相似度的算法。
加入了real-time purning ，可是由于SparkStreaming的batch特性，
连续的每个实时处理的real-time purning 并不适合batch处理的SparkStreaming,
而且代码待优化，并且速度很慢


可以更改入口并且更改Main函数选择你需要的方法：
  TencentRec版本的ItemCF : 方法为ItemCF
  TencentRec版本的StreamingRcommend : 方法为StreamingRecommend
  TencentRec的Real-time update and recommend ：方法为RealTimeRecommend
  TencentRec的Real-time with real time purning :方法为RealTimePurning 
  

  注意：上传版本仅仅是调试入口，如果要submint到集群还是需要将入口修改成提交的模式
