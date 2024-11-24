# 说明
kafka中存在大量的延时任务，传统的jdk的延时任务实现：timer、ScheduleThreadPool，使用的是优先队列+时间循环比对的方式，<br/>
效率不高（复杂度nlogn）。时间轮是延时任务的高效解决方式。