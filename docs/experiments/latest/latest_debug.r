library(ggplot2)
library(sqldf)
library(Cairo)
library(stringr)
library(scales)
# Rome

dev.set(0)
df=read.csv('/home/adnan/rome/dev/leanstore/docs/experiments/latest/results.csv')
d=sqldf("select * from df ")
tx <- ggplot(d, aes(t, tx, color=c_contention_management, group=c_contention_management)) + geom_line()
tx <- tx + facet_grid (row=vars(c_zipf_factor), cols=vars(c_contention_update_tracker_pct, c_dram_gib))
print(tx)

aux =sqldf("select t, max(GHz) GHz, min(instr) instr,
 max(space_usage_gib) space_usage_gib,
 sum(dt_restarts_update_same_size) restarts,
 sum(dt_researchy_0) splits,
 sum(dt_researchy_1) merge_succ,
 sum(dt_researchy_2) merge_fail,
c_contention_management,
latest_window_ms,  c_backoff_strategy, c_dram_gib, c_zipf_factor, c_worker_threads,c_contention_update_tracker_pct from d  group by t, c_dram_gib, c_zipf_factor, latest_window_ms,c_worker_threads, c_backoff_strategy,c_contention_update_tracker_pct, c_contention_management")
head(aux)
plot <- ggplot(aux, aes(t)) + geom_line(aes(y=splits), color="red") + geom_line(aes(y=merge_succ), colour="blue")
#plot <- ggplot(aux, aes(t)) + geom_line(aes(y=space_usage_gib), color="red")
plot <- plot + facet_grid (row=vars(latest_window_ms, c_contention_management), cols=vars(c_contention_update_tracker_pct,c_dram_gib))
print(plot)

                                        #+ geom_line(aes(y=merge_fail), color="green", linetype="dotted")
# + geom_line(aes(y=space_usage_gib), color="purple", linetype="longdash")
sqldf("select max(tx)/1e6, c_contention_management from d group by c_contention_management")
sqldf("select max(tx),c_zipf_factor,sum(dt_researchy_0), sum(dt_restarts_update_same_size),c_dram_gib from d group by c_dram_gib")